use crate::reader::{Reader, EXTENT_CHUNK_TILE_COUNT};
use crate::{tilebelt, vector_tile_ops};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use mbtiles_tool::vector_tile;
use prost::Message;
use std::collections::HashMap;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::{thread, time};

fn maybe_decompress(data: Vec<u8>) -> Vec<u8> {
  if data[0] == 0x1f && data[1] == 0x8b {
    let mut out = Vec::with_capacity(data.len() * 2);
    let mut zlib = GzDecoder::new(data.as_slice());
    zlib.read_to_end(&mut out).unwrap();
    return out;
  }
  data
}

fn initialize_processors(
  process_queue_rx: crossbeam_channel::Receiver<tilebelt::TileData>,
  output_queue_tx: crossbeam_channel::Sender<tilebelt::TileData>,
  maxzoom: u8,
  target_zoom: u8,
) -> Vec<thread::JoinHandle<()>> {
  let max_workers = std::cmp::max(num_cpus::get() - 2, 2);
  let mut processor_thread_handles = Vec::with_capacity(max_workers);

  for worker_id in 0..max_workers {
    let thread_process_queue_rx = process_queue_rx.clone();
    let thread_output_queue_tx = output_queue_tx.clone();
    processor_thread_handles.push(thread::spawn(move || {
      while let Ok(tile_data) = thread_process_queue_rx.recv() {
        // first, pass the original tile through to the output
        thread_output_queue_tx.send(tile_data.clone()).unwrap();
        if (tile_data.tile.2 as u8) == maxzoom {
          // because this tile is the maximum available resolution, we use it to generate
          // higher resolution tiles until target_zoom.
          let raw_tile_data = maybe_decompress(tile_data.data.to_vec());
          let parsed_tile = vector_tile::Tile::decode(&*raw_tile_data).unwrap();
          let tiles_to_generate = tilebelt::get_children_until_zoom(&tile_data.tile, target_zoom);
          for tile in tiles_to_generate.iter() {
            let (ancestor, steps, (rel_x, rel_y)) =
              tilebelt::get_relative_position_in_ancestor(tile, maxzoom);
            assert_eq!(tile_data.tile, ancestor);
            let scaled_tile = vector_tile_ops::scale_tile(parsed_tile.clone(), steps, rel_x, rel_y);
            let scaled_tile_data = scaled_tile.encode_to_vec();
            let mut gz = GzEncoder::new(Vec::new(), Compression::default());
            gz.write_all(&scaled_tile_data).unwrap();
            let compressed_data = gz.finish().unwrap();
            let tile_data = tilebelt::TileData {
              tile: *tile,
              data: Arc::new(compressed_data),
            };
            thread_output_queue_tx.send(tile_data).unwrap();
          }
        }
      }
      println!("Worker {} finished.", worker_id);
    }));
  }
  processor_thread_handles
}

fn initialize_writer(
  output: PathBuf,
  queue: crossbeam_channel::Receiver<tilebelt::TileData>,
  metadata: HashMap<String, String>,
) -> thread::JoinHandle<()> {
  thread::spawn(move || {
    let mut last_ts = time::Instant::now();
    let mut tile_count = 0;

    let connection = sqlite::open(output).unwrap();
    connection
      .execute(
        "
        PRAGMA synchronous = OFF;
        PRAGMA journal_mode = MEMORY;

        CREATE TABLE IF NOT EXISTS metadata (
          name text,
          value text
        );

        CREATE TABLE IF NOT EXISTS tiles (
          zoom_level INTEGER,
          tile_column INTEGER,
          tile_row INTEGER,
          tile_data blob
        );

        CREATE UNIQUE INDEX IF NOT EXISTS name ON metadata (name);
        CREATE UNIQUE INDEX IF NOT EXISTS xyz ON tiles (zoom_level, tile_column, tile_row);

        BEGIN TRANSACTION;
      ",
      )
      .unwrap();

    let mut insert_stmt = connection
      .prepare(
        "
        INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data)
        VALUES (?, ?, ?, ?)
      ",
      )
      .unwrap();

    while let Ok(work) = queue.recv() {
      tile_count += 1;

      let tile = tilebelt::flip_x(work.tile);

      insert_stmt.bind(1, tile.2 as i64).unwrap();
      insert_stmt.bind(2, tile.0 as i64).unwrap();
      insert_stmt.bind(3, tile.1 as i64).unwrap();
      insert_stmt.bind(4, &**work.data).unwrap();

      insert_stmt.next().unwrap();
      insert_stmt.reset().unwrap();

      if tile_count % EXTENT_CHUNK_TILE_COUNT == 0 {
        connection
          .execute("END TRANSACTION; BEGIN TRANSACTION;")
          .unwrap();

        let ts = time::Instant::now();
        let elapsed = ts.duration_since(last_ts);
        println!(
          "[output] {} tiles in {}ms ({:.4}ms/tile)",
          tile_count,
          elapsed.as_millis(),
          elapsed.as_millis() as f64 / (EXTENT_CHUNK_TILE_COUNT as f64),
        );
        last_ts = ts;
      }
    }
    connection.execute("END TRANSACTION;").unwrap();

    let mut insert_metadata_stmt = connection
      .prepare(
        "
        INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)
      ",
      )
      .unwrap();
    for (name, value) in metadata.iter() {
      insert_metadata_stmt.bind(1, &**name).unwrap();
      insert_metadata_stmt.bind(2, &**value).unwrap();
      insert_metadata_stmt.next().unwrap();
      insert_metadata_stmt.reset().unwrap();
    }

    println!("Output finished, {} tiles", tile_count);
    connection.execute("PRAGMA journal_mode = DELETE").unwrap();
  })
}

pub fn overzoom(input: PathBuf, output: PathBuf, target_zoom: u8) {
  let mut reader = Reader::new(input);
  let mut metadata_rows = reader.read_metadata();
  let maxzoom = metadata_rows["maxzoom"]
    .parse::<u8>()
    .unwrap_or(u8::max_value());
  if maxzoom >= target_zoom {
    panic!("Input file is already at or above target zoom level");
  }
  if maxzoom == u8::max_value() {
    panic!("Input file has no maxzoom metadata");
  }

  println!(
    "Extending tiles from z{} to z{} and saving to {}...",
    maxzoom,
    target_zoom,
    output.display()
  );

  metadata_rows.insert("maxzoom".to_string(), target_zoom.to_string());

  let (process_queue_tx, process_queue_rx) = crossbeam_channel::unbounded::<tilebelt::TileData>();
  let (output_queue_tx, output_queue_rx) = crossbeam_channel::unbounded::<tilebelt::TileData>();

  let processor_thread_handles =
    initialize_processors(process_queue_rx, output_queue_tx, maxzoom, target_zoom);
  let writer_handle = initialize_writer(output.clone(), output_queue_rx, metadata_rows);

  for tile in reader.iter() {
    let flipped_tile = tilebelt::flip_x(tile.tile);
    process_queue_tx
      .send(tilebelt::TileData {
        tile: flipped_tile,
        data: tile.data,
      })
      .unwrap();
  }
  drop(process_queue_tx);

  for handle in processor_thread_handles {
    handle.join().unwrap();
  }
  writer_handle.join().unwrap();

  println!("Filled {} with all the good things", output.display());
}
