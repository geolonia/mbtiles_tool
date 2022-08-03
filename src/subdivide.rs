use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time;

use crate::reader::{Reader, EXTENT_CHUNK_TILE_COUNT};
use crate::tilebelt::{tile_is_ancestor, Tile, TileData};

struct MetadataRow {
  name: String,
  value: String,
}

#[derive(Serialize, Deserialize)]
struct SubdivideOutput {
  name: String,
  tiles: Vec<Tile>,
  maxzoom: Option<u32>,
}

#[derive(Serialize, Deserialize)]
struct SubdivideConfig {
  outputs: Vec<SubdivideOutput>,
}

pub fn subdivide(config_path: PathBuf, input: PathBuf, output: PathBuf) {
  println!(
    "Reading config from {}, input from {} and output to {}",
    config_path.display(),
    input.display(),
    output.display()
  );

  let config: SubdivideConfig =
    serde_json::from_reader(std::fs::File::open(&config_path).unwrap()).unwrap();

  let connection = sqlite::open(&input).unwrap();
  connection.execute("PRAGMA query_only = true;").unwrap();

  let mut metadata_rows: Vec<MetadataRow> = Vec::new();
  let mut metadata_read_statement = connection
    .prepare("SELECT name, value FROM metadata")
    .unwrap();
  while let sqlite::State::Row = metadata_read_statement.next().unwrap() {
    let name = metadata_read_statement.read::<String>(0).unwrap();
    let value = metadata_read_statement.read::<String>(1).unwrap();
    metadata_rows.push(MetadataRow { name, value });
  }
  let metadata_rows_ref = Arc::new(metadata_rows);

  let mut output_queue_txs: Vec<crossbeam_channel::Sender<TileData>> = Vec::new();
  let mut output_queue_rxs: Vec<crossbeam_channel::Receiver<TileData>> = Vec::new();
  let mut tile_to_output_idx_map: Vec<(Tile, u32, usize)> = Vec::new();
  let mut output_threads: Vec<std::thread::JoinHandle<()>> = Vec::new();

  for (i, output_config) in config.outputs.iter().enumerate() {
    let (output_queue_tx, output_queue_rx) = crossbeam_channel::bounded::<TileData>(100_000);
    let output_thread_queue_rx = output_queue_rx.clone();

    let config_maxzoom = output_config.maxzoom.unwrap_or(999);

    output_queue_txs.push(output_queue_tx);
    output_queue_rxs.push(output_queue_rx);

    for tile in &output_config.tiles {
      tile_to_output_idx_map.push((*tile, config_maxzoom, i));
    }

    let output_thread_metadata_rows = Arc::clone(&metadata_rows_ref);
    let output_config_name = output_config.name.clone();
    let output_thread_path = output.join(format!("{}.mbtiles", output_config_name));
    println!(
      "Spawning thread for output {} to {}",
      output_config_name,
      output_thread_path.display()
    );
    let output_thread_handle = thread::spawn(move || {
      let mut last_ts = time::Instant::now();
      let mut tile_count = 0;

      let connection = sqlite::open(output_thread_path).unwrap();
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

      let mut max_zoom = 0;
      let mut min_zoom = 999;
      while let Ok(work) = output_thread_queue_rx.recv() {
        {
          tile_count += 1;

          max_zoom = std::cmp::max(max_zoom, work.tile.2);
          min_zoom = std::cmp::min(min_zoom, work.tile.2);

          insert_stmt.bind(1, work.tile.2 as i64).unwrap();
          insert_stmt.bind(2, work.tile.0 as i64).unwrap();
          insert_stmt.bind(3, work.tile.1 as i64).unwrap();
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
              "[{}] {} tiles in {}ms ({:.4}ms/tile)",
              output_config_name,
              tile_count,
              elapsed.as_millis(),
              elapsed.as_millis() as f64 / (EXTENT_CHUNK_TILE_COUNT as f64),
            );
            last_ts = ts;
          }
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
      for row in output_thread_metadata_rows.iter() {
        insert_metadata_stmt.bind(1, &*row.name).unwrap();
        insert_metadata_stmt.bind(2, &*row.value).unwrap();
        insert_metadata_stmt.next().unwrap();
        insert_metadata_stmt.reset().unwrap();
      }

      let override_metadata: Vec<MetadataRow> = vec![
        MetadataRow {
          name: "maxzoom".to_string(),
          value: max_zoom.to_string(),
        },
        MetadataRow {
          name: "minzoom".to_string(),
          value: min_zoom.to_string(),
        },
      ];
      for row in override_metadata.iter() {
        insert_metadata_stmt.bind(1, &*row.name).unwrap();
        insert_metadata_stmt.bind(2, &*row.value).unwrap();
        insert_metadata_stmt.next().unwrap();
        insert_metadata_stmt.reset().unwrap();
      }

      println!(
        "Output thread {} finished, {} tiles",
        output_config_name, tile_count
      );
      connection.execute("PRAGMA journal_mode = DELETE").unwrap();
    });

    output_threads.push(output_thread_handle);
  }

  let mut reader = Reader::new(input);
  for input_tile in reader.iter() {
    let tile_column = input_tile.tile.0;
    let tile_row = input_tile.tile.1;
    let zoom_level = input_tile.tile.2;

    let flipped_row = (1 << zoom_level) - 1 - tile_row;

    let this_tile = (tile_column, flipped_row, zoom_level);

    let tile_to_output_idx_map = tile_to_output_idx_map.iter();
    for (tile, maxzoom, i) in tile_to_output_idx_map {
      if zoom_level > *maxzoom {
        continue;
      }

      if tile_is_ancestor(&this_tile, tile) {
        output_queue_txs[*i]
          .send(TileData {
            tile: (tile_column, tile_row, zoom_level),
            data: input_tile.data.clone(),
          })
          .unwrap();
        // don't break here so we can support overlapping outputs
      }
    }
  }

  drop(output_queue_txs);

  for output_thread in output_threads {
    output_thread.join().unwrap();
  }

  println!("Done subdivision.");
}
