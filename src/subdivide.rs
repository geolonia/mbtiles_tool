use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time;

use crate::tilebelt::{tile_is_ancestor, Tile};

const EXTENT_CHUNK_TILE_COUNT: u64 = u64::pow(2, 15);

#[derive(Debug, Clone, Copy)]
struct InputTileZoomExtent {
  zoom: u8,
  min_x: u64,
  max_x: u64,
  min_y: u64,
  max_y: u64,
}

impl InputTileZoomExtent {
  fn tile_count(self) -> u64 {
    ((self.max_x - self.min_x) + 1) * ((self.max_y - self.min_y) + 1)
  }
}

fn split_tile_extent(e: InputTileZoomExtent) -> Vec<InputTileZoomExtent> {
  // split the box into two halves, on the long axis
  // if the box is too small, just return the original box

  let half_width = (e.max_x - e.min_x) / 2;
  let half_height = (e.max_y - e.min_y) / 2;
  if half_width <= 1 || half_height <= 1 {
    return vec![e];
  }
  let mut ret = Vec::new();

  if half_width > half_height {
    // the rectangle is wider than it is tall, so split it horizontally
    let left = InputTileZoomExtent {
      zoom: e.zoom,
      min_x: e.min_x,
      max_x: e.min_x + half_width,
      min_y: e.min_y,
      max_y: e.max_y,
    };
    let right = InputTileZoomExtent {
      zoom: e.zoom,
      min_x: e.min_x + half_width + 1,
      max_x: e.max_x,
      min_y: e.min_y,
      max_y: e.max_y,
    };
    ret.push(left);
    ret.push(right);
  } else {
    // the rectangle is taller than it is wide, so split it vertically
    let top = InputTileZoomExtent {
      zoom: e.zoom,
      min_x: e.min_x,
      max_x: e.max_x,
      min_y: e.min_y,
      max_y: e.min_y + half_height,
    };
    let bottom = InputTileZoomExtent {
      zoom: e.zoom,
      min_x: e.min_x,
      max_x: e.max_x,
      min_y: e.min_y + half_height + 1,
      max_y: e.max_y,
    };
    ret.push(top);
    ret.push(bottom);
  }

  ret
}

// recursively split tile extents until no more extents contain more than EXTENT_CHUNK_TILE_COUNT tiles
fn split_tile_extent_recursive(e: InputTileZoomExtent) -> Vec<InputTileZoomExtent> {
  let mut ret = Vec::new();
  if e.tile_count() <= EXTENT_CHUNK_TILE_COUNT {
    ret.push(e);
  } else {
    let extents = split_tile_extent(e);
    for e in extents {
      ret.extend(split_tile_extent_recursive(e));
    }
  }
  ret
}

struct WorkJob {
  tile: Tile,
}

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

  let mut output_queue_txs: Vec<crossbeam_channel::Sender<WorkJob>> = Vec::new();
  let mut output_queue_rxs: Vec<crossbeam_channel::Receiver<WorkJob>> = Vec::new();
  let mut tile_to_output_idx_map: Vec<(Tile, u32, usize)> = Vec::new();
  let mut output_threads: Vec<std::thread::JoinHandle<()>> = Vec::new();

  for (i, output_config) in config.outputs.iter().enumerate() {
    let (output_queue_tx, output_queue_rx) = crossbeam_channel::bounded::<WorkJob>(100_000);
    let output_thread_queue_rx = output_queue_rx.clone();

    let config_maxzoom = output_config.maxzoom.unwrap_or(999);

    output_queue_txs.push(output_queue_tx);
    output_queue_rxs.push(output_queue_rx);

    for tile in &output_config.tiles {
      tile_to_output_idx_map.push((*tile, config_maxzoom, i));
    }

    let output_thread_input = input.clone();
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

      let input_conn = sqlite::open(output_thread_input).unwrap();
      let mut input_conn_stmt = input_conn
        .prepare(
          "SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?",
        )
        .unwrap();

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

          input_conn_stmt.bind(1, work.tile.2 as i64).unwrap();
          input_conn_stmt.bind(2, work.tile.0 as i64).unwrap();
          input_conn_stmt.bind(3, work.tile.1 as i64).unwrap();
          input_conn_stmt.next().unwrap();
          let data = input_conn_stmt.read::<Vec<u8>>(0).unwrap();
          input_conn_stmt.reset().unwrap();

          max_zoom = std::cmp::max(max_zoom, work.tile.2);
          min_zoom = std::cmp::min(min_zoom, work.tile.2);

          insert_stmt.bind(1, work.tile.2 as i64).unwrap();
          insert_stmt.bind(2, work.tile.0 as i64).unwrap();
          insert_stmt.bind(3, work.tile.1 as i64).unwrap();
          insert_stmt.bind(4, &*data).unwrap();

          insert_stmt.next().unwrap();
          insert_stmt.reset().unwrap();

          if tile_count % 100_000 == 0 {
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
              elapsed.as_millis() as f64 / 100_000_f64,
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

  println!("Querying mbtiles for tile extents...");
  let mut input_extents = Vec::<InputTileZoomExtent>::new();
  let connection = sqlite::open(&input).unwrap();
  connection.execute("PRAGMA query_only = true;").unwrap();
  let mut extent_stmt = connection
    .prepare(
      "
      SELECT
        zoom_level,
        MIN(tile_column) AS min_tile_column,
        MAX(tile_column) AS max_tile_column,
        MIN(tile_row) AS min_tile_row,
        MAX(tile_row) AS max_tile_row
      FROM tiles
      GROUP BY zoom_level
      ;
    ",
    )
    .unwrap();
  while let sqlite::State::Row = extent_stmt.next().unwrap() {
    let zoom_level = extent_stmt.read::<i64>(0).unwrap();
    let min_tile_column = extent_stmt.read::<i64>(1).unwrap();
    let max_tile_column = extent_stmt.read::<i64>(2).unwrap();
    let min_tile_row = extent_stmt.read::<i64>(3).unwrap();
    let max_tile_row = extent_stmt.read::<i64>(4).unwrap();
    input_extents.push(InputTileZoomExtent {
      zoom: zoom_level as u8,
      min_x: min_tile_column as u64,
      min_y: min_tile_row as u64,
      max_x: max_tile_column as u64,
      max_y: max_tile_row as u64,
    });
  }
  // split extents in to chunks for processing
  let mut extents = Vec::<InputTileZoomExtent>::new();
  for input_extent in input_extents {
    // each extent should have at most EXTENT_CHUNK_TILE_COUNT tiles
    // let mut extent = input_extent;
    if input_extent.tile_count() > EXTENT_CHUNK_TILE_COUNT {
      let extents_to_add = split_tile_extent_recursive(input_extent);
      extents.extend(extents_to_add);
    } else {
      extents.push(input_extent);
    }
  }

  // println!("extents: {:?}", extents);
  let shared_extents = Arc::new(extents);
  let shared_output_queue_txs = Arc::new(output_queue_txs);
  let shared_tile_to_output_idx_map = Arc::new(tile_to_output_idx_map);

  // worker threads
  let max_workers = std::cmp::max(num_cpus::get() / 2, 2);
  println!("Spawning {} input workers.", max_workers);

  let mut input_thread_handles = Vec::new();

  for worker_id in 0..max_workers {
    let thread_extents = shared_extents.clone();
    let thread_input = input.clone();
    let output_queue_txs = shared_output_queue_txs.clone();
    let input_tile_to_output_idx_map = shared_tile_to_output_idx_map.clone();
    let input_thread_handle = thread::spawn(move || {
      let connection = sqlite::open(thread_input).unwrap();
      connection.execute("PRAGMA query_only = true;").unwrap();

      let mut statement = connection
        .prepare(
          "
        SELECT
          zoom_level,
          tile_column,
          tile_row
        FROM
          tiles
        WHERE
          zoom_level = ? AND
          tile_column >= ? AND
          tile_column <= ? AND
          tile_row >= ? AND
          tile_row <= ?
      ",
        )
        .unwrap();

      let extent_n = worker_id + 1;
      for extent in thread_extents
        .iter()
        .skip(extent_n - 1)
        .step_by(max_workers)
      {
        statement.bind(1, extent.zoom as i64).unwrap();
        statement.bind(2, extent.min_x as i64).unwrap();
        statement.bind(3, extent.max_x as i64).unwrap();
        statement.bind(4, extent.min_y as i64).unwrap();
        statement.bind(5, extent.max_y as i64).unwrap();

        while let sqlite::State::Row = statement.next().unwrap() {
          let zoom_level = statement.read::<i64>(0).unwrap() as u32;
          let tile_column = statement.read::<i64>(1).unwrap() as u32;
          let tile_row = statement.read::<i64>(2).unwrap() as u32;

          // flipped = (1 << row[0]) - 1 - row[2]
          let flipped_row = (1 << zoom_level) - 1 - tile_row;

          let this_tile = (tile_column, flipped_row, zoom_level);

          let tile_to_output_idx_map = input_tile_to_output_idx_map.iter();
          for (tile, maxzoom, i) in tile_to_output_idx_map {
            if zoom_level > *maxzoom {
              continue;
            }

            if tile_is_ancestor(&this_tile, tile) {
              output_queue_txs[*i]
                .send(WorkJob {
                  tile: (tile_column, tile_row, zoom_level),
                })
                .unwrap();
              // don't break here so we can support overlapping outputs
            }
          }
        }

        statement.reset().unwrap();
      }

      println!("Finished reading tiles ({}).", worker_id);
    });
    input_thread_handles.push(input_thread_handle);
  }

  drop(shared_output_queue_txs);

  for input_thread in input_thread_handles {
    input_thread.join().unwrap();
  }
  for output_thread in output_threads {
    output_thread.join().unwrap();
  }

  println!("Done subdivision.");
}
