use crossbeam_utils::atomic::AtomicCell;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time;
use std::time::Duration;

use crate::tilebelt::{tile_is_ancestor, Tile};

// because recv() will block indefinitely, we set a timeout on recv.
// flags are used to signal whether the input / output has finished,
// so this timeout is only required because without it, the signal
// won't be refreshed
const QUEUE_RECV_TIMEOUT_MS: u64 = 100;

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

  let input_done: Arc<AtomicCell<bool>> = Arc::new(AtomicCell::new(false));

  let mut output_queue_txs: Vec<crossbeam_channel::Sender<WorkJob>> = Vec::new();
  let mut output_queue_rxs: Vec<crossbeam_channel::Receiver<WorkJob>> = Vec::new();
  let mut tile_to_output_idx_map: HashMap<Tile, usize> = HashMap::new();
  let mut output_threads: Vec<std::thread::JoinHandle<()>> = Vec::new();

  for (i, output_config) in config.outputs.iter().enumerate() {
    let (output_queue_tx, output_queue_rx) = crossbeam_channel::bounded::<WorkJob>(100_000);
    let output_thread_queue_rx = output_queue_rx.clone();

    output_queue_txs.push(output_queue_tx);
    output_queue_rxs.push(output_queue_rx);

    for tile in &output_config.tiles {
      tile_to_output_idx_map.insert(*tile, i);
    }

    let output_thread_input = input.clone();
    let output_thread_metadata_rows = Arc::clone(&metadata_rows_ref);
    let output_config_name = output_config.name.clone();
    let output_thread_input_done = Arc::clone(&input_done);
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
      while !output_thread_input_done.load() {
        if let Ok(work) =
          output_thread_queue_rx.recv_timeout(Duration::from_millis(QUEUE_RECV_TIMEOUT_MS))
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

  let mut tile_read_statement = connection
    .prepare(
      "
    SELECT
      zoom_level,
      tile_column,
      tile_row
    FROM
      tiles
    ORDER BY zoom_level, tile_column, tile_row ASC
  ",
    )
    .unwrap();

  while let sqlite::State::Row = tile_read_statement.next().unwrap() {
    let zoom_level = tile_read_statement.read::<i64>(0).unwrap() as u32;
    let tile_column = tile_read_statement.read::<i64>(1).unwrap() as u32;
    let tile_row = tile_read_statement.read::<i64>(2).unwrap() as u32;

    // flipped = (1 << row[0]) - 1 - row[2]
    let flipped_row = (1 << zoom_level) - 1 - tile_row;

    let this_tile = (tile_column, flipped_row, zoom_level);
    for (tile, i) in &tile_to_output_idx_map {
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
  println!("Finished reading input from mbtiles.");
  input_done.store(true);

  for output_thread in output_threads {
    output_thread.join().unwrap();
  }

  println!("Done subdivision.");
}
