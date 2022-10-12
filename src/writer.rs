use crate::reader::EXTENT_CHUNK_TILE_COUNT;
use crate::tilebelt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::{thread, time};

pub fn initialize_writer(
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
