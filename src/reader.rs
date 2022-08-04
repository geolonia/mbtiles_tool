use crate::tilebelt::TileData;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use rand::seq::SliceRandom;
use rand::thread_rng;

pub const EXTENT_CHUNK_TILE_COUNT: u64 = u64::pow(2, 15);

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

fn initialize_extents(input: PathBuf) -> Vec<InputTileZoomExtent> {
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
  extents
}

fn initialize_threads(
  extents: Vec<InputTileZoomExtent>,
  input: PathBuf,
  output_queue_tx: crossbeam_channel::Sender<TileData>,
) {
  // worker threads
  let max_workers = std::cmp::max(num_cpus::get() - 2, 2);
  println!("Spawning {} input workers.", max_workers);

  let shared_extents = Arc::new(extents);
  for worker_id in 0..max_workers {
    let thread_extents = shared_extents.clone();
    let thread_input = input.clone();
    let thread_output_queue_tx = output_queue_tx.clone();
    thread::spawn(move || {
      let connection = sqlite::open(thread_input).unwrap();
      connection.execute("PRAGMA query_only = true;").unwrap();

      let mut statement = connection
        .prepare(
          "
        SELECT
          zoom_level,
          tile_column,
          tile_row,
          tile_data
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

      let mut our_extents: Vec<&InputTileZoomExtent> = thread_extents
        .iter()
        .skip(extent_n - 1)
        .step_by(max_workers)
        .collect();
      // shuffle our_extents to evenly distribute workload
      our_extents.shuffle(&mut thread_rng());

      for extent in our_extents {
        statement.bind(1, extent.zoom as i64).unwrap();
        statement.bind(2, extent.min_x as i64).unwrap();
        statement.bind(3, extent.max_x as i64).unwrap();
        statement.bind(4, extent.min_y as i64).unwrap();
        statement.bind(5, extent.max_y as i64).unwrap();

        while let sqlite::State::Row = statement.next().unwrap() {
          let zoom_level = statement.read::<i64>(0).unwrap() as u32;
          let tile_column = statement.read::<i64>(1).unwrap() as u32;
          let tile_row = statement.read::<i64>(2).unwrap() as u32;
          let tile_data = Arc::new(statement.read::<Vec<u8>>(3).unwrap());

          thread_output_queue_tx
            .send(TileData {
              tile: (tile_column, tile_row, zoom_level),
              data: tile_data.clone(),
            })
            .unwrap();
        }

        statement.reset().unwrap();
      }

      println!("Finished reading tiles ({}).", worker_id);
    });
  }
}

pub struct Reader {
  input: PathBuf,
  output_rx: crossbeam_channel::Receiver<TileData>,
}

impl Reader {
  pub fn new(input: PathBuf) -> Reader {
    let (output_tx, output_rx) = crossbeam_channel::unbounded();
    let extents = initialize_extents(input.clone());
    initialize_threads(extents, input.clone(), output_tx);
    Reader { input, output_rx }
  }

  pub fn iter(&mut self) -> crossbeam_channel::Iter<TileData> {
    self.output_rx.iter()
  }

  pub fn read_metadata(&mut self) -> HashMap<String, String> {
    let connection = sqlite::open(&self.input).unwrap();
    connection.execute("PRAGMA query_only = true;").unwrap();
    let mut metadata_stmt = connection
      .prepare("SELECT name, value FROM metadata;")
      .unwrap();
    let mut metadata = HashMap::<String, String>::new();
    while let sqlite::State::Row = metadata_stmt.next().unwrap() {
      let name = metadata_stmt.read::<String>(0).unwrap();
      let value = metadata_stmt.read::<String>(1).unwrap();
      metadata.insert(name, value);
    }
    metadata
  }
}
