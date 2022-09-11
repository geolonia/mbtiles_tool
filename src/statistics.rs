use cli_table::{print_stdout, Table, WithTitle};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Table)]
struct ZoomLevelStats {
  #[table(title = "z")]
  zoom: u8,
  #[table(title = "Tile size (min)")]
  min_tile_data: u32,
  #[table(title = "Tile size (max)")]
  max_tile_data: u32,
  #[table(title = "Tile size (average)")]
  avg_tile_data: f64,
  #[table(title = "Tile count")]
  tile_count: u64,
}

#[derive(Table)]
struct LargeTileStats {
  #[table(title = "z")]
  z: u8,
  #[table(title = "x")]
  x: u64,
  #[table(title = "y")]
  y: u64,
  #[table(title = "Tile size")]
  tile_data_length: u32,
}

pub struct StatisticsOutput {
  name: String,
  zoom_level_stats: Vec<ZoomLevelStats>,
  large_tile_stats: HashMap<u32, Vec<LargeTileStats>>,
}

impl StatisticsOutput {
  pub fn print_cli_table(self) {
    println!("Statistics for {}:", self.name);
    print_stdout(self.zoom_level_stats.with_title()).unwrap();

    for (threshold, stats) in self.large_tile_stats {
      println!("Large tiles with size > {} bytes:", threshold);
      print_stdout(stats.with_title()).unwrap();
    }
  }
}

fn calculate_zoom_level_stats(connection: &sqlite::Connection) -> Vec<ZoomLevelStats> {
  let mut out = Vec::<ZoomLevelStats>::new();
  let mut stmt = connection
    .prepare("select zoom_level, min(length(tile_data)), max(length(tile_data)), avg(length(tile_data)), count(*) from tiles group by zoom_level order by zoom_level asc;
    ")
    .unwrap();

  while let sqlite::State::Row = stmt.next().unwrap() {
    out.push(ZoomLevelStats {
      zoom: stmt.read::<i64>(0).unwrap() as u8,
      min_tile_data: stmt.read::<i64>(1).unwrap() as u32,
      max_tile_data: stmt.read::<i64>(2).unwrap() as u32,
      avg_tile_data: stmt.read::<f64>(3).unwrap(),
      tile_count: stmt.read::<i64>(4).unwrap() as u64,
    });
  }
  out
}

fn calculate_large_tile_stats(
  connection: &sqlite::Connection,
  threshold: u32,
) -> Vec<LargeTileStats> {
  let mut out = Vec::<LargeTileStats>::new();
  let mut stmt = connection
    .prepare("select zoom_level as z, ((1 << zoom_level) - 1 - tile_row) as x, tile_column as y, length(tile_data) from tiles where length(tile_data) > ? order by zoom_level asc;
    ")
    .unwrap();
  stmt.bind(1, threshold as i64).unwrap();
  while let sqlite::State::Row = stmt.next().unwrap() {
    out.push(LargeTileStats {
      z: stmt.read::<i64>(0).unwrap() as u8,
      x: stmt.read::<i64>(1).unwrap() as u64,
      y: stmt.read::<i64>(2).unwrap() as u64,
      tile_data_length: stmt.read::<i64>(3).unwrap() as u32,
    });
  }
  out
}

pub fn calculate_statistics(input: PathBuf) -> StatisticsOutput {
  let connection = sqlite::open(input.to_owned()).unwrap();
  connection.execute("PRAGMA query_only = true;").unwrap();

  let zoom_level_stats = calculate_zoom_level_stats(&connection);

  let thresholds = vec![400_000, 500_000];
  let large_tile_stats: HashMap<u32, Vec<LargeTileStats>> = thresholds
    .iter()
    .map(|threshold| {
      (
        *threshold,
        calculate_large_tile_stats(&connection, *threshold),
      )
    })
    .collect();

  StatisticsOutput {
    name: input.to_str().unwrap().to_string(),
    zoom_level_stats,
    large_tile_stats,
  }
}
