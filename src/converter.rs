pub(crate) use std::{collections::HashMap, path::PathBuf};
use std::{io::prelude::*, thread};
use std::{path::Path, sync::Arc};

use flate2::{write::GzEncoder, Compression};
use walkdir::WalkDir;

use crate::tilebelt;

fn maybe_compress(data: Vec<u8>) -> Vec<u8> {
  if data[0] != 0x1f && data[1] != 0x8b {
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(&data).unwrap();
    let compressed_data = gz.finish().unwrap();
    return compressed_data;
  }
  data
}

fn initialize_processors(
  input: &Path,
  process_queue_rx: crossbeam_channel::Receiver<PathBuf>,
  output_queue_tx: crossbeam_channel::Sender<tilebelt::TileData>,
) -> Vec<thread::JoinHandle<()>> {
  let max_workers = std::cmp::max(num_cpus::get() - 2, 2);
  let mut processor_thread_handles = Vec::with_capacity(max_workers);

  for worker_id in 0..max_workers {
    let thread_input = input.to_path_buf();
    let thread_process_queue_rx = process_queue_rx.clone();
    let thread_output_queue_tx = output_queue_tx.clone();
    processor_thread_handles.push(thread::spawn(move || {
      while let Ok(path) = thread_process_queue_rx.recv() {
        if path.is_dir() {
          continue;
        }
        let extension = path.extension().unwrap();
        if extension != "pbf" && extension != "mvt" {
          // only supports vector tiles right now
          continue;
        }
        let filename = path.strip_prefix(&thread_input).unwrap().to_str().unwrap();
        // println!("Reading {}", filename);
        let parts: Vec<&str> = filename.split(&['/', '.']).collect();
        let z = parts[0].parse::<u32>().unwrap();
        let x = parts[1].parse::<u32>().unwrap();
        let y = parts[2].parse::<u32>().unwrap();
        let tile: tilebelt::Tile = (x, y, z);
        let data = std::fs::read(path).unwrap();
        let compressed_data = maybe_compress(data);
        thread_output_queue_tx
          .send(tilebelt::TileData {
            tile,
            data: Arc::new(compressed_data),
          })
          .unwrap();
      }
      println!("Worker {} finished.", worker_id);
    }));
  }
  processor_thread_handles
}

pub fn convert(input: PathBuf, output: PathBuf) {
  let metadata_json = input.join("metadata.json");
  let mut metadata: HashMap<String, String> = HashMap::new();
  if metadata_json.exists() {
    println!("Found metadata.json, reading...");
    let metadata_str = std::fs::read_to_string(metadata_json).unwrap();
    let metadata_untyped: serde_json::Map<String, serde_json::Value> =
      serde_json::from_str(&metadata_str).unwrap();
    metadata_untyped.into_iter().for_each(|(k, v)| {
      if let Some(vstr) = v.as_str() {
        metadata.insert(k, vstr.to_string());
      } else {
        metadata.insert(k, v.to_string());
      }
    });
  }

  let (process_queue_tx, process_queue_rx) = crossbeam_channel::unbounded::<PathBuf>();
  let (tile_queue_tx, tile_queue_rx) = crossbeam_channel::unbounded::<tilebelt::TileData>();
  let writer_handle = crate::writer::initialize_writer(output, tile_queue_rx, metadata);
  let processor_handles = initialize_processors(&input, process_queue_rx, tile_queue_tx);

  let files = WalkDir::new(input).min_depth(1).max_depth(3);
  for entry in files {
    process_queue_tx
      .send(entry.unwrap().path().to_path_buf())
      .unwrap();
  }
  drop(process_queue_tx);

  writer_handle.join().unwrap();
  for handle in processor_handles {
    handle.join().unwrap();
  }
}
