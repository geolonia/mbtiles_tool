mod reader;
mod subdivide;
mod tilebelt;

use clap::{Parser, Subcommand};
use std::io;
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[clap(
  name = "mbtiles_tool",
  about = "A tool for working with mbtiles archives",
  version
)]
struct Cli {
  #[clap(subcommand)]
  command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
  #[clap(
    name = "subdivide",
    about = "Subdivide a mbtiles archive into smaller archives on tile boundaries"
  )]
  Subdivide {
    /// Subdivision configuration file
    #[clap(value_parser)]
    config: PathBuf,

    /// Input
    #[clap(value_parser)]
    input: PathBuf,

    /// Output
    #[clap(value_parser)]
    output: PathBuf,
  },
}

fn main() {
  let args = Cli::parse();
  match args.command {
    Commands::Subdivide {
      config,
      input,
      output,
    } => {
      // fail if input file does not exist
      if !input.exists() {
        panic!("Input file does not exist");
      }

      // ask if we should overwrite the output directory
      if output.exists() {
        print!("Output directory already exists. Overwrite? (y/n) ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        if input.trim() != "y" {
          panic!("Aborted");
        }
        // remove the output directory
        std::fs::remove_dir_all(&output).unwrap();
      }
      std::fs::create_dir(&output).unwrap();

      subdivide::subdivide(config, input, output);
    }
  }
}
