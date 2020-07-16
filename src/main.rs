#[macro_use]
extern crate pest_derive;
#[macro_use]
extern crate serde_derive;

mod error;
mod expressions;
mod index;
mod server;
mod utils;

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use log::{info, warn};
use structopt::StructOpt;

use crate::error::CribleError;
use crate::index::{import_csv, FSIndex, Index, MemoryIndex, VerboseIndex};
use crate::server::{run_server, run_writer};

#[derive(Debug)]
enum IndexDef {
    Memory,
    FS(std::path::PathBuf),
}

impl FromStr for IndexDef {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !value.contains("://") {
            Err("Invalid index type (missing protocol)".to_string())
        } else {
            let parts: Vec<&str> = value.split("://").collect();
            if parts.len() > 2 || parts.is_empty() {
                Err("Invalid index type".to_string())
            } else {
                let protocol = parts.get(0).unwrap();
                let rest = parts.get(1);
                match (*protocol, rest) {
                    ("memory", None) | ("memory", Some(&"")) => Ok(IndexDef::Memory),
                    ("memory", Some(x)) => {
                        Err(format!("Memory index does not accept parameter. Received '{}'", x))
                    }
                    ("fs", None) | ("fs", Some(&"")) => Ok(IndexDef::FS("crible-data".into())),
                    ("fs", Some(x)) => Ok(IndexDef::FS(x.into())),
                    (x, _) => Err(format!("Unknown index protocol {}", x)),
                }
            }
        }
    }
}

fn make_index(
    index_def: IndexDef,
    clear: bool,
    verbose: bool,
) -> Result<Box<dyn Index + Send + Sync>, CribleError> {
    match index_def {
        IndexDef::Memory => {
            warn!("Using the memory backend! Writes will not persists across restarts.");
            Ok(if verbose {
                Box::new(VerboseIndex::new(MemoryIndex::new()))
            } else {
                Box::new(MemoryIndex::new())
            })
        }
        IndexDef::FS(directory) => {
            let inner = if clear {
                FSIndex::new(directory)
            } else {
                FSIndex::load(directory)?
            };

            Ok(if verbose {
                Box::new(VerboseIndex::new(inner))
            } else {
                Box::new(inner)
            })
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "crible")]
enum Command {
    /// Import a CSV file and convert it to crible local file format.
    ImportCSV {
        /// Source CSV file.
        #[structopt(long, parse(from_os_str))]
        input: std::path::PathBuf,
        #[structopt(long, default_value = "memory://")]
        index: IndexDef,
        #[structopt(long)]
        verbose: bool,
        #[structopt(long)]
        clear: bool,
    },
    /// Run the crible server.
    Serve {
        #[structopt(short, long, default_value = "127.0.0.1:5000")]
        bind: SocketAddr,
        #[structopt(long, default_value = "memory://")]
        index: IndexDef,
        #[structopt(long)]
        verbose: bool,
    },
}

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "crible=info");
    }
    pretty_env_logger::init_timed();

    match Command::from_args() {
        Command::ImportCSV {
            input,
            index,
            verbose,
            clear,
        } => {
            let mut file = std::fs::File::open(&input).unwrap();
            let index = make_index(index, clear, verbose).unwrap();
            if clear {
                index.clear().unwrap();
                index.save().unwrap();
            }

            if verbose {
                crate::utils::timed_cb(
                    || import_csv(&mut file, &*index).unwrap(),
                    |d| {
                        info!("Read CSV data in {:?}", d);
                        info!(
                            "Index stats: {:?}, {} facets",
                            index.stats().unwrap(),
                            index.len().unwrap()
                        );
                    },
                )
            } else {
                import_csv(&mut file, &*index).unwrap();
            }
        }
        Command::Serve {
            index,
            verbose,
            bind,
        } => {
            let index = make_index(index, false, verbose).unwrap();
            let state = Arc::new(index);
            info!("Starting background saving task");
            run_writer(
                state.clone(),
                std::time::Duration::from_millis(1000),
                3,
            );
            info!("Starting server on {}", bind);
            run_server(bind, state).await;
        }
    }
}
