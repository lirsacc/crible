#[macro_use]
extern crate pest_derive;
#[macro_use]
extern crate serde_derive;

mod backend;
mod expressions;
mod search_index;
mod server;
mod utils;

use std::net::SocketAddr;
use std::sync::Arc;

use log::info;
use structopt::StructOpt;
use tokio::sync::RwLock;

use crate::backend::{import_csv, FSBackend, TimedBackend};
use crate::search_index::SearchIndex;
use crate::server::{run_server, run_writer};

#[derive(Debug, StructOpt)]
#[structopt(name = "crible")]
enum Command {
    /// Import a CSV file and convert it to crible local file format.
    ImportCSV {
        /// Source CSV file.
        #[structopt(short, long, parse(from_os_str))]
        input: std::path::PathBuf,
        /// Where to store the generated database. If pointing to an existing
        /// database, this will override its content.
        #[structopt(short, long, parse(from_os_str))]
        output: std::path::PathBuf,
        /// Do not delete dimensions that are missing from the input file. This
        /// allows importing multiple CSV files separately.
        #[structopt(long)]
        incremental: bool,
    },
    /// Run the crible server.
    Serve {
        #[structopt(long, parse(from_os_str))]
        database: std::path::PathBuf,
        #[structopt(short, long, default_value = "127.0.0.1:5000")]
        bind: SocketAddr,
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
            output,
            incremental,
        } => {
            let backend = FSBackend::new(output);
            let mut file = std::fs::File::open(&input).unwrap();
            import_csv(&mut file, &backend, !incremental).unwrap();
        }
        Command::Serve { database, bind } => {
            let backend: TimedBackend<FSBackend> =
                TimedBackend::wrap(FSBackend::new(database));
            let index = SearchIndex::from_backend(&backend).unwrap();
            let state = Arc::new(RwLock::new(index));
            info!("Starting background saving task");
            run_writer(state.clone(), Arc::new(backend));
            info!("Starting server on {}", bind);
            run_server(bind, state).await;
        }
    }
}
