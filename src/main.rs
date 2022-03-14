#![deny(unstable_features)]
#![warn(
    clippy::print_stdout,
    clippy::mut_mut,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications
)]

use color_eyre::Report;
use structopt::StructOpt;

use std::sync::Arc;

use tokio::sync::RwLock;

mod backends;
mod expression;
mod index;
mod server;
mod utils;

use crate::backends::BackendOptions;

#[derive(Debug, StructOpt)]
#[structopt(name = "crible")]
enum Command {
    /// Run the server.
    Serve {
        /// Backend configuration url
        #[structopt(long = "backend", default_value = "memory://")]
        backend_options: BackendOptions,

        /// Disable all write operations.
        #[structopt(long)]
        read_only: bool,

        #[structopt(short, long, default_value = "3000")]
        port: u16,
    },
    /// Copy data from one backend to another
    Copy {
        /// Source backend configuration url
        #[structopt(long)]
        from: BackendOptions,

        /// Destination backend configuration url
        #[structopt(long)]
        to: BackendOptions,
    },
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    utils::setup_logging();

    match Command::from_args() {
        Command::Serve { port, backend_options, read_only } => {
            let backend = backend_options.build().unwrap();
            let index = backend.load().await.unwrap(); // TODO: Error handling.

            let backend_handle = Arc::new(RwLock::new(backend));
            let index_handle = Arc::new(RwLock::new(index));

            server::run_server(port, index_handle, backend_handle, read_only)
                .await?;
        }
        Command::Copy { from, to } => {
            let from_backend = from.build().unwrap();
            let mut to_backend = to.build().unwrap();

            tracing::info!("Copying data from {:?} to {:?}", from, to);

            to_backend.clear().await.unwrap();
            to_backend.dump(&from_backend.load().await.unwrap()).await.unwrap()
        }
    }

    Ok(())
}
