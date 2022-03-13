use color_eyre::Report;
use structopt::StructOpt;

use std::sync::Arc;

use tokio::sync::RwLock;

mod backends;
mod expression;
mod index;
mod server;
mod utils;

use crate::backends::{Backend, BackendOptions};
use crate::index::Index;

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
        Command::Serve {
            port,
            backend_options,
            read_only,
        } => {
            let backend = backend_options.build();
            let index = backend.load().await.unwrap(); // TODO: Error handling.

            let backend_handle = Arc::new(RwLock::new(backend));
            let index_handle = Arc::new(RwLock::new(index));

            // tokio::spawn(backend_background_task(
            //     backend_handle.clone(),
            //     index_handle.clone(),
            // ));

            server::run_server(port, index_handle, backend_handle, read_only).await?;
        }
        Command::Copy { from, to } => {
            let from_backend = from.build();
            let mut to_backend = to.build();

            tracing::info!("Copying data from {:?} to {:?}", from, to);

            to_backend.clear().await.unwrap();
            to_backend
                .dump(&from_backend.load().await.unwrap())
                .await
                .unwrap()
        }
    }

    Ok(())
}

// async fn interval_background_task(

// ) {
//     let mut interval = tokio::time::interval(std::time::Duration::from_millis(10 * 1_000));
//     loop {
//         tokio::select! {
//             _ = crate::utils::shutdown_signal("Backend task") => {},
//             _ = interval.tick() => {
//                 let mut backend = backend_handle.as_ref().write().await;
//                 let index = index_handle.as_ref().read().await;
//                 backend.dump(&index).await.unwrap(); // TODO: Error handling.
//                 tracing::info!("Flushed index");
//             }
//         }
//     }
// }
