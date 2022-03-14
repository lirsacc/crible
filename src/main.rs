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

use clap::{Parser, Subcommand};
use color_eyre::Report;

use std::io::Write;
use std::sync::Arc;

use tokio::sync::RwLock;

mod backends;
mod expression;
mod index;
mod server;
mod utils;

use crate::backends::{Backend, BackendOptions};
use crate::expression::Expression;
use crate::index::Index;

#[derive(Parser)]
#[clap(version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
#[clap(version, about, long_about = None)]
enum Command {
    /// Run the server.
    Serve {
        /// Backend configuration url
        #[clap(
            long = "backend",
            env = "CRIBLE_BACKEND",
            default_value = "memory://"
        )]
        backend_options: BackendOptions,

        #[clap(short, long, env = "CRIBLE_PORT", default_value = "3000")]
        port: u16,

        /// Disable all write operations.
        #[clap(long, env = "CRIBLE_READ_ONLY")]
        read_only: bool,

        /// Refresh interval in milliseconds
        #[clap(long = "refresh", env = "CRIBLE_REFRESH")]
        refresh_timeout: Option<u64>,
    },
    /// Execute a single query against the index.
    Query {
        /// Backend configuration url
        #[clap(
            long = "backend",
            env = "CRIBLE_BACKEND",
            default_value = "memory://"
        )]
        backend_options: BackendOptions,

        #[clap(long)]
        query: Expression,
    },
    /// Copy data from one backend to another
    Copy {
        /// Source backend configuration url
        #[clap(long)]
        from: BackendOptions,

        /// Destination backend configuration url
        #[clap(long)]
        to: BackendOptions,
    },
}

async fn refresh_index(
    backend_handle: Arc<RwLock<Box<dyn Backend + Send + Sync>>>,
    index_handle: Arc<RwLock<Index>>,
    every: std::time::Duration,
) {
    let mut interval = tokio::time::interval(every);
    loop {
        tokio::select! {
            _ = crate::utils::shutdown_signal("Backend task") => {
                break;
            },
            _ = interval.tick() => {
                tracing::debug!("Refreshing index");
                match backend_handle.as_ref().write().await.load().await {
                    Ok(new_index) => {
                        let mut index = index_handle.as_ref().write().await;
                        *index = new_index;
                        tracing::info!("Refreshed index");
                    }
                    Err(e) => {
                        tracing::error!("Failed to load index data: {}", e);
                    },
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    utils::setup_logging();

    let cli = Cli::parse();

    match &cli.command {
        Command::Serve {
            port,
            backend_options,
            read_only,
            refresh_timeout,
        } => {
            let backend = backend_options.build().unwrap();
            let index = backend.load().await.unwrap();

            let backend_handle = Arc::new(RwLock::new(backend));
            let index_handle = Arc::new(RwLock::new(index));

            if let Some(interval) = refresh_timeout {
                if !read_only {
                    tracing::warn!("Background refresh enabled in write mode.");
                }
                tokio::spawn(refresh_index(
                    backend_handle.clone(),
                    index_handle.clone(),
                    std::time::Duration::from_millis(*interval),
                ));
            }

            server::run_server(*port, index_handle, backend_handle, *read_only)
                .await?;

            Ok(())
        }
        Command::Query { backend_options, query } => {
            let backend = backend_options.build().unwrap();
            let index = backend.load().await.unwrap();

            let res = index.execute(query)?;

            let stdout = std::io::stdout();
            let mut buffer = std::io::BufWriter::new(stdout.lock());

            for x in res.iter() {
                writeln!(buffer, "{}", x)?;
            }
            Ok(())
        }
        Command::Copy { from, to } => {
            let from_backend = from.build().unwrap();
            let mut to_backend = to.build().unwrap();
            to_backend.clear().await.unwrap();
            to_backend.dump(&from_backend.load().await.unwrap()).await.unwrap();
            Ok(())
        }
    }
}
