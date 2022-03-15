#![deny(unstable_features)]
#![forbid(unsafe_code)]
#![warn(
    clippy::print_stdout,
    clippy::mut_mut,
    clippy::large_types_passed_by_value,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications
)]

use clap::{Parser, Subcommand};
use color_eyre::Report;
use tokio::sync::RwLock;
use tracing::Instrument;

use std::io::Write;
use std::sync::Arc;

mod backends;
mod expression;
mod index;
mod server;
mod utils;

use crate::backends::{Backend, BackendOptions};
use crate::expression::Expression;
use crate::index::Index;

#[cfg(not(debug_assertions))]
const _DEFAULT_DEBUG: bool = false;
#[cfg(debug_assertions)]
const _DEFAULT_DEBUG: bool = true;

#[derive(Parser)]
#[clap(version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    /// Enable debug logging
    #[clap(short, long, env = "CRIBLE_DEBUG")]
    debug: Option<bool>,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
#[clap(version, about, long_about = None)]
enum Command {
    /// Run the server.
    Serve {
        /// Backend configuration url
        #[clap(long = "backend", required = true, env = "CRIBLE_BACKEND")]
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
        #[clap(long = "backend", required = true, env = "CRIBLE_BACKEND")]
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

async fn run_refresh_task(
    backend_handle: Arc<RwLock<Box<dyn Backend>>>,
    index_handle: Arc<RwLock<Index>>,
    every: std::time::Duration,
) {
    tracing::info!(
        "Starting refresh task. Will update backend every {:?}.",
        every
    );

    let mut interval = tokio::time::interval(every);

    loop {
        tokio::select! {
            _ = crate::utils::shutdown_signal("Backend task") => {
                break;
            },
            _ = interval.tick() => {
                async {
                    match backend_handle
                        .as_ref()
                        .write()
                        .await
                        .load()
                        .instrument(tracing::info_span!("load_index"))
                        .await
                    {
                        Ok(new_index) => {
                            let mut index = index_handle.as_ref().write().await;
                            *index = new_index;
                        }
                        Err(e) => {
                            tracing::error!("Failed to load index data: {}", e);
                        }
                    }
                }
                .instrument(tracing::info_span!("refresh_index"))
                .await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    let cli = Cli::parse();

    utils::setup_logging(cli.debug.unwrap_or(_DEFAULT_DEBUG));

    match &cli.command {
        Command::Serve {
            port,
            backend_options,
            read_only,
            refresh_timeout,
        } => {
            let backend = backend_options.build().unwrap();
            let index = backend
                .load()
                .instrument(tracing::info_span!("load_index"))
                .await
                .unwrap();

            let backend_handle = Arc::new(RwLock::new(backend));
            let index_handle = Arc::new(RwLock::new(index));

            if let Some(interval) = refresh_timeout {
                if !read_only {
                    tracing::warn!("Background refresh enabled in write mode");
                }
                tokio::spawn(run_refresh_task(
                    backend_handle.clone(),
                    index_handle.clone(),
                    std::time::Duration::from_millis(*interval),
                ));
            }

            tracing::info!("Starting server on port {:?}", port);

            server::run(*port, index_handle, backend_handle, *read_only)
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
            to_backend
                .dump(
                    &from_backend
                        .load()
                        .instrument(tracing::debug_span!("load_index"))
                        .await
                        .unwrap(),
                )
                .instrument(tracing::debug_span!("dump_index"))
                .await
                .unwrap();
            Ok(())
        }
    }
}
