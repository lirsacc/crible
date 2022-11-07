#![deny(unstable_features)]
#![forbid(unsafe_code)]
#![warn(
    clippy::mut_mut,
    clippy::large_types_passed_by_value,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications
)]

mod backends;
mod executor;
mod operations;
mod server;
mod utils;

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use color_eyre::Report;
use crible_lib::expression::Expression;
use eyre::Context;
use parking_lot::{Mutex, RwLock};
use shadow_rs::shadow;

use crate::backends::BackendOptions;
use crate::executor::ExecutorBuilder;

shadow!(build);

#[cfg(not(debug_assertions))]
const _DEFAULT_DEBUG: bool = false;
#[cfg(debug_assertions)]
const _DEFAULT_DEBUG: bool = true;

#[derive(Subcommand, Debug)]
#[clap(version, about, long_about = None)]
enum Command {
    /// Run the server.
    Serve {
        /// Backend configuration url.
        #[clap(long = "backend", required = true, env = "CRIBLE_BACKEND")]
        backend_options: BackendOptions,

        #[clap(
            short = 'l',
            long = "listen",
            env = "CRIBLE_BIND",
            default_value = "127.0.0.1:3000"
        )]
        bind: String,

        /// Disable all write operations.
        #[clap(long, env = "CRIBLE_READ_ONLY")]
        read_only: bool,

        /// Refresh interval in milliseconds.
        #[clap(long = "refresh", env = "CRIBLE_REFRESH_TIMEOUT")]
        refresh_timeout: Option<u64>,

        /// Number of execuotor threads. Defaults to the number of CPU cores
        /// available if unspecified.
        #[clap(short = 't', long = "threads", env = "CRIBLE_THREAD_COUNT")]
        thread_count: Option<usize>,

        /// the maximum number of requests that can be put in the queue.
        /// Requests that exceed this limit are rejected with 429 HTTP status.
        #[clap(
            short = 'q',
            long = "queue-size",
            env = "CRIBLE_REQUEST_QUEUE_SIZE"
        )]
        queue_size: Option<usize>,

        /// TCP keep-alive setting in seconds. If unspecified keep alive is
        /// disabled.
        #[clap(
            short = 'k',
            long = "tcp-keep-alive",
            env = "CRIBLE_TCP_KEEP_ALIVE"
        )]
        keep_alive: Option<u64>,
    },
    /// Execute a single query against the index.
    Query {
        /// Backend configuration url.
        #[clap(long = "backend", required = true, env = "CRIBLE_BACKEND")]
        backend_options: BackendOptions,

        #[clap(long)]
        query: Expression,
    },
    /// Copy data from one backend to another.
    Copy {
        /// Source backend configuration url.
        #[clap(long)]
        from: BackendOptions,

        /// Destination backend configuration url.
        #[clap(long)]
        to: BackendOptions,
    },
}


#[derive(Parser)]
#[clap(version, about, long_about = None, long_version = build::CLAP_LONG_VERSION)]
pub struct App {
    /// Enable debug logging
    #[clap(short, long, env = "CRIBLE_DEBUG")]
    debug: Option<bool>,

    #[clap(subcommand)]
    command: Command,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Report> {
    let app = App::parse();
    crate::utils::setup_logging(app.debug.unwrap_or(_DEFAULT_DEBUG));
    match &app.command {
        Command::Serve {
            bind,
            backend_options,
            read_only,
            refresh_timeout,
            thread_count,
            queue_size,
            keep_alive,
        } => {
            let addr: SocketAddr = bind
                .parse()
                .wrap_err_with(|| format!("Invalid bind `{}`", &bind))?;

            let backend =
                backend_options.build().wrap_err("Invalid backend")?;

            let index = backend.load().wrap_err("Failed to load index")?;

            let executor = {
                let mut executor_builder = ExecutorBuilder::new(
                    Arc::new(RwLock::new(index)),
                    Arc::new(Mutex::new(backend)),
                )
                .read_only(*read_only);

                if let Some(c) = thread_count {
                    executor_builder = executor_builder.pool_size(*c);
                }

                if let Some(c) = queue_size {
                    executor_builder = executor_builder.queue_size(*c);
                }

                // TODO: Unwrap
                executor_builder.build().unwrap()
            };

            let state = server::State::new(executor);

            if let Some(interval) = refresh_timeout {
                if !read_only {
                    tracing::warn!(
                        "Background refresh enabled in write mode. This is \
                            generally unsafe as backends are not guaranteed to \
                            be transactional."
                    );
                }
                tokio::spawn(server::run_refresh_task(
                    state.clone(),
                    std::time::Duration::from_millis(*interval),
                ));
            }

            tracing::info!("Starting server on port {:?}", addr);

            server::run(
                &addr,
                keep_alive.map(std::time::Duration::from_secs),
                state,
            )
            .await?;

            Ok(())
        }
        Command::Query { backend_options, query } => {
            let backend =
                backend_options.build().wrap_err("Invalid backend")?;
            let index = backend.load().wrap_err("Failed to load index")?;

            let res = index.execute(query)?;

            let stdout = std::io::stdout();
            let mut buffer = std::io::BufWriter::new(stdout.lock());

            for x in res.iter() {
                writeln!(buffer, "{}", x)?;
            }
            Ok(())
        }
        Command::Copy { from, to } => {
            let from_backend =
                from.build().wrap_err("Invalid source backend")?;
            let to_backend =
                to.build().wrap_err("Invalid destination backend")?;
            to_backend.clear()?;

            let mut index =
                from_backend.load().wrap_err("Failed to load index")?;

            index.optimize();

            to_backend.dump(&index).wrap_err("Failed to dump index")?;
            Ok(())
        }
    }
}
