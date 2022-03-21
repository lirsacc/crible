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
use tracing::Instrument;

use std::io::Write;
use std::net::SocketAddr;

use crible_lib::expression::Expression;

mod backends;
mod server;
mod utils;

use crate::backends::BackendOptions;

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

        /// Refresh interval in milliseconds
        #[clap(long = "refresh", env = "CRIBLE_REFRESH_TIMEOUT")]
        refresh_timeout: Option<u64>,

        /// Flush interval in milliseconds. 0 or absent means flush on write;
        /// incompatible with --read-only.
        #[clap(long = "flush", env = "CRIBLE_FLUSH_TIMEOUT")]
        flush_timeout: Option<u64>,
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

#[tokio::main]
async fn main() -> Result<(), Report> {
    let cli = Cli::parse();

    utils::setup_logging(cli.debug.unwrap_or(_DEFAULT_DEBUG));

    match &cli.command {
        Command::Serve {
            bind,
            backend_options,
            read_only,
            refresh_timeout,
            flush_timeout,
        } => {
            let addr: SocketAddr = bind.parse().expect("Invalid bind");

            let in_write_mode = flush_timeout.is_some() || !read_only;
            let flush_on_write = flush_timeout.map_or(!*read_only, |x| x == 0);

            let backend = backend_options.build().unwrap();
            let index = backend
                .load()
                .instrument(tracing::info_span!("load_index"))
                .await
                .unwrap();

            let state =
                server::State::new(index, backend, *read_only, flush_on_write);

            if let Some(interval) = refresh_timeout {
                if in_write_mode {
                    tracing::warn!(
                        "Background refresh enabled in write mode. This is generally unsafe as backends are not guaranteed to be transactional."
                    );
                }
                tokio::spawn(server::run_refresh_task(
                    state.clone(),
                    std::time::Duration::from_millis(*interval),
                ));
            }

            if in_write_mode && !flush_on_write {
                tokio::spawn(server::run_flush_task(
                    state.clone(),
                    std::time::Duration::from_millis(flush_timeout.unwrap()),
                ));
            }

            tracing::info!("Starting server on port {:?}", addr);

            server::run(&addr, state).await?;

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
            let mut index = from_backend
                .load()
                .instrument(tracing::debug_span!("load_index"))
                .await
                .unwrap();

            index.optimize();

            to_backend
                .dump(&index)
                .instrument(tracing::debug_span!("dump_index"))
                .await
                .unwrap();
            Ok(())
        }
    }
}
