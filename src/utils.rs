use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use tokio::signal;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn set_env_var_default(name: &str, default: &str) {
    if std::env::var(name).is_err() {
        std::env::set_var(name, default);
    }
}

pub fn setup_logging(debug: bool) {
    if debug {
        set_env_var_default("RUST_LIB_BACKTRACE", "1");
        set_env_var_default("RUST_BACKTRACE", "1");
        set_env_var_default("RUST_LOG", "info,crible=debug,crible_lib=debug");
    } else {
        set_env_var_default("RUST_LOG", "warn,crible=info,crible_lib=info");
    }

    if debug {
        color_eyre::install().unwrap();
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(
                tracing_subscriber::fmt::layer()
                    .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer().json().with_span_list(true))
            .init();
    }
}

pub async fn shutdown_signal(ctx: &'static str) {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install TERM signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {
            tracing::warn!("Ctrl+C received, starting graceful shutdown for {}", ctx);
        },
        _ = terminate => {
            tracing::warn!("TERM received, starting graceful shutdown for {}", ctx);
        },
    }
}

pub fn add_extension<T: AsRef<OsStr>>(path: &mut PathBuf, extension: T) {
    match path.extension() {
        Some(ext) => {
            let mut ext = ext.to_os_string();
            ext.push(".");
            ext.push(extension.as_ref());
            path.set_extension(ext)
        }
        None => path.set_extension(extension.as_ref()),
    };
}

pub fn tmp_path<T: AsRef<Path>>(path: &T) -> PathBuf {
    let mut pb = path.as_ref().to_path_buf();
    add_extension(&mut pb, "tmp");
    pb
}
