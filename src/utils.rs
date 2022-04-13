use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use tokio::signal;
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};
use url::{Host, Url};

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

// Munge a url in a filesystem path.
// This is not great and makes many, likely wrong assumptions about paths but it
// allows a consistent and fairly ergonomic interface between backends.
// Handling of explicit file:// urls would be nice as well.
pub fn single_path_from_url(
    url: &Url,
) -> Result<Option<PathBuf>, eyre::Report> {
    let mut parts = PathBuf::new();

    if let Some(host) = url.host() {
        match host {
            Host::Domain(d) => parts.push(d),
            _ => {
                return Err(eyre::Report::msg(format!(
                    "Cannot extract single path from {:?}",
                    url
                )))
            }
        }
    }

    let raw_path = &url.path();
    if raw_path.len() > 1 {
        // Drop leading /
        parts.push(&raw_path[1..]);
    }

    if parts.as_os_str().is_empty() {
        Ok(None)
    } else {
        Ok(Some(parts))
    }
}

#[cfg(test)]
mod tests {
    use super::single_path_from_url;
    use rstest::*;

    use std::str::FromStr;
    use url::Url;

    #[rstest]
    #[case("fs://index.bin", Some("index.bin"))]
    #[case("fs://index.bin/", Some("index.bin"))]
    #[case("fs://datasets/index.bin", Some("datasets/index.bin"))]
    #[case("fs://datasets.com/index.bin", Some("datasets.com/index.bin"))]
    fn test_single_path_from_url(
        #[case] value: &str,
        #[case] expected: Option<&str>,
    ) {
        let url: Url = Url::from_str(value).unwrap();
        assert_eq!(
            single_path_from_url(&url).unwrap(),
            expected.map(|x| x.into())
        );
    }
}
