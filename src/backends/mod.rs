use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

use crible_lib::{Encoder, Index};
use url::{Host, Url};

mod fs;
mod memory;
mod redis;

pub use self::fs::FSBackend;
pub use self::memory::Memory;
pub use self::redis::Redis;

static DEFAULT_FS_LOCATION: &str = "data.bin";
static DEFAULT_REDIS_PREFIX: &str = "crible";

// Munge a url in a filesystem path.
// This is not great and makes many, likely wrong assumptions about paths but it
// allows a consistent and fairly ergonomic interface between backends.
fn single_path_from_url(url: &Url) -> Result<Option<PathBuf>, eyre::Report> {
    let mut parts = PathBuf::new();

    if let Some(host) = url.host() {
        match host {
            Host::Domain(d) => parts.push(d),
            _ => {
                return Err(eyre::Report::msg(format!(
                    "Cannot extract single path from {:?}",
                    url
                )));
            }
        }
    }

    let raw_path = &url.path();
    if raw_path.len() > 1 {
        // Drop leading /
        parts.push(&raw_path[1..]);
    }

    if parts.as_os_str().is_empty() { Ok(None) } else { Ok(Some(parts)) }
}

pub trait Backend: Send + Sync + std::fmt::Debug {
    fn load(&self) -> Result<Index, eyre::Report>;
    fn dump(&self, index: &Index) -> Result<(), eyre::Report>;
    fn clear(&self) -> Result<(), eyre::Report>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendOptions {
    Memory,
    Fs { path: PathBuf, encoder: Encoder },
    Redis { url: Url, key: String },
}

impl FromStr for BackendOptions {
    type Err = eyre::Report;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut url = url::Url::parse(value)?;

        let query_pairs =
            url.query_pairs().into_owned().collect::<HashMap<String, String>>();

        match url.scheme() {
            "fs" | "file" => {
                let path = single_path_from_url(&url)?
                    .unwrap_or_else(|| DEFAULT_FS_LOCATION.into());
                let encoder = match query_pairs.get("format") {
                    None => match path.extension() {
                        None => Encoder::Bin,
                        Some(ext) => match ext.to_str() {
                            Some(x) => {
                                Encoder::from_str(x).unwrap_or(Encoder::Bin)
                            }
                            None => {
                                return Err(eyre::Report::msg(format!(
                                    "Invalid path {:?}",
                                    &path
                                )));
                            }
                        },
                    },
                    Some(format_str) => Encoder::from_str(format_str.as_ref())?,
                };

                Ok(BackendOptions::Fs { path, encoder })
            }
            "memory" => Ok(BackendOptions::Memory),
            "redis" => {
                url.set_query(None);
                Ok(BackendOptions::Redis {
                    url,
                    key: query_pairs
                        .get("prefix")
                        .cloned()
                        .unwrap_or_else(|| DEFAULT_REDIS_PREFIX.into()),
                })
            }
            x => Err(eyre::Report::msg(format!("Unknown scheme: {:?}", x))),
        }
    }
}

impl BackendOptions {
    pub fn build(&self) -> Result<Box<dyn Backend>, eyre::Report> {
        Ok(match self {
            Self::Memory => Box::<Memory>::default(),
            Self::Fs { path, encoder } => {
                Box::new(FSBackend::new(path, *encoder))
            }
            Self::Redis { url, key } => Box::new(Redis::new(url, key.clone())?),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::*;
    use url::Url;

    use super::{single_path_from_url, BackendOptions};

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

    #[test]
    fn test_memory_option() {
        assert_eq!(
            BackendOptions::Memory,
            BackendOptions::from_str("memory://").unwrap(),
        )
    }

    #[test]
    fn test_redis_option() {
        assert_eq!(
            BackendOptions::Redis {
                key: "crible2".into(),
                url: url::Url::from_str("localhost:4444/2").unwrap(),
            },
            BackendOptions::from_str("redis://localhost:4444/2?prefix=crible2")
                .unwrap(),
        )
    }
}
