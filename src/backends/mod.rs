use async_trait::async_trait;

use std::collections::HashMap;
use std::str::FromStr;

use crible_lib::{Encoder, Index};

mod fs;
mod memory;
mod redis;

pub use self::fs::FSBackend;
pub use self::memory::Memory;
pub use self::redis::Redis;

#[async_trait]
pub trait Backend: Send + Sync + std::fmt::Debug {
    async fn load(&self) -> Result<Index, eyre::Report>;
    async fn dump<'a>(&self, index: &Index) -> Result<(), eyre::Report>;
    async fn clear(&self) -> Result<(), eyre::Report>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum BackendOptions {
    Memory,
    Fs { path: std::path::PathBuf, encoder: Encoder },
    Redis { url: url::Url, key: String },
}

impl FromStr for BackendOptions {
    type Err = eyre::Report;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut url = url::Url::parse(value)?;
        let query_pairs =
            url.query_pairs().into_owned().collect::<HashMap<String, String>>();
        match url.scheme() {
            "fs" => {
                let path = crate::utils::single_path_from_url(&url)?
                    .unwrap_or_else(|| "data.bin".into());
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
                        .get("key")
                        .cloned()
                        .unwrap_or_else(|| "crible".to_owned()),
                })
            }
            x => Err(eyre::Report::msg(format!("Unknown scheme: {:?}", x))),
        }
    }
}

impl BackendOptions {
    pub fn build(&self) -> Result<Box<dyn Backend>, eyre::Report> {
        Ok(match self {
            Self::Memory => Box::new(Memory::default()),
            Self::Fs { path, encoder } => {
                Box::new(FSBackend::new(path, *encoder))
            }
            Self::Redis { url, key } => Box::new(Redis::new(url, key.clone())?),
        })
    }
}
