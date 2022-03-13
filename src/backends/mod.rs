use async_trait::async_trait;

use std::str::FromStr;

use crate::index::Index;

mod binfs;
mod jsonfs;
mod memory;

pub use self::binfs::BinFSBackend;
pub use self::jsonfs::JsonFSBackend;
pub use self::memory::MemoryBackend;

#[async_trait]
pub trait Backend {
    async fn load(&self) -> Result<Index, eyre::Report>;
    async fn dump<'a>(&mut self, index: &Index) -> Result<(), eyre::Report>;
    async fn clear(&mut self) -> Result<(), eyre::Report>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum BackendOptions {
    Memory,
    Bin(Option<std::path::PathBuf>),
    Json(Option<std::path::PathBuf>),
}

fn single_path_from_url(url: &url::Url) -> Result<Option<std::path::PathBuf>, eyre::Report> {
    match (url.host(), url.path()) {
        (None, "") => Ok(None),
        (Some(host), "") => match host {
            url::Host::Domain(d) => Ok(Some(d.into())),
            _ => Err(eyre::Report::msg(format!(
                "Cannot extrat single path from {:?}",
                url
            ))),
        },
        (_, path) => {
            let path = &path[1..]; // Drop leading /
            if path.is_empty() {
                Ok(None)
            } else {
                Ok(Some(path.into()))
            }
        }
    }
}

impl FromStr for BackendOptions {
    type Err = eyre::Report;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let url = url::Url::parse(value)?;
        match url.scheme() {
            "fs" => Ok(BackendOptions::Bin(single_path_from_url(&url)?)),
            "json" => Ok(BackendOptions::Json(single_path_from_url(&url)?)),
            "memory" => Ok(BackendOptions::Memory),
            x => Err(eyre::Report::msg(format!("Unknown scheme: {:?}", x))),
        }
    }
}

impl BackendOptions {
    pub fn build(&self) -> Box<dyn Backend + Send + Sync> {
        match self {
            Self::Memory => Box::new(MemoryBackend::default()),
            Self::Bin(p) => Box::new(match p {
                None => BinFSBackend::default(),
                Some(x) => BinFSBackend::new(x),
            }),
            Self::Json(p) => Box::new(match p {
                None => JsonFSBackend::default(),
                Some(x) => JsonFSBackend::new(x),
            }),
        }
    }
}
