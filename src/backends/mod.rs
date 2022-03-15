use async_trait::async_trait;

use std::collections::HashMap;
use std::str::FromStr;

use crate::index::Index;

mod jsonfs;
mod memory;
mod redis;

pub use self::jsonfs::JsonFSBackend;
pub use self::memory::MemoryBackend;
pub use self::redis::RedisBackend;

#[async_trait]
pub trait Backend: Send + Sync + std::fmt::Debug {
    async fn load(&self) -> Result<Index, eyre::Report>;
    async fn dump<'a>(&mut self, index: &Index) -> Result<(), eyre::Report>;
    async fn clear(&mut self) -> Result<(), eyre::Report>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum BackendOptions {
    Memory,
    Json(Option<std::path::PathBuf>),
    Redis { url: url::Url, key: Option<String> },
}

fn single_path_from_url(
    url: &url::Url,
) -> Result<Option<std::path::PathBuf>, eyre::Report> {
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
        let mut url = url::Url::parse(value)?;
        match url.scheme() {
            "json" => Ok(BackendOptions::Json(single_path_from_url(&url)?)),
            "memory" => Ok(BackendOptions::Memory),
            "redis" => {
                let query_pairs = url
                    .query_pairs()
                    .into_owned()
                    .collect::<HashMap<String, String>>();
                url.set_query(None);
                let key = query_pairs.get("key").map(|x| x.to_owned());
                Ok(BackendOptions::Redis { url, key })
            }
            x => Err(eyre::Report::msg(format!("Unknown scheme: {:?}", x))),
        }
    }
}

impl BackendOptions {
    pub fn build(&self) -> Result<Box<dyn Backend>, eyre::Report> {
        Ok(match self {
            Self::Memory => Box::new(MemoryBackend::default()),
            Self::Json(p) => Box::new(match p {
                None => JsonFSBackend::default(),
                Some(x) => JsonFSBackend::new(x),
            }),
            Self::Redis { url, key } => {
                Box::new(RedisBackend::new(url, key.to_owned())?)
            }
        })
    }
}
