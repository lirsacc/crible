use std::collections::HashMap;

use crible_lib::index::Index;
use croaring::Bitmap;
use eyre::Context;
use redis::Commands;

use super::Backend;

#[derive(Debug)]
pub struct Redis {
    client: redis::Client,
    key: String,
}

impl Redis {
    pub fn new(url: &url::Url, key: String) -> Result<Self, eyre::Report> {
        Ok(Self {
            client: redis::Client::open(url.to_string()).wrap_err_with(
                || format!("Failed to create Redis client for `{}`", &url),
            )?,
            key,
        })
    }
}

impl Backend for Redis {
    fn dump<'a>(&self, index: &Index) -> Result<(), eyre::Report> {
        let mut pipe = redis::pipe();
        for (k, v) in index.inner() {
            pipe.hset(&self.key, k, v.serialize());
        }
        let mut con = self.client.get_connection()?;
        pipe.query(&mut con)?;
        Ok(())
    }

    fn load(&self) -> Result<Index, eyre::Report> {
        let mut con = self.client.get_connection()?;
        let data: HashMap<String, Vec<u8>> = con.hgetall(&self.key)?;
        Ok(Index::new(
            data.iter()
                .map(|(k, v)| (k.clone(), Bitmap::deserialize(v)))
                .collect(),
        ))
    }

    fn clear(&self) -> Result<(), eyre::Report> {
        let mut con = self.client.get_connection()?;
        con.del(&self.key)?;
        Ok(())
    }
}
