use async_trait::async_trait;
use croaring::Bitmap;
use redis::AsyncCommands;

use crible_lib::index::Index;
use std::collections::HashMap;

use super::Backend;

#[derive(Debug)]
pub struct Redis {
    client: redis::Client,
    key: String,
}

impl Redis {
    pub fn new(url: &url::Url, key: String) -> Result<Self, eyre::Report> {
        Ok(Self { client: redis::Client::open(url.to_string())?, key })
    }
}

#[async_trait]
impl Backend for Redis {
    async fn dump<'a>(&mut self, index: &Index) -> Result<(), eyre::Report> {
        let mut pipe = redis::pipe();
        for (k, v) in index.inner() {
            pipe.hset(&self.key, k, v.serialize());
        }
        let mut con = self.client.get_async_connection().await?;
        pipe.query_async(&mut con).await?;
        Ok(())
    }

    async fn load(&self) -> Result<Index, eyre::Report> {
        let mut con = self.client.get_async_connection().await?;
        let data: HashMap<String, Vec<u8>> = con.hgetall(&self.key).await?;
        Ok(Index::new(
            data.iter()
                .map(|(k, v)| (k.clone(), Bitmap::deserialize(v)))
                .collect(),
        ))
    }

    async fn clear(&mut self) -> Result<(), eyre::Report> {
        let mut con = self.client.get_async_connection().await?;
        con.del(&self.key).await?;
        Ok(())
    }
}
