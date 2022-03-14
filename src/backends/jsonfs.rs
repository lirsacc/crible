use std::{collections::HashMap, io::BufRead};

use async_trait::async_trait;
use croaring::Bitmap;

use super::Backend;
use crate::index::Index;

// TODO: Use buffered read and writes.

#[derive(Debug)]
pub struct JsonFSBackend {
    path: std::path::PathBuf,
}

/// Filesystem backend using an easily cross-compatible json format. The data is
/// saved as a newline delimited Json file where each line is a pair [key,
/// base64 encoded serializded bitmap].
impl JsonFSBackend {
    pub fn new<T: Into<std::path::PathBuf> + AsRef<std::ffi::OsStr>>(
        p: &T,
    ) -> Self {
        Self { path: p.into() }
    }

    pub async fn write(&self, index: &Index) -> Result<(), eyre::Report> {
        let tmp = crate::utils::tmp_path(&self.path);
        tokio::fs::create_dir_all(&self.path.parent().unwrap()).await?;
        match tokio::fs::remove_file(&tmp).await {
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            x => x,
        }?;
        tokio::fs::write(&tmp, &self.serialize(index)?).await?;
        tokio::fs::rename(&tmp, &self.path).await?;
        Ok(())
    }

    pub async fn read(&self) -> Result<Index, eyre::Report> {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => self.deserialize(&bytes),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                let index = Index::default();
                self.write(&index).await?;
                Ok(index)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn serialize(&self, index: &Index) -> Result<String, eyre::Report> {
        let mut res = String::new();
        for (k, v) in index.0.iter() {
            let element = (k, base64::encode(v.serialize()));
            res.push_str(&serde_json::to_string(&element)?);
            res.push('\n');
        }
        Ok(res)
    }

    pub fn deserialize(&self, bytes: &[u8]) -> Result<Index, eyre::Report> {
        let mut res: HashMap<String, Bitmap> = HashMap::new();
        for line in bytes.lines() {
            let line = line?;
            if line.is_empty() {
                break;
            }
            let (k, v): (String, String) = serde_json::from_str(&line)?;
            res.insert(k.to_owned(), Bitmap::deserialize(&base64::decode(v)?));
        }
        Ok(Index::new(res))
    }
}

#[async_trait]
impl Backend for JsonFSBackend {
    async fn dump<'a>(&mut self, index: &Index) -> Result<(), eyre::Report> {
        self.write(index).await
    }

    async fn load(&self) -> Result<Index, eyre::Report> {
        self.read().await
    }

    async fn clear(&mut self) -> Result<(), eyre::Report> {
        match tokio::fs::remove_file(&self.path).await {
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            x => x,
        }?;
        Ok(())
    }
}

impl Default for JsonFSBackend {
    fn default() -> Self {
        Self { path: "data.json".into() }
    }
}
