use std::collections::HashMap;

use async_trait::async_trait;
use croaring::Bitmap;

use super::Backend;
use crate::index::Index;

pub struct JsonFSBackend {
    path: std::path::PathBuf,
}

type JsonFSFormat = HashMap<String, String>;

/// Filesystem backend using an easily cross-compatible json format. The data is
/// saved as a Json object where each key is a property and each value is the
/// based64 encoded serialized Roaring Bitmap.
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
        Ok(serde_json::to_string(
            &index
                .0
                .iter()
                .map(|(k, v)| (k.to_owned(), base64::encode(v.serialize())))
                .collect::<JsonFSFormat>(),
        )?)
    }

    pub fn deserialize(&self, bytes: &[u8]) -> Result<Index, eyre::Report> {
        let data: JsonFSFormat = serde_json::from_slice(bytes)?;
        Ok(Index::new(
            data.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        Bitmap::deserialize(&base64::decode(v).unwrap()),
                    )
                })
                .collect(),
        ))
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
