use std::collections::HashMap;

use async_trait::async_trait;
use croaring::Bitmap;
use savefile_derive::Savefile;

use super::Backend;
use crate::index::Index;

const BIN_FS_FORMAT_VERSION: u32 = 1;

#[derive(Debug)]
pub struct BinFSBackend {
    path: std::path::PathBuf,
}

#[derive(Savefile)]
struct BinFSFormat(HashMap<String, Vec<u8>>);

/// Filesystem backend using an optimized binary format. This is not meant ot be
/// interoperable and should be more compact that the JsonFSBackend format.s
impl BinFSBackend {
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

    pub fn serialize(&self, index: &Index) -> Result<Vec<u8>, eyre::Report> {
        Ok(savefile::save_to_mem(
            BIN_FS_FORMAT_VERSION,
            &BinFSFormat(
                index
                    .0
                    .iter()
                    .map(|(k, v)| (k.to_owned(), v.serialize()))
                    .collect::<HashMap<String, Vec<u8>>>(),
            ),
        )?)
    }

    pub fn deserialize(&self, bytes: &[u8]) -> Result<Index, eyre::Report> {
        let data: BinFSFormat =
            savefile::load_from_mem(bytes, BIN_FS_FORMAT_VERSION)?;
        Ok(Index::new(
            data.0
                .iter()
                .map(|(k, v)| (k.clone(), Bitmap::deserialize(v)))
                .collect(),
        ))
    }
}

#[async_trait]
impl Backend for BinFSBackend {
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

impl Default for BinFSBackend {
    fn default() -> Self {
        Self { path: "data.bin".into() }
    }
}
