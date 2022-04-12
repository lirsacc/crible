use async_trait::async_trait;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crible_lib::{Encoder, Index};

use super::Backend;

// TODO: Use buffered read and writes.

#[derive(Debug)]
pub struct FSBackend {
    path: std::path::PathBuf,
    encoder: Encoder,
}

/// Filesystem backend backed by any of the supported encoders.
impl FSBackend {
    pub fn new<T: Into<std::path::PathBuf> + AsRef<std::ffi::OsStr>>(
        p: &T,
        encoder: Encoder,
    ) -> Self {
        Self { path: p.into(), encoder }
    }

    pub async fn write(&self, index: &Index) -> Result<(), eyre::Report> {
        let tmp = crate::utils::tmp_path(&self.path);
        tokio::fs::create_dir_all(&self.path.parent().unwrap()).await?;
        match tokio::fs::remove_file(&tmp).await {
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            x => x,
        }?;

        let f = tokio::fs::OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(&tmp)
            .await?;

        self.encoder.encode_async(f.compat(), index).await?;

        tokio::fs::rename(&tmp, &self.path).await?;
        Ok(())
    }

    pub async fn read(&self) -> Result<Index, eyre::Report> {
        let f = tokio::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&self.path)
            .await?;

        Ok(self.encoder.decode_async(f.compat()).await?)
    }
}

#[async_trait]
impl Backend for FSBackend {
    async fn dump<'a>(&self, index: &Index) -> Result<(), eyre::Report> {
        self.write(index).await
    }

    async fn load(&self) -> Result<Index, eyre::Report> {
        self.read().await
    }

    async fn clear(&self) -> Result<(), eyre::Report> {
        match tokio::fs::remove_file(&self.path).await {
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            x => x,
        }?;
        Ok(())
    }
}
