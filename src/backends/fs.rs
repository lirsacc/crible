use std::fs;

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

    pub fn write(&self, index: &Index) -> Result<(), eyre::Report> {
        let tmp = crate::utils::tmp_path(&self.path);
        fs::create_dir_all(self.path.parent().unwrap())?;
        match fs::remove_file(&tmp) {
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            x => x,
        }?;

        let f = fs::OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(&tmp)?;

        self.encoder.encode(f, index)?;

        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    pub fn read(&self) -> Result<Index, eyre::Report> {
        let f = fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&self.path)?;

        Ok(self.encoder.decode(f)?)
    }
}

impl Backend for FSBackend {
    fn dump<'a>(&self, index: &Index) -> Result<(), eyre::Report> {
        self.write(index)
    }

    fn load(&self) -> Result<Index, eyre::Report> {
        self.read()
    }

    fn clear(&self) -> Result<(), eyre::Report> {
        match fs::remove_file(&self.path) {
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            x => x,
        }?;
        Ok(())
    }
}
