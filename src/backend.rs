use croaring::Bitmap;
use log::info;
use thiserror::Error;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{prelude::*, Seek, SeekFrom};

use crate::utils::timed_cb;

#[derive(Error, Debug)]
pub enum BackendError {
    #[error("io error")]
    IOError(#[from] std::io::Error),
    #[error("csv error")]
    CSVError(#[from] csv::Error),
    #[error("integer parsing error error")]
    IntegerParsingError(#[from] std::num::ParseIntError),
}

pub trait Backend: Send + Sync {
    fn save<'a>(
        &self,
        facets: impl IntoIterator<Item = (&'a str, &'a Bitmap)>,
        clear: bool,
    ) -> Result<(), BackendError>;
    fn save_facet<'a>(
        &self,
        key: &'a str,
        facet: &'a Bitmap,
    ) -> Result<(), BackendError>;
    fn delete_facet<'a>(&self, key: &'a str) -> Result<(), BackendError>;
    fn load(&self) -> Result<Vec<(String, Bitmap)>, BackendError>;
    fn clear(&self) -> Result<(), BackendError>;
}

pub struct FSBackend {
    directory: std::path::PathBuf,
}

impl FSBackend {
    pub fn new<T: Into<std::path::PathBuf>>(target: T) -> Self {
        Self {
            directory: target.into(),
        }
    }
}

impl Backend for FSBackend {
    fn save<'a>(
        &self,
        facets: impl IntoIterator<Item = (&'a str, &'a Bitmap)>,
        clear: bool,
    ) -> Result<(), BackendError> {
        let mut seen: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        if clear {
            match std::fs::remove_dir_all(&self.directory) {
                Ok(()) => Ok(()),
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => Ok(()),
                    _ => Err(e),
                },
            }?;
        };

        std::fs::create_dir_all(&self.directory)?;

        for (k, v) in facets {
            self.save_facet(&k, &v)?;
            seen.insert(k.to_owned());
        }

        Ok(())
    }

    fn save_facet<'a>(
        &self,
        key: &'a str,
        data: &'a Bitmap,
    ) -> Result<(), BackendError> {
        let path = self.directory.join(key);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&data.serialize())?;
        Ok(())
    }

    fn delete_facet<'a>(&self, key: &'a str) -> Result<(), BackendError> {
        let path = self.directory.join(key);
        std::fs::remove_file(path)?;
        Ok(())
    }

    fn load(&self) -> Result<Vec<(String, Bitmap)>, BackendError> {
        let mut data: Vec<(String, Bitmap)> = Vec::new();
        for r in std::fs::read_dir(&self.directory)? {
            let path = r?.path();
            let bytes = std::fs::read(&path)?;
            let decoded: Bitmap = Bitmap::deserialize(&bytes[..]);
            let key = path.file_name().unwrap().to_str().unwrap().to_owned();
            data.push((key, decoded));
        }
        Ok(data)
    }

    fn clear(&self) -> Result<(), BackendError> {
        std::fs::remove_dir_all(&self.directory)?;
        Ok(())
    }
}

pub struct TimedBackend<T>
where
    T: Backend,
{
    inner: T,
}

impl<T> TimedBackend<T>
where
    T: Backend,
{
    pub fn wrap(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Backend for TimedBackend<T>
where
    T: Backend,
{
    fn save<'a>(
        &self,
        facets: impl IntoIterator<Item = (&'a str, &'a Bitmap)>,
        clear: bool,
    ) -> Result<(), BackendError> {
        timed_cb(
            || self.inner.save(facets, clear),
            |d| info!("Saved all facets in {:?}", d),
        )
    }

    fn save_facet<'a>(
        &self,
        key: &'a str,
        facet: &'a Bitmap,
    ) -> Result<(), BackendError> {
        timed_cb(
            || self.inner.save_facet(key, facet),
            |d| info!("Saved facet {} in {:?}", key, d),
        )
    }

    fn delete_facet<'a>(&self, key: &'a str) -> Result<(), BackendError> {
        timed_cb(
            || self.inner.delete_facet(key),
            |d| info!("Deleted facet {} in {:?}", key, d),
        )
    }

    fn load(&self) -> Result<Vec<(String, Bitmap)>, BackendError> {
        timed_cb(
            || self.inner.load(),
            |d| info!("Loaded all facets in {:?}", d),
        )
    }

    fn clear(&self) -> Result<(), BackendError> {
        timed_cb(
            || self.inner.clear(),
            |d| info!("Cleared storage backend in {:?}", d),
        )
    }
}

pub fn import_csv(
    input: &mut dyn std::io::Read,
    backend: &impl Backend,
    clear: bool,
) -> Result<(), BackendError> {
    let mut rdr = csv::Reader::from_reader(input);

    let headers = rdr.headers()?.clone();
    let index_columns = &headers
        .into_iter()
        .enumerate()
        .collect::<Vec<(usize, &str)>>()[1..headers.len()];
    let mut data: HashMap<String, Bitmap> = HashMap::new();

    for row in rdr.records() {
        let record = row?;
        let value = &record[0].parse::<u32>()?;

        for (i, t) in index_columns {
            let key_str = &record[*i];
            let mut keys: Vec<String> = Vec::new();

            if key_str.is_empty() {
                keys.push(format!("{}-null", t));
            } else if key_str.contains('|') {
                for part in key_str.split('|') {
                    keys.push(format!("{}-{}", t, part.parse::<u32>()?))
                }
            } else {
                keys.push(format!("{}-{}", t, key_str.parse::<u32>()?));
            }

            for key in keys {
                match data.entry(key) {
                    Entry::Occupied(e) => e.into_mut().add(*value),
                    Entry::Vacant(e) => {
                        e.insert(Bitmap::of(&[*value]));
                    }
                };
            }
        }
    }

    backend.save(data.iter().map(|(k, v)| (k.as_ref(), v)), clear)?;
    Ok(())
}
