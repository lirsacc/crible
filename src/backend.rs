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
    #[error("serialization error")]
    EncodingError(#[from] bincode::Error),
}

pub trait Backend {
    fn save(
        &self,
        facets: impl IntoIterator<Item = (String, Vec<u64>)>,
        delete_missing: bool,
    ) -> Result<(), BackendError>;
    fn save_facet<'a>(
        &self,
        key: &'a str,
        data: &'a [u64],
    ) -> Result<(), BackendError>;
    fn delete_facet<'a>(&self, key: &'a str) -> Result<(), BackendError>;
    fn load(&self) -> Result<Vec<(String, Vec<u64>)>, BackendError>;
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
    fn save(
        &self,
        facets: impl IntoIterator<Item = (String, Vec<u64>)>,
        delete_missing: bool,
    ) -> Result<(), BackendError> {
        let mut seen: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        if delete_missing {
            match std::fs::remove_dir_all(&self.directory) {
                Ok(()) => Ok(()),
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => Ok(()),
                    _ => Err(e),
                },
            }?;
        };

        std::fs::create_dir_all(&self.directory)?;

        for (k, v) in facets.into_iter() {
            self.save_facet(&k, &v)?;
            seen.insert(k.to_owned());
        }

        if delete_missing {
            for r in std::fs::read_dir(&self.directory)? {
                let path = r?.path();
                let key =
                    path.file_name().unwrap().to_str().unwrap().to_owned();
                if !seen.contains(&key) {
                    std::fs::remove_file(path)?;
                }
            }
        }

        Ok(())
    }

    fn save_facet<'a>(
        &self,
        key: &'a str,
        data: &'a [u64],
    ) -> Result<(), BackendError> {
        let path = self.directory.join(key);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&bincode::serialize(data)?)?;
        Ok(())
    }

    fn delete_facet<'a>(&self, key: &'a str) -> Result<(), BackendError> {
        let path = self.directory.join(key);
        std::fs::remove_file(path)?;
        Ok(())
    }

    fn load(&self) -> Result<Vec<(String, Vec<u64>)>, BackendError> {
        let mut data: Vec<(String, Vec<u64>)> = vec![];
        for r in std::fs::read_dir(&self.directory)? {
            let path = r?.path();
            let bytes = std::fs::read(&path)?;
            let decoded: Vec<u64> = bincode::deserialize(&bytes[..])?;
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
    fn save(
        &self,
        facets: impl IntoIterator<Item = (String, Vec<u64>)>,
        delete_missing: bool,
    ) -> Result<(), BackendError> {
        timed_cb(
            || self.inner.save(facets, delete_missing),
            |d| info!("Saved in {:?}", d),
        )
    }

    fn save_facet<'a>(
        &self,
        key: &'a str,
        data: &'a [u64],
    ) -> Result<(), BackendError> {
        timed_cb(
            || self.inner.save_facet(key, data),
            |d| info!("Saved facet {} in {:?}", key, d),
        )
    }

    fn delete_facet<'a>(&self, key: &'a str) -> Result<(), BackendError> {
        timed_cb(
            || self.inner.delete_facet(key),
            |d| info!("Deleted facet {} in {:?}", key, d),
        )
    }

    fn load(&self) -> Result<Vec<(String, Vec<u64>)>, BackendError> {
        timed_cb(|| self.inner.load(), |d| info!("Loaded in {:?}", d))
    }

    fn clear(&self) -> Result<(), BackendError> {
        timed_cb(|| self.inner.clear(), |d| info!("Cleared in {:?}", d))
    }
}

pub fn import_csv(
    input: &mut dyn std::io::Read,
    backend: &impl Backend,
    delete_missing: bool,
) -> Result<(), BackendError> {
    let mut rdr = csv::Reader::from_reader(input);

    let headers = rdr.headers()?.clone();
    let index_columns = &headers
        .into_iter()
        .enumerate()
        .collect::<Vec<(usize, &str)>>()[1..headers.len()];
    let mut data: HashMap<String, Vec<u64>> = HashMap::new();

    for row in rdr.records() {
        let record = row?;
        let value = &record[0].parse::<u64>()?;

        for (i, t) in index_columns {
            let key_str = &record[*i];
            let mut keys: Vec<String> = Vec::new();

            if key_str.is_empty() {
                keys.push(format!("{}-null", t));
            } else if key_str.contains('|') {
                for part in key_str.split('|') {
                    keys.push(format!("{}-{}", t, part.parse::<u64>()?))
                }
            } else {
                keys.push(format!("{}-{}", t, key_str.parse::<u64>()?));
            }

            for key in keys {
                match data.entry(key) {
                    Entry::Occupied(e) => e.into_mut().push(*value),
                    Entry::Vacant(e) => {
                        e.insert(vec![*value]);
                    }
                };
            }
        }
    }

    backend.save(data, delete_missing)?;
    Ok(())
}
