use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{prelude::*, Seek};
use std::sync::{Arc, RwLock};

use boolean_expression::Expr;
use croaring::Bitmap;
use log::info;
use rusqlite::params;

use crate::error::{CribleError, Result};
use crate::expressions::{apply_expression, to_sqlite_filter};

#[derive(Debug, Serialize)]
pub struct Stats {
    pub cardinality: u64,
    pub minimum: Option<u32>,
    pub maximum: Option<u32>,
}

pub trait Index {
    fn len(&self) -> Result<usize>;
    fn stats(&self) -> Result<Stats>;
    fn facet_stats(&self, facet_id: &str) -> Result<Stats>;
    fn facet_ids(&self) -> Result<Vec<String>>;
    fn add(&self, facet_id: &str, value: u32) -> Result<()>;
    fn remove(&self, facet_id: &str, value: u32) -> Result<()>;
    fn deindex(&self, value: u32) -> Result<()>;
    fn drop_facet(&self, facet_id: &str) -> Result<()>;
    fn set_facet(&self, facet_id: &str, bitmap: Bitmap) -> Result<()>;
    fn set_many(&self, bitmaps: HashMap<String, Bitmap>) -> Result<()>;
    fn clear(&self) -> Result<()>;
    fn save(&self) -> Result<()>;
    fn apply(&self, expr: Expr<String>) -> Result<Bitmap>;
}

pub struct VerboseIndex<T: Index + Sized>(T);

impl<T: Index + Sized> VerboseIndex<T> {
    pub fn new(inner: T) -> Self {
        Self(inner)
    }
}

impl<T: Index> Index for VerboseIndex<T> {
    fn len(&self) -> Result<usize> {
        crate::utils::timed_cb(
            || self.0.len(),
            |d| info!("VerboseIndex::len took {:?}", d),
        )
    }

    fn stats(&self) -> Result<Stats> {
        crate::utils::timed_cb(
            || self.0.stats(),
            |d| info!("VerboseIndex::stats took {:?}", d),
        )
    }

    fn facet_stats(&self, facet_id: &str) -> Result<Stats> {
        crate::utils::timed_cb(
            || self.0.facet_stats(facet_id),
            |d| info!("VerboseIndex::facet_stats took {:?}", d),
        )
    }

    fn facet_ids(&self) -> Result<Vec<String>> {
        crate::utils::timed_cb(
            || self.0.facet_ids(),
            |d| info!("VerboseIndex::facet_ids took {:?}", d),
        )
    }

    fn add(&self, facet_id: &str, value: u32) -> Result<()> {
        crate::utils::timed_cb(
            || self.0.add(facet_id, value),
            |d| info!("VerboseIndex::add took {:?}", d),
        )
    }
    fn remove(&self, facet_id: &str, value: u32) -> Result<()> {
        crate::utils::timed_cb(
            || self.0.remove(facet_id, value),
            |d| info!("VerboseIndex::remove took {:?}", d),
        )
    }

    fn deindex(&self, value: u32) -> Result<()> {
        crate::utils::timed_cb(
            || self.0.deindex(value),
            |d| info!("VerboseIndex::deindex took {:?}", d),
        )
    }

    fn drop_facet(&self, facet_id: &str) -> Result<()> {
        crate::utils::timed_cb(
            || self.0.drop_facet(facet_id),
            |d| info!("VerboseIndex::drop_facet took {:?}", d),
        )
    }

    fn set_facet(&self, facet_id: &str, bitmap: Bitmap) -> Result<()> {
        crate::utils::timed_cb(
            || self.0.set_facet(facet_id, bitmap),
            |d| info!("VerboseIndex::set_facet took {:?}", d),
        )
    }

    fn set_many(&self, bitmaps: HashMap<String, Bitmap>) -> Result<()> {
        crate::utils::timed_cb(
            || self.0.set_many(bitmaps),
            |d| info!("VerboseIndex::set_many took {:?}", d),
        )
    }

    fn save(&self) -> Result<()> {
        crate::utils::timed_cb(
            || self.0.save(),
            |d| info!("VerboseIndex::save took {:?}", d),
        )
    }

    fn clear(&self) -> Result<()> {
        crate::utils::timed_cb(
            || self.0.clear(),
            |d| info!("VerboseIndex::clear took {:?}", d),
        )
    }

    fn apply(&self, expr: Expr<String>) -> Result<Bitmap> {
        crate::utils::timed_cb(
            || self.0.apply(expr),
            |d| info!("VerboseIndex::apply took {:?}", d),
        )
    }
}

struct _MemoryIndexData {
    pub(crate) facets: HashMap<String, Bitmap>,
    pub(crate) root: Bitmap,
}

impl _MemoryIndexData {
    // TODO: This is required to compute the root NOT expressions but is quite
    // slow (compared to the rest of the operations).
    fn recompute_root(&mut self) {
        let sources: Vec<&Bitmap> = self.facets.values().collect();
        let mut root = Bitmap::fast_or(&sources);
        root.run_optimize();
        self.root = root;
    }
}

impl Default for _MemoryIndexData {
    fn default() -> Self {
        Self {
            facets: HashMap::new(),
            root: Bitmap::create(),
        }
    }
}

pub struct MemoryIndex(Arc<RwLock<_MemoryIndexData>>);

impl MemoryIndex {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(_MemoryIndexData::default())))
    }
}

impl Index for MemoryIndex {
    fn len(&self) -> Result<usize> {
        Ok(self.0.read()?.facets.len())
    }

    fn stats(&self) -> Result<Stats> {
        let data = self.0.read()?;
        Ok(Stats {
            cardinality: data.root.cardinality(),
            minimum: data.root.minimum(),
            maximum: data.root.maximum(),
        })
    }

    fn facet_stats(&self, facet_id: &str) -> Result<Stats> {
        let data = self.0.read()?;
        match data.facets.get(facet_id) {
            Some(f) => Ok(Stats {
                cardinality: f.cardinality(),
                minimum: f.minimum(),
                maximum: f.maximum(),
            }),
            None => Err(CribleError::FacetDoesNotExist(facet_id.to_owned())),
        }
    }

    fn facet_ids(&self) -> Result<Vec<String>> {
        // TODO: Clone?
        Ok(self.0.read()?.facets.keys().cloned().collect())
    }

    fn add(&self, facet_id: &str, value: u32) -> Result<()> {
        let data = &mut self.0.write()?;
        let facet = match data.facets.entry(facet_id.to_owned()) {
            Entry::Occupied(e) => &mut *e.into_mut(),
            Entry::Vacant(e) => &mut *e.insert(Bitmap::create()),
        };
        if !facet.contains(value) {
            facet.add(value);
            facet.run_optimize();
            data.recompute_root();
        }
        Ok(())
    }

    fn remove(&self, facet_id: &str, value: u32) -> Result<()> {
        let data = &mut self.0.write()?;
        let facet = match data.facets.entry(facet_id.to_owned()) {
            Entry::Occupied(e) => Ok(&mut *e.into_mut()),
            Entry::Vacant(_) => {
                Err(CribleError::FacetDoesNotExist(facet_id.to_owned()))
            }
        }?;
        if facet.contains(value) {
            facet.remove(value);
            facet.run_optimize();
            data.recompute_root();
        }
        Ok(())
    }

    fn deindex(&self, value: u32) -> Result<()> {
        let mut changed = false;
        let data = &mut self.0.write()?;
        for facet in data.facets.values_mut() {
            if facet.contains(value) {
                facet.remove(value);
                facet.run_optimize();
                changed = true;
            };
        }
        if changed {
            data.recompute_root();
        }
        Ok(())
    }

    fn drop_facet(&self, facet_id: &str) -> Result<()> {
        let data = &mut self.0.write()?;
        match data.facets.remove(facet_id) {
            Some(_) => {
                data.recompute_root();
                Ok(())
            }
            None => Err(CribleError::FacetDoesNotExist(facet_id.to_owned())),
        }
    }

    fn set_facet(&self, facet_id: &str, bitmap: Bitmap) -> Result<()> {
        let data = &mut self.0.write()?;
        data.facets.insert(facet_id.to_owned(), bitmap);
        data.recompute_root();
        Ok(())
    }

    fn set_many(&self, bitmaps: HashMap<String, Bitmap>) -> Result<()> {
        let data = &mut self.0.write()?;
        for (facet_id, bitmap) in bitmaps {
            data.facets.insert(facet_id.to_owned(), bitmap);
        }
        data.recompute_root();
        Ok(())
    }

    fn save(&self) -> Result<()> {
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let facets = &mut self.0.write()?.facets;
        facets.clear();
        Ok(())
    }

    fn apply(&self, expr: Expr<String>) -> Result<Bitmap> {
        let data = self.0.read()?;
        apply_expression(&data.root, &data.facets, expr)
    }
}

pub struct FSIndex {
    directory: std::path::PathBuf,
    inner: MemoryIndex,
}

impl FSIndex {
    pub fn new<T: Into<std::path::PathBuf>>(directory: T) -> Self {
        Self {
            directory: directory.into(),
            inner: MemoryIndex::new(),
        }
    }

    pub fn load<T: Into<std::path::PathBuf>>(directory: T) -> Result<Self> {
        let instance = Self::new(directory);
        std::fs::create_dir_all(&instance.directory)?;
        let mut data: HashMap<String, Bitmap> = HashMap::new();
        for r in std::fs::read_dir(&instance.directory)? {
            let path = r?.path();
            let bytes = std::fs::read(&path)?;
            data.insert(
                path.file_name().unwrap().to_str().unwrap().to_owned(),
                Bitmap::deserialize(&bytes[..]),
            );
        }
        instance.inner.set_many(data)?;
        Ok(instance)
    }
}

impl Index for FSIndex {
    fn len(&self) -> Result<usize> {
        self.inner.len()
    }

    fn stats(&self) -> Result<Stats> {
        self.inner.stats()
    }

    fn facet_stats(&self, facet_id: &str) -> Result<Stats> {
        self.inner.facet_stats(facet_id)
    }

    fn facet_ids(&self) -> Result<Vec<String>> {
        self.inner.facet_ids()
    }

    fn add(&self, facet_id: &str, value: u32) -> Result<()> {
        self.inner.add(facet_id, value)
    }
    fn remove(&self, facet_id: &str, value: u32) -> Result<()> {
        self.inner.remove(facet_id, value)
    }

    fn deindex(&self, value: u32) -> Result<()> {
        self.inner.deindex(value)
    }

    fn drop_facet(&self, facet_id: &str) -> Result<()> {
        self.inner.drop_facet(facet_id)
    }

    fn set_facet(&self, facet_id: &str, bitmap: Bitmap) -> Result<()> {
        self.inner.set_facet(facet_id, bitmap)
    }

    fn set_many(&self, bitmaps: HashMap<String, Bitmap>) -> Result<()> {
        self.inner.set_many(bitmaps)
    }

    fn save(&self) -> Result<()> {
        // TODO: This is very crude and most likely very much not as efficient
        // as it could be.
        let data = &self.inner.0.read()?.facets;
        std::fs::remove_dir_all(&self.directory)?;
        std::fs::create_dir_all(&self.directory)?;
        for (facet_id, data) in data {
            let path = self.directory.join(facet_id);
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;
            file.seek(std::io::SeekFrom::Start(0))?;
            file.write_all(&data.serialize())?;
        }
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        self.inner.clear()
    }

    fn apply(&self, expr: Expr<String>) -> Result<Bitmap> {
        self.inner.apply(expr)
    }
}

// TODO: Use r2d2 to make this Send + Sync for use in warp. I think there should
// be another way to do this without a connection pool.
pub struct SQLiteIndex(r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>);

impl SQLiteIndex {
    pub fn init(
        pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    ) -> Result<Self> {
        let conn = pool.get()?;
        conn.execute(
            "create table if not exists search_index (value unsigned int, facet varchar(64));",
            rusqlite::NO_PARAMS
        )?;
        conn.execute(
            "create unique index if not exists value_facet_uq on search_index (facet, value);",
            rusqlite::NO_PARAMS
        )?;
        conn.execute(
            "create index if not exists facet_idx on search_index (facet);",
            rusqlite::NO_PARAMS,
        )?;
        conn.execute(
            "create index if not exists value_idx on search_index (value);",
            rusqlite::NO_PARAMS,
        )?;
        Ok(Self(pool))
    }
}

impl Index for SQLiteIndex {
    fn len(&self) -> Result<usize> {
        let conn = self.0.get()?;
        let res: isize = conn.query_row(
            "select count(distinct facet) from search_index;",
            rusqlite::NO_PARAMS,
            |row| row.get(0),
        )?;
        Ok(res as usize)
    }

    fn stats(&self) -> Result<Stats> {
        let conn = self.0.get()?;
        conn.query_row_and_then(
            "select count(distinct value), max(value), min(value) from search_index;",
            rusqlite::NO_PARAMS,
            |row| {
                let c: i64 = row.get(0)?;
                Ok(
                    Stats {
                        cardinality: c as u64,
                        maximum: row.get(1)?,
                        minimum: row.get(2)?,
                    }
                )
            },
        )
    }

    fn facet_stats(&self, facet_id: &str) -> Result<Stats> {
        let conn = self.0.get()?;
        conn.query_row_and_then(
            "select count(distinct value), max(value), min(value) from search_index where facet = ?1;",
            &[facet_id],
            |row| {
                let c: i64 = row.get(0)?;
                if c > 0 {
                    Ok(Stats {
                        cardinality: c as u64,
                        maximum: row.get(1)?,
                        minimum: row.get(2)?,
                    })
                } else {
                    Err(CribleError::FacetDoesNotExist(facet_id.to_owned()))
                }
            },
        )
    }

    fn facet_ids(&self) -> Result<Vec<String>> {
        Ok(self
            .0
            .get()?
            .prepare("select distinct facet from search_index;")?
            .query_map(rusqlite::NO_PARAMS, |row| row.get(0))?
            .map(|x| x.unwrap())
            .collect())
    }

    fn add(&self, facet_id: &str, value: u32) -> Result<()> {
        let conn = self.0.get()?;
        conn.execute(
            "insert or ignore into search_index (value, facet) values (?1, ?2);",
            params![value, facet_id]
        )?;
        Ok(())
    }

    fn remove(&self, facet_id: &str, value: u32) -> Result<()> {
        let conn = self.0.get()?;
        conn.execute(
            "delete from search_index where value = ?1 and facet = ?2;",
            params![value, facet_id],
        )?;
        Ok(())
    }

    fn deindex(&self, value: u32) -> Result<()> {
        let conn = self.0.get()?;
        conn.execute(
            "delete from search_index where value = ?1;",
            params![value],
        )?;
        Ok(())
    }

    fn drop_facet(&self, facet_id: &str) -> Result<()> {
        let conn = self.0.get()?;
        conn.execute(
            "delete from search_index where facet = ?1;",
            params![facet_id],
        )?;
        Ok(())
    }

    fn set_facet(&self, facet_id: &str, bitmap: Bitmap) -> Result<()> {
        let mut conn = self.0.get()?;
        let tx = conn.transaction()?;
        tx.execute(
            "delete from search_index where facet = ?1;",
            params![facet_id],
        )?;
        for value in bitmap.iter() {
            tx.execute(
                "insert into search_index (value, facet) values (?1, ?2);",
                params![value, facet_id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn set_many(&self, bitmaps: HashMap<String, Bitmap>) -> Result<()> {
        let mut conn = self.0.get()?;
        let tx = conn.transaction()?;
        // TODO: Would prefer to use the params.
        let facets_in = bitmaps
            .keys()
            .map(|x| format!("'{}'", x))
            .collect::<Vec<String>>()
            .join(", ");
        tx.execute(
            &format!(
                "delete from search_index where facet in ({});",
                facets_in
            ),
            rusqlite::NO_PARAMS,
        )?;
        for (facet_id, values) in bitmaps {
            for value in values.iter() {
                tx.execute(
                    "insert into search_index (value, facet) values (?1, ?2);",
                    params![value, facet_id],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let conn = self.0.get()?;
        conn.execute("delete from search_index;", rusqlite::NO_PARAMS)?;
        Ok(())
    }

    fn save(&self) -> Result<()> {
        Ok(())
    }

    fn apply(&self, expr: Expr<String>) -> Result<Bitmap> {
        let conn = self.0.get()?;
        let query: String = to_sqlite_filter(expr)?;
        let values: Vec<u32> = conn
            .prepare(&format!(
                "select value from search_index where {}",
                query
            ))?
            .query_map(rusqlite::NO_PARAMS, |row| row.get(0))?
            .map(|x| x.unwrap())
            .collect();
        Ok(Bitmap::of(&values))
    }
}

pub fn import_csv(
    input: &mut dyn std::io::Read,
    index: &dyn Index,
) -> Result<()> {
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

    index.set_many(data)?;
    index.save()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Index, MemoryIndex};
    use crate::expressions::parse_expression;

    #[test]
    fn simple_in() {
        let index = MemoryIndex::new();
        index.add("foo-0", 42).unwrap();
        let matches = index
            .apply(parse_expression("foo-0").unwrap())
            .unwrap()
            .to_vec();

        assert_eq!(matches, vec![42]);
    }

    #[test]
    fn simple_and() {
        let index = MemoryIndex::new();
        index.add("foo-0", 42).unwrap();
        index.add("foo-0", 43).unwrap();
        index.add("foo-1", 42).unwrap();
        index.add("foo-1", 44).unwrap();

        let matches = index
            .apply(parse_expression("(foo-0 AND foo-1)").unwrap())
            .unwrap()
            .to_vec();

        assert_eq!(matches, vec![42]);
    }

    #[test]
    fn simple_or() {
        let index = MemoryIndex::new();
        index.add("foo-0", 42).unwrap();
        index.add("foo-0", 43).unwrap();
        index.add("foo-1", 42).unwrap();
        index.add("foo-1", 44).unwrap();

        let mut matches = index
            .apply(parse_expression("(foo-0 OR foo-1)").unwrap())
            .unwrap()
            .to_vec();
        matches.sort();

        assert_eq!(matches, vec![42, 43, 44]);
    }

    #[test]
    fn simple_not() {
        let index = MemoryIndex::new();
        index.add("foo-0", 42).unwrap();
        index.add("foo-0", 43).unwrap();
        index.add("foo-1", 42).unwrap();
        index.add("foo-1", 44).unwrap();

        let mut matches = index
            .apply(parse_expression("NOT foo-0").unwrap())
            .unwrap()
            .to_vec();
        matches.sort();

        assert_eq!(matches, vec![44]);
    }
}
