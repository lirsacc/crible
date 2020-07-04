use boolean_expression::Expr;
use croaring::Bitmap;
use thiserror::Error;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::u32;

use crate::backend::{Backend, BackendError};

#[derive(Error, Debug)]
pub enum SearchIndexError {
    #[error("backend error")]
    BackendError(#[from] BackendError),
    #[error("facet does not exist")]
    FacetDoesNotExist(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FacetStats {
    pub key: String,
    pub cardinality: u64,
    pub minimum: Option<u32>,
    pub maximum: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GlobalStats {
    pub length: usize,
    pub cardinality: u64,
    pub minimum: Option<u32>,
    pub maximum: Option<u32>,
}

pub struct SearchIndex {
    facets: HashMap<String, Bitmap>,
    last_change: std::time::Instant,
    root: Bitmap,
}

impl SearchIndex {
    pub fn new() -> Self {
        Self {
            facets: HashMap::new(),
            last_change: std::time::Instant::now(),
            root: Bitmap::create(),
        }
    }

    pub fn from_backend(backend: &impl Backend) -> Result<Self, BackendError> {
        let mut index = Self::new();
        for (k, v) in backend.load()? {
            index.facets.insert(k, v);
        }
        index.record_change();
        Ok(index)
    }

    pub fn record_change(&mut self) {
        self.last_change = std::time::Instant::now();
        // TODO: This is slow to do all the time. Ideally would happen on demand.
        self.recompute_root();
    }

    pub fn has_changed_since(&self, since: std::time::Instant) -> bool {
        since < self.last_change
    }

    pub fn len(&self) -> usize {
        self.facets.keys().len()
    }

    pub fn stats(&self) -> GlobalStats {
        GlobalStats {
            length: self.len(),
            cardinality: self.root.cardinality(),
            minimum: self.root.minimum(),
            maximum: self.root.maximum(),
        }
    }

    pub fn facet_stats(
        &self,
        key: &str,
    ) -> Result<FacetStats, SearchIndexError> {
        let f = self.facet(key)?;
        Ok(FacetStats {
            key: key.to_owned(),
            cardinality: f.cardinality(),
            minimum: f.minimum(),
            maximum: f.maximum(),
        })
    }

    pub fn iter_facets(&self) -> impl Iterator<Item = (&String, &Bitmap)> {
        self.facets.iter()
    }

    pub fn facet(&self, key: &str) -> Result<&Bitmap, SearchIndexError> {
        match &self.facets.get(key) {
            Some(tm) => Ok(tm),
            None => Err(SearchIndexError::FacetDoesNotExist(key.to_owned())),
        }
    }

    pub fn facet_mut(&mut self, key: &str) -> &mut Bitmap {
        match self.facets.entry(key.to_owned()) {
            Entry::Occupied(e) => &mut *e.into_mut(),
            Entry::Vacant(e) => &mut *e.insert(Bitmap::create()),
        }
    }

    pub fn facet_mut_strict(
        &mut self,
        key: &str,
    ) -> Result<&mut Bitmap, SearchIndexError> {
        match self.facets.entry(key.to_owned()) {
            Entry::Occupied(e) => Ok(&mut *e.into_mut()),
            Entry::Vacant(_) => {
                Err(SearchIndexError::FacetDoesNotExist(key.to_owned()))
            }
        }
    }

    pub fn add(&mut self, key: &str, value: u32) {
        let facet = self.facet_mut(key);
        if !facet.contains(value) {
            facet.add(value);
            facet.run_optimize();
            self.record_change();
        }
    }

    pub fn remove(
        &mut self,
        key: &str,
        value: u32,
    ) -> Result<(), SearchIndexError> {
        let facet = self.facet_mut_strict(key)?;
        if facet.contains(value) {
            facet.remove(value);
            facet.run_optimize();
            self.record_change();
        };
        Ok(())
    }

    pub fn deindex(&mut self, value: u32) {
        let mut changed = false;
        for facet in self.facets.values_mut() {
            if facet.contains(value) {
                facet.remove(value);
                facet.run_optimize();
                changed = true;
            };
        }
        if changed {
            self.record_change();
        }
    }

    pub fn drop_facet(&mut self, key: &str) {
        self.facets.remove(key);
        self.record_change();
    }

    pub fn recompute_root(&mut self) {
        let sources: Vec<&Bitmap> =
            self.iter_facets().map(|(_, v)| v).collect();
        let mut root = Bitmap::fast_or(&sources);
        root.run_optimize();
        self.root = root;
    }

    pub fn apply_expression(
        &self,
        expr: Expr<String>,
    ) -> Result<Bitmap, SearchIndexError> {
        match expr {
            Expr::Const(_) => unreachable!(),
            Expr::Not(e) => Ok(self.root.andnot(&self.apply_expression(*e)?)),
            Expr::Terminal(key) => Ok(self.facet(&key)?.clone()),
            Expr::And(lhs, rhs) => Ok(match (*lhs, *rhs) {
                (Expr::Not(x), Expr::Not(y)) => self.root.andnot(
                    &self
                        .apply_expression(*x)?
                        .or(&self.apply_expression(*y)?),
                ),
                (Expr::Not(x), y) | (y, Expr::Not(x)) => self
                    .apply_expression(y)?
                    .andnot(&self.apply_expression(*x)?),
                (x, y) => {
                    self.apply_expression(x)?.and(&self.apply_expression(y)?)
                }
            }),
            Expr::Or(lhs, rhs) => Ok(self
                .apply_expression(*lhs)?
                .or(&self.apply_expression(*rhs)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SearchIndex;
    use crate::expressions::parse_expression;

    #[test]
    fn simple_in() {
        let mut index = SearchIndex::new();
        index.add("foo-0", 42);
        let matches = index
            .apply_expression(parse_expression("foo-0").unwrap())
            .unwrap()
            .to_vec();

        assert_eq!(matches, vec![42]);
    }

    #[test]
    fn simple_and() {
        let mut index = SearchIndex::new();
        index.add("foo-0", 42);
        index.add("foo-0", 43);
        index.add("foo-1", 42);
        index.add("foo-1", 44);

        let matches = index
            .apply_expression(parse_expression("(foo-0 AND foo-1)").unwrap())
            .unwrap()
            .to_vec();

        assert_eq!(matches, vec![42]);
    }

    #[test]
    fn simple_or() {
        let mut index = SearchIndex::new();
        index.add("foo-0", 42);
        index.add("foo-0", 43);
        index.add("foo-1", 42);
        index.add("foo-1", 44);

        let mut matches = index
            .apply_expression(parse_expression("(foo-0 OR foo-1)").unwrap())
            .unwrap()
            .to_vec();
        matches.sort();

        assert_eq!(matches, vec![42, 43, 44]);
    }

    #[test]
    fn simple_not() {
        let mut index = SearchIndex::new();
        index.add("foo-0", 42);
        index.add("foo-0", 43);
        index.add("foo-1", 42);
        index.add("foo-1", 44);

        let mut matches = index
            .apply_expression(parse_expression("NOT foo-0").unwrap())
            .unwrap()
            .to_vec();
        matches.sort();

        assert_eq!(matches, vec![44]);
    }
}
