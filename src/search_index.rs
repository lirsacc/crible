use boolean_expression::Expr;
use croaring::Treemap;
use thiserror::Error;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::u64;

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
    pub minimum: Option<u64>,
    pub maximum: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GlobalStats {
    pub length: usize,
    pub cardinality: u64,
    pub minimum: Option<u64>,
    pub maximum: Option<u64>,
}

pub struct SearchIndex {
    facets: HashMap<String, croaring::Treemap>,
    last_change: std::time::Instant,
}

impl SearchIndex {
    pub fn new() -> Self {
        Self {
            facets: HashMap::new(),
            last_change: std::time::Instant::now(),
        }
    }

    pub fn from_backend(backend: &impl Backend) -> Result<Self, BackendError> {
        let mut index = Self::new();
        for (k, v) in backend.load()? {
            let tm = Treemap::of(&v);
            index.facets.insert(k, tm);
        }
        index.optimize();
        index.last_change = std::time::Instant::now();
        Ok(index)
    }

    pub fn record_change(&mut self) {
        self.last_change = std::time::Instant::now();
    }

    pub fn has_changed_since(&self, since: std::time::Instant) -> bool {
        since < self.last_change
    }

    pub fn len(&self) -> usize {
        self.facets.keys().len()
    }

    pub fn stats(&self) -> GlobalStats {
        let mut f = Treemap::create();
        for (_, tm) in self.iter_facets() {
            f.or_inplace(tm);
        }
        GlobalStats {
            length: self.len(),
            cardinality: f.cardinality(),
            minimum: f.minimum(),
            maximum: f.maximum(),
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

    pub fn iter_facets(&self) -> impl Iterator<Item = (&String, &Treemap)> {
        self.facets.iter()
    }

    pub fn facet(&self, key: &str) -> Result<&Treemap, SearchIndexError> {
        match &self.facets.get(key) {
            Some(tm) => Ok(tm),
            None => Err(SearchIndexError::FacetDoesNotExist(key.to_owned())),
        }
    }

    pub fn facet_mut(&mut self, key: &str) -> &mut Treemap {
        match self.facets.entry(key.to_owned()) {
            Entry::Occupied(e) => &mut *e.into_mut(),
            Entry::Vacant(e) => &mut *e.insert(Treemap::create()),
        }
    }

    pub fn facet_mut_strict(
        &mut self,
        key: &str,
    ) -> Result<&mut Treemap, SearchIndexError> {
        match self.facets.entry(key.to_owned()) {
            Entry::Occupied(e) => Ok(&mut *e.into_mut()),
            Entry::Vacant(_) => {
                Err(SearchIndexError::FacetDoesNotExist(key.to_owned()))
            }
        }
    }

    pub fn add(&mut self, key: &str, value: u64) {
        let facet = self.facet_mut(key);
        facet.add(value);
        facet.run_optimize();
        self.record_change();
    }

    pub fn remove(
        &mut self,
        key: &str,
        value: u64,
    ) -> Result<(), SearchIndexError> {
        self.facet_mut_strict(key)?.remove(value);
        self.record_change();
        Ok(())
    }

    // WARN: Slow.
    pub fn deindex(&mut self, value: u64) {
        for tm in self.facets.values_mut() {
            tm.remove(value);
        }
        self.record_change();
    }

    pub fn drop_facet(&mut self, key: &str) {
        self.facets.remove(key);
        self.record_change();
    }

    pub fn optimize(&mut self) {
        for tm in self.facets.values_mut() {
            tm.run_optimize();
        }
    }

    pub fn apply_expression(
        &self,
        expr: Expr<String>,
    ) -> Result<Treemap, SearchIndexError> {
        match expr {
            Expr::Const(_) => unreachable!(),
            Expr::Not(_) => unimplemented!(),
            Expr::Terminal(key) => {
                let blank = Treemap::create();
                Ok(blank.or(self.facet(&key)?))
            }
            Expr::And(lhs, rhs) => Ok(self
                .apply_expression(*lhs)?
                .and(&self.apply_expression(*rhs)?)),
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
}
