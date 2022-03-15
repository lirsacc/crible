use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::From;

use croaring::Bitmap;
use serde_derive::Serialize;
use thiserror::Error;

use crate::expression::Expression;

// TODO: Cache root?
// TODO: Rayon / concurrent iteration for some functions.

#[derive(Error, Debug, PartialEq, Eq)]
pub enum IndexError {
    #[error("property {0:?} does not exist")]
    PropertyDoesNotExist(String),
}

#[derive(Clone, Default)]
pub struct Index(pub(crate) HashMap<String, Bitmap>);

impl Index {
    pub fn new(data: HashMap<String, Bitmap>) -> Self {
        Self(data)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn root(&self) -> Bitmap {
        Bitmap::fast_or(&self.0.values().collect::<Vec<&Bitmap>>())
    }

    pub fn stats(&self) -> Stats {
        if self.is_empty() {
            Stats::default()
        } else {
            (&self.root()).into()
        }
    }

    pub fn property_stats(&self) -> HashMap<String, Stats> {
        self.0.iter().map(|(k, v)| (k.to_owned(), v.into())).collect()
    }

    pub fn all_properties<'a>(&'a self) -> Vec<&'a str> {
        let mut p: Vec<&str> = self.0.keys().map(|x| x.as_ref()).collect();
        p.sort_unstable();
        p
    }

    pub fn properties_matching_id<'a>(&'a self, id: u32) -> Vec<&'a str> {
        let mut x: Vec<&'a str> = self
            .0
            .iter()
            .filter_map(
                |(k, v)| if v.contains(id) { Some(k.as_ref()) } else { None },
            )
            .collect();
        x.sort_unstable();
        x
    }

    pub fn get_property(&self, property: &str) -> Option<&Bitmap> {
        self.0.get(property)
    }

    pub fn set_property(&mut self, property: &str, bm: Bitmap) {
        self.0.insert(property.to_owned(), bm);
    }

    pub fn delete_property(&mut self, property: &str) -> bool {
        self.0.remove(property).is_some()
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn set(&mut self, property: &str, id: u32) -> bool {
        (match self.0.entry(property.to_owned()) {
            Entry::Occupied(e) => &mut *e.into_mut(),
            Entry::Vacant(e) => &mut *e.insert(Bitmap::create()),
        })
        .add_checked(id)
    }

    pub fn set_many(&mut self, property: &str, ids: &[u32]) {
        (match self.0.entry(property.to_owned()) {
            Entry::Occupied(e) => &mut *e.into_mut(),
            Entry::Vacant(e) => &mut *e.insert(Bitmap::create()),
        })
        .add_many(ids)
    }

    pub fn unset(&mut self, property: &str, id: u32) -> bool {
        match self.0.entry(property.to_owned()) {
            Entry::Occupied(e) => (&mut *e.into_mut()).remove_checked(id),
            Entry::Vacant(_) => false,
        }
    }

    pub fn remove_id(&mut self, id: u32) -> bool {
        let mut changed = false;
        for bm in self.0.values_mut() {
            changed = bm.remove_checked(id) || changed;
        }
        changed
    }

    pub fn execute(
        &self,
        expression: &Expression,
    ) -> Result<Bitmap, IndexError> {
        match expression {
            Expression::Root => Ok(self.root()),
            Expression::Property(name) => self
                .get_property(name)
                .ok_or_else(|| {
                    IndexError::PropertyDoesNotExist(name.to_owned())
                })
                .cloned(),
            Expression::And(l, r) => {
                Ok(self.execute(l.as_ref())?.and(&self.execute(r.as_ref())?))
            }
            Expression::Or(l, r) => {
                Ok(self.execute(l.as_ref())?.or(&self.execute(r.as_ref())?))
            }
            Expression::Xor(l, r) => {
                Ok(self.execute(l.as_ref())?.xor(&self.execute(r.as_ref())?))
            }
            Expression::Sub(l, r) => {
                Ok(self.execute(l.as_ref())?.andnot(&self.execute(r.as_ref())?))
            }
            Expression::Not(e) => Ok(self.root() - self.execute(e.as_ref())?),
        }
    }

    pub fn cardinalities(
        &self,
        source: &Bitmap,
        prefix: Option<&str>,
    ) -> HashMap<String, u64> {
        match prefix {
            None => (&self.0)
                .iter()
                .map(|(k, v)| (k.to_owned(), source.and_cardinality(v)))
                .collect(),
            Some(p) => (&self.0)
                .iter()
                .filter_map(|(k, v)| {
                    if k.starts_with(p) {
                        Some((k.to_owned(), source.and_cardinality(v)))
                    } else {
                        None
                    }
                })
                .collect(),
        }
    }
}

impl std::fmt::Debug for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Index [{} properties]", self.0.len())
    }
}

#[derive(Debug, Serialize, Default)]
pub struct Stats {
    pub cardinality: u64,
    pub minimum: Option<u32>,
    pub maximum: Option<u32>,
}

impl From<&Bitmap> for Stats {
    fn from(bm: &Bitmap) -> Self {
        Self {
            cardinality: bm.cardinality(),
            minimum: bm.minimum(),
            maximum: bm.maximum(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::Expression;

    #[test]
    fn simple_in() {
        let mut index = Index::default();
        index.set("foo:0", 42);
        let matches = index
            .execute(&Expression::parse("foo:0").unwrap())
            .unwrap()
            .to_vec();

        assert_eq!(matches, vec![42]);
    }

    #[test]
    fn simple_and() {
        let mut index = Index::default();
        index.set("foo:0", 42);
        index.set("foo:0", 43);
        index.set("foo:1", 42);
        index.set("foo:1", 44);

        let matches = index
            .execute(&Expression::parse("(foo:0 AND foo:1)").unwrap())
            .unwrap()
            .to_vec();

        assert_eq!(matches, vec![42]);
    }

    #[test]
    fn simple_or() {
        let mut index = Index::default();
        index.set("foo:0", 42);
        index.set("foo:0", 43);
        index.set("foo:1", 42);
        index.set("foo:1", 44);

        let matches = index
            .execute(&Expression::parse("(foo:0 OR foo:1)").unwrap())
            .unwrap()
            .to_vec();

        assert_eq!(matches, vec![42, 43, 44]);
    }

    #[test]
    fn simple_sub() {
        let mut index = Index::default();
        index.set("foo:0", 42);
        index.set("foo:0", 43);
        index.set("foo:1", 42);
        index.set("foo:1", 44);

        let matches = index
            .execute(&Expression::parse("(foo:0 - foo:1)").unwrap())
            .unwrap()
            .to_vec();

        assert_eq!(matches, vec![43]);
    }

    #[test]
    fn simple_not() {
        let mut index = Index::default();
        index.set("foo:0", 42);
        index.set("foo:0", 43);
        index.set("foo:1", 42);
        index.set("foo:1", 44);

        let matches = index
            .execute(&Expression::parse("NOT foo:0").unwrap())
            .unwrap()
            .to_vec();

        assert_eq!(matches, vec![44]);
    }
}
