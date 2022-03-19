use std::collections::HashMap;
use std::convert::From;

use croaring::Bitmap;
use serde_derive::Serialize;
use thiserror::Error;

use crate::expression::Expression;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("property {0:?} does not exist")]
    PropertyDoesNotExist(String),
}

#[derive(Clone, Default)]
pub struct Index(pub(crate) HashMap<String, Bitmap>);

/// An Index is simply a very large bit-matrix where each row is an individual
/// property and each column is unique element id represented by a bit on the
/// row. The index is a container with a convenient interface to set and unset
/// bits and execute boolean operations across rows.
///
/// All semantics must exist outside of the Index (meaning of the
/// properties, of their combinations, etc.).
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

    // TODO: Could we cache this internally?
    pub fn root(&self) -> Bitmap {
        Bitmap::fast_or(&self.0.values().collect::<Vec<&Bitmap>>())
    }

    pub fn stats(&self) -> Stats {
        if self.is_empty() {
            Stats::default()
        } else {
            self.root().into()
        }
    }

    // Operate on rows.

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

    // Operate on individual bits.

    pub fn set(&mut self, property: &str, bit: u32) -> bool {
        self.0
            .entry(property.to_owned())
            .or_insert_with(Bitmap::create)
            .add_checked(bit)
    }

    pub fn set_many(&mut self, property: &str, bits: &[u32]) {
        self.0
            .entry(property.to_owned())
            .or_insert_with(Bitmap::create)
            .add_many(bits);
    }

    pub fn unset(&mut self, property: &str, bit: u32) -> bool {
        self.0.get_mut(property).map_or(false, |bm| bm.remove_checked(bit))
    }

    pub fn unset_many(&mut self, property: &str, bits: &[u32]) {
        if let Some(bm) = self.0.get_mut(property) {
            bm.andnot_inplace(&Bitmap::of(bits));
        }
    }

    pub fn unset_all_bits(&mut self, bits: &[u32]) {
        let mask = Bitmap::of(bits);
        for bm in self.0.values_mut() {
            bm.andnot_inplace(&mask);
        }
    }

    // Operations on all properties for a given bit.
    // WARN: These are slow as given the structure the index we end up iterating
    // over all properties.

    pub fn get_properties_with_bit(&self, bit: u32) -> Vec<String> {
        let mut vec: Vec<String> =
            self.into_iter()
                .filter_map(|(k, v)| {
                    if v.contains(bit) {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect();
        vec.sort_unstable();
        vec
    }

    pub fn set_properties_with_bit(
        &mut self,
        bit: u32,
        properties: &[String],
    ) -> bool {
        self.0.iter_mut().fold(false, |changed, (k, v)| {
            (if !properties.contains(k) {
                v.remove_checked(bit)
            } else {
                v.add_checked(bit)
            }) || changed
        })
    }

    // Run queries.

    pub fn execute(&self, expression: &Expression) -> Result<Bitmap, Error> {
        match expression {
            Expression::Root => Ok(self.root()),
            Expression::Property(name) => self
                .get_property(name)
                .ok_or_else(|| Error::PropertyDoesNotExist(name.clone()))
                .cloned(),
            Expression::And(inner) => {
                let mut res: Bitmap = self.execute(&inner[0])?;
                for e in &inner[1..] {
                    // TODO: Would it be cheaper to break here if one is empty?
                    res.and_inplace(&self.execute(e)?)
                }
                Ok(res)
            }
            Expression::Or(inner) => {
                let mut inner_executed = Vec::with_capacity(inner.len());
                for x in inner {
                    inner_executed.push(self.execute(x)?);
                }
                Ok(Bitmap::fast_or(&inner_executed.iter().collect::<Vec<_>>()))
            }
            Expression::Xor(inner) => {
                let mut inner_executed = Vec::with_capacity(inner.len());
                for x in inner {
                    inner_executed.push(self.execute(x)?);
                }
                Ok(Bitmap::fast_xor(&inner_executed.iter().collect::<Vec<_>>()))
            }
            Expression::Sub(inner) => {
                let mut res: Bitmap = self.execute(&inner[0])?;
                for e in &inner[1..] {
                    res.andnot_inplace(&self.execute(e)?)
                }
                Ok(res)
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
                .filter_map(|x| _filter_map_cardinality(source, x))
                .collect(),
            Some(p) => (&self.0)
                .iter()
                .filter_map(|(k, v)| {
                    if k.starts_with(p) {
                        _filter_map_cardinality(source, (k, v))
                    } else {
                        None
                    }
                })
                .collect(),
        }
    }
}

#[inline]
fn _filter_map_cardinality(
    source: &Bitmap,
    (k, v): (&String, &Bitmap),
) -> Option<(String, u64)> {
    let x = source.and_cardinality(v);
    if x > 0 {
        Some((k.clone(), x))
    } else {
        None
    }
}

impl std::fmt::Debug for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Index [{} properties]", self.0.len())
    }
}

impl<'a> IntoIterator for &'a Index {
    type Item = (&'a String, &'a Bitmap);
    type IntoIter = std::collections::hash_map::Iter<'a, String, Bitmap>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Serialize, Default)]
pub struct Stats {
    pub cardinality: u64,
    pub minimum: Option<u32>,
    pub maximum: Option<u32>,
}

impl From<Bitmap> for Stats {
    fn from(bm: Bitmap) -> Self {
        Self {
            cardinality: bm.cardinality(),
            minimum: bm.minimum(),
            maximum: bm.maximum(),
        }
    }
}

impl From<&Bitmap> for Stats {
    fn from(bm: &Bitmap) -> Self {
        bm.into()
    }
}

// TODO: These are limited unit tests. Should write some more complete tests
// over real-life data.
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
