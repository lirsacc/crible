use std::collections::HashMap;
use std::convert::{From, Into};

use croaring::Bitmap;
use serde_derive::Serialize;
use thiserror::Error;

use crate::expression::Expression;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("property {0:?} does not exist")]
    PropertyDoesNotExist(String),
}

#[derive(Clone, Default, PartialEq)]
pub struct Index(HashMap<String, Bitmap>);

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

    pub fn of<T, S>(value: T) -> Self
    where
        S: AsRef<str>,
        for<'a> &'a T: IntoIterator<Item = &'a (S, Vec<u32>)>,
    {
        Self::new(
            value
                .into_iter()
                .map(|(k, v)| (k.as_ref().to_owned(), Bitmap::of(v)))
                .collect(),
        )
    }

    /// Return the number of unique properties covered by the index.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    ///
    /// let index = Index::default();
    /// assert_eq!(index.len(), 0);
    ///
    /// let index = Index::of([
    ///     ("foo", vec![1, 2, 3, 4]),
    ///     ("bar", vec![5, 6, 7]),
    ///     ("baz", vec![8, 9]),
    /// ]);
    /// assert_eq!(index.len(), 3);
    /// ```
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Return the number of unique properties covered by the index.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    ///
    /// let index = Index::default();
    /// assert!(index.is_empty());
    ///
    /// let index = Index::of([
    ///     ("foo", vec![1, 2, 3, 4]),
    ///     ("bar", vec![5, 6, 7]),
    ///     ("baz", vec![8, 9]),
    /// ]);
    /// assert!(!index.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return a Bitmap containing all values in the index..
    ///
    /// ```
    /// # use crible_lib::index::Index;
    ///
    /// let index = Index::default();
    /// assert!(index.root().is_empty());
    ///
    /// let index = Index::of([
    ///     ("foo", vec![1, 2, 3, 4]),
    ///     ("bar", vec![5, 6, 7]),
    ///     ("baz", vec![8, 9]),
    /// ]);
    /// assert_eq!(index.root().to_vec(), [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    /// ```
    pub fn root(&self) -> Bitmap {
        // TODO: Could we cache this internally?
        // Just iterating is actually slightly faster at low property counts but
        // given the gain is relatively small it's better overall to use
        // fast_or.
        Bitmap::fast_or(&self.0.values().collect::<Vec<&Bitmap>>())
    }

    /// Access the inner hashmap.
    pub fn inner(&self) -> &HashMap<String, Bitmap> {
        &self.0
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

    /// Set a bit for a single property. Returns whether the bit was not already set.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    ///
    /// let mut index = Index::default();
    ///
    /// assert!(index.set("foo", 1));
    /// assert!(!index.set("foo", 1));
    ///
    /// assert_eq!(index.get_property("foo").unwrap().to_vec(), vec![1]);
    /// ```
    pub fn set(&mut self, property: &str, bit: u32) -> bool {
        self.0
            .entry(property.to_owned())
            .or_insert_with(Bitmap::create)
            .add_checked(bit)
    }

    /// Set multiple bits for a single property.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    ///
    /// let mut index = Index::default();
    ///
    /// index.set_many("foo", &vec![1, 2, 3, 4]);
    ///
    /// assert_eq!(index.get_property("foo").unwrap().to_vec(), vec![1, 2, 3, 4]);
    /// ```
    pub fn set_many(&mut self, property: &str, bits: &[u32]) {
        self.0
            .entry(property.to_owned())
            .or_insert_with(Bitmap::create)
            .add_many(bits);
    }

    /// Set multiple bits from a all properties.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    ///
    /// let mut index = Index::of([
    ///     ("foo", vec![1, 4]),
    ///     ("bar", vec![5, 6, 7]),
    ///     ("baz", vec![8, 9]),
    /// ]);
    ///
    /// index.set_all(&vec![2, 3]);
    ///
    /// assert_eq!(index.get_property("foo").unwrap().to_vec(), vec![1, 2, 3, 4]);
    /// assert_eq!(index.get_property("bar").unwrap().to_vec(), vec![2, 3, 5, 6, 7]);
    /// assert_eq!(index.get_property("baz").unwrap().to_vec(), vec![2, 3, 8, 9]);
    /// ```
    pub fn set_all(&mut self, bits: &[u32]) {
        let mask = Bitmap::of(bits);
        for bm in self.0.values_mut() {
            bm.or_inplace(&mask);
        }
    }

    /// Unset a bit for a single property. Returns whether the bit was present.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    ///
    /// let mut index = Index::default();
    ///
    /// index.set_many("foo", &vec![1, 2, 3, 4]);
    /// assert!(index.unset("foo", 1));
    /// assert!(!index.unset("foo", 1));
    ///
    /// assert_eq!(index.get_property("foo").unwrap().to_vec(), vec![2, 3, 4]);
    /// ```
    pub fn unset(&mut self, property: &str, bit: u32) -> bool {
        self.0.get_mut(property).map_or(false, |bm| bm.remove_checked(bit))
    }

    /// Unset multiple bits from a single property.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    ///
    /// let mut index = Index::default();
    ///
    /// index.set_many("foo", &vec![1, 2, 3, 4]);
    /// index.unset_many("foo", &vec![1, 4]);
    ///
    /// assert_eq!(index.get_property("foo").unwrap().to_vec(), vec![2, 3]);
    /// ```
    pub fn unset_many(&mut self, property: &str, bits: &[u32]) {
        if let Some(bm) = self.0.get_mut(property) {
            bm.andnot_inplace(&Bitmap::of(bits));
        }
    }

    /// Unset multiple bits from a all properties.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    ///
    /// let mut index = Index::of([
    ///     ("foo", vec![1, 2, 3, 4]),
    ///     ("bar", vec![1, 2, 3, 5, 6, 7]),
    ///     ("baz", vec![4, 8, 9]),
    /// ]);
    ///
    /// index.unset_all(&vec![2, 3, 4]);
    ///
    /// assert_eq!(index.get_property("foo").unwrap().to_vec(), vec![1]);
    /// assert_eq!(index.get_property("bar").unwrap().to_vec(), vec![1, 5, 6, 7]);
    /// assert_eq!(index.get_property("baz").unwrap().to_vec(), vec![8, 9]);
    /// ```
    pub fn unset_all(&mut self, bits: &[u32]) {
        let mask = Bitmap::of(bits);
        for bm in self.0.values_mut() {
            bm.andnot_inplace(&mask);
        }
    }

    // Operations on all properties for a given bit.

    /// List all properties where `bit` is set.
    ///
    /// WARN: This can be slow as it iterates over the entire index.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    ///
    /// let index = Index::of([
    ///     ("foo", vec![1, 2, 3]),
    ///     ("bar", vec![1, 3, 4]),
    ///     ("baz", vec![2, 3, 4]),
    /// ]);
    ///
    /// assert_eq!(index.get_properties_with_bit(2), vec!["baz", "foo"]);
    /// ```
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

    /// Set `bit` for all given properties and remove it from all others.
    ///
    /// WARN: This can be slow as it iterates over the entire index.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    ///
    /// let mut index = Index::of([
    ///     ("foo", vec![1, 2, 3]),
    ///     ("bar", vec![1, 3, 4]),
    ///     ("baz", vec![2, 3, 4]),
    /// ]);
    ///
    /// index.set_properties_with_bit(8, &vec!["foo", "bar"].iter().map(|s| s.to_owned()).collect::<Vec<_>>());
    /// assert_eq!(index.get_properties_with_bit(8), vec!["bar", "foo"]);
    /// ```
    pub fn set_properties_with_bit<T: AsRef<str>>(
        &mut self,
        bit: u32,
        properties: &[T],
    ) -> bool {
        let c: Vec<&str> = properties.iter().map(|x| x.as_ref()).collect();
        self.0.iter_mut().fold(false, |changed, (k, v)| {
            (if !c.contains(&k.as_ref()) {
                v.remove_checked(bit)
            } else {
                v.add_checked(bit)
            }) || changed
        })
    }

    // Run queries.

    /// Execute a query against the index.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    /// # use std::str::FromStr;
    ///
    /// let mut index = Index::of([
    ///     ("foo", vec![1, 2, 3, 6]),
    ///     ("bar", vec![1, 3, 4, 7]),
    ///     ("baz", vec![3, 4, 5, 7]),
    /// ]);
    ///
    /// assert_eq!(
    ///     index.execute(&"*".parse().unwrap()).unwrap().to_vec(),
    ///     vec![1, 2, 3, 4, 5, 6, 7],
    /// );
    ///
    /// assert_eq!(
    ///     index.execute(&"foo".parse().unwrap()).unwrap().to_vec(),
    ///     vec![1, 2, 3, 6],
    /// );
    ///
    /// assert!(
    ///     index.execute(&"unknown".parse().unwrap()).is_err()
    /// );
    ///
    /// assert_eq!(
    ///     index.execute(&"foo and bar".parse().unwrap()).unwrap().to_vec(),
    ///     vec![1, 3],
    /// );
    ///
    /// assert_eq!(
    ///     index.execute(&"foo or bar".parse().unwrap()).unwrap().to_vec(),
    ///     vec![1, 2, 3, 4, 6, 7],
    /// );
    ///
    /// assert_eq!(
    ///     index.execute(&"foo xor bar".parse().unwrap()).unwrap().to_vec(),
    ///     vec![2, 4, 6, 7],
    /// );
    ///
    /// assert_eq!(
    ///     index.execute(&"foo - bar".parse().unwrap()).unwrap().to_vec(),
    ///     vec![2, 6],
    /// );
    /// ```
    ///
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
            // TODO: Is there a version using `flip()` which is faster? As root
            // can be slow on a large index.
            Expression::Not(e) => Ok(self.root() - self.execute(e.as_ref())?),
        }
    }

    /// Compute the cardinality of a given Bitmap with all other Bitmaps in the
    /// index. This is mostly useful to filter which properties still have
    /// result after executing a predicate.
    ///
    /// ```
    /// # use crible_lib::index::Index;
    /// # use std::str::FromStr;
    /// # use std::collections::HashMap;
    ///
    /// let mut index = Index::of([
    ///     ("foo", vec![1, 2, 3, 6]),
    ///     ("bar", vec![1, 3, 4, 7]),
    ///     ("baz", vec![3, 4, 5, 7]),
    /// ]);
    ///
    /// let res = index.execute(&"foo and bar".parse().unwrap()).unwrap();
    ///
    /// let unprefixed = index.cardinalities(&res, None);
    /// assert_eq!(*unprefixed.get("foo").unwrap(), 2);
    /// assert_eq!(*unprefixed.get("bar").unwrap(), 2);
    /// assert_eq!(*unprefixed.get("baz").unwrap(), 1);
    ///
    /// let prefixed = index.cardinalities(&res, Some("ba"));
    /// assert!(prefixed.get("foo").is_none());
    /// assert_eq!(*prefixed.get("bar").unwrap(), 2);
    /// assert_eq!(*prefixed.get("baz").unwrap(), 1);
    /// ```
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

#[derive(Debug, Serialize, Default, PartialEq)]
pub struct Stats {
    pub cardinality: u64,
    pub minimum: Option<u32>,
    pub maximum: Option<u32>,
}

impl From<Bitmap> for Stats {
    fn from(bm: Bitmap) -> Self {
        (&bm).into()
    }
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

impl From<Index> for Stats {
    fn from(index: Index) -> Self {
        (&index).into()
    }
}

impl From<&Index> for Stats {
    fn from(index: &Index) -> Self {
        Self::from(index.root())
    }
}

// TODO: These are limited unit tests. Should write some more complete tests
// over real-life data.
#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    // macro_rules! assert_query {
    //     ($index:expr, $value:expr, $expected:expr) => {
    //         assert_eq!(
    //             $index
    //                 .execute(&Expression::parse($value).unwrap())
    //                 .unwrap()
    //                 .to_vec(),
    //             $expected,
    //         );
    //     };
    // }

    #[rstest]
    #[case("*", &[1, 2, 3, 4, 5, 6, 7, 8, 9])]
    #[case("foo", &[1, 2, 3, 4, 9])]
    #[case("not foo", &[5, 6, 7, 8])]
    #[case("!!foo", &[1, 2, 3, 4, 9])]
    #[case("foo and bar", &[1, 3])]
    #[case("bar and baz", &[6])]
    #[case("foo or bar", &[1, 2, 3, 4, 5, 6, 7, 9])]
    #[case("foo xor bar", &[2, 4, 5, 6, 7, 9])]
    #[case("foo and not bar", &[2, 4, 9])]
    #[case("not foo and bar", &[5, 6, 7])]
    #[case("not (foo and bar)", &[2, 4, 5, 6, 7, 8, 9])]
    #[case("(foo and bar) or baz", &[1, 3, 4, 6, 8, 9])]
    #[case("foo - (bar and baz) - (foo xor bar)", &[1, 3])]
    #[case("baz - foo - bar", &[8])]
    fn test_queries(#[case] input: &str, #[case] expected: &[u32]) {
        let index = Index::of([
            ("foo", vec![1, 2, 3, 4, 9]),
            ("bar", vec![1, 3, 5, 6, 7]),
            ("baz", vec![4, 6, 8, 9]),
        ]);
        let res = index.execute(&input.parse().unwrap()).unwrap();
        assert_eq!(&res.to_vec(), expected);
    }

    #[test]
    fn test_stats() {
        assert_eq!(Stats::default(), Index::default().into());
        assert_eq!(Stats::default(), Bitmap::default().into());

        let index = Index::of([
            ("foo", vec![1, 2, 3, 4, 9]),
            ("bar", vec![1, 3, 5, 6, 7]),
            ("baz", vec![4, 6, 8, 9]),
        ]);

        assert_eq!(
            Stats { cardinality: 9, minimum: Some(1), maximum: Some(9) },
            (&index).into()
        );

        assert_eq!(
            &Stats { cardinality: 5, minimum: Some(1), maximum: Some(9) },
            &index.get_property("foo").unwrap().into(),
        );
    }
}
