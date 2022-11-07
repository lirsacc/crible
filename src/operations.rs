use std::collections::HashMap;
use std::convert::From;

use crible_lib::expression::Expression;
use crible_lib::Index;
use parking_lot::RwLock;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug)]
pub enum OperationError {
    ReadOnly,
    Expression(crible_lib::expression::Error),
    Index(crible_lib::index::Error),
}

impl From<crible_lib::expression::Error> for OperationError {
    fn from(e: crible_lib::expression::Error) -> Self {
        OperationError::Expression(e)
    }
}

impl From<crible_lib::index::Error> for OperationError {
    fn from(e: crible_lib::index::Error) -> Self {
        OperationError::Index(e)
    }
}

type OperationResult<T> = Result<T, OperationError>;

pub trait Operation {
    type Output;

    fn run(self, index: &RwLock<Index>) -> Self::Output;
}

/// Run a query against the index. The result will include all unique elements
/// matching the query and optionally (if `include_cardinalities` is provided
/// and true) a map containing the cardinality of the intersection of the query
/// and every property included in the index.
#[derive(Deserialize, Debug)]
pub struct Query {
    query: String,
    include_cardinalities: Option<bool>,
}

#[derive(Serialize, Debug)]
pub struct QueryResult {
    values: Vec<u32>,
    cardinalities: Option<HashMap<String, u64>>,
}

impl Operation for Query {
    type Output = OperationResult<QueryResult>;

    #[inline]
    fn run(self, index: &RwLock<Index>) -> OperationResult<QueryResult> {
        let expr = Expression::parse(&self.query)?;
        let idx = index.read();
        let bm = idx.execute(&expr)?;
        let cardinalities = match self.include_cardinalities {
            Some(true) => Some(idx.par_cardinalities(&bm, None)),
            _ => None,
        };
        Ok(QueryResult { values: bm.to_vec(), cardinalities })
    }
}

#[derive(Deserialize, Debug)]
pub struct Count {
    query: String,
}

impl Operation for Count {
    type Output = OperationResult<u64>;

    #[inline]
    fn run(self, index: &RwLock<Index>) -> OperationResult<u64> {
        let expr = Expression::parse(&self.query)?;
        let idx = index.read();
        let bm = idx.execute(&expr)?;
        Ok(bm.cardinality())
    }
}

#[derive(Deserialize, Debug)]
pub struct Stats;

#[derive(Serialize, Debug)]
pub struct StatsResult {
    root: crible_lib::index::Stats,
    properties: HashMap<String, crible_lib::index::Stats>,
}

impl Operation for Stats {
    type Output = StatsResult;

    #[inline]
    fn run(self, index: &RwLock<Index>) -> StatsResult {
        let idx = index.read();
        StatsResult {
            root: (&*idx).into(),
            properties: idx
                .into_iter()
                .map(|(k, v)| (k.clone(), v.into()))
                .collect(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Set {
    property: String,
    bit: u32,
}

impl Operation for Set {
    type Output = bool;

    #[inline]
    fn run(self, index: &RwLock<Index>) -> bool {
        index.write().set(&self.property, self.bit)
    }
}

#[derive(Deserialize, Debug)]
pub struct SetMany {
    values: HashMap<String, Vec<u32>>,
}

impl Operation for SetMany {
    type Output = ();

    #[inline]
    fn run(self, index: &RwLock<Index>) {
        let mut idx = index.write();
        for (property, bits) in &self.values {
            idx.set_many(property, bits);
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Unset {
    property: String,
    bit: u32,
}

impl Operation for Unset {
    type Output = bool;

    #[inline]
    fn run(self, index: &RwLock<Index>) -> bool {
        index.write().unset(&self.property, self.bit)
    }
}

#[derive(Deserialize, Debug)]
pub struct UnsetMany {
    values: HashMap<String, Vec<u32>>,
}

impl Operation for UnsetMany {
    type Output = ();

    #[inline]
    fn run(self, index: &RwLock<Index>) {
        let mut idx = index.write();
        for (property, bits) in &self.values {
            idx.unset_many(property, bits);
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct GetBit {
    bit: u32,
}

impl Operation for GetBit {
    type Output = Vec<String>;

    #[inline]
    fn run(self, index: &RwLock<Index>) -> Self::Output {
        index.read().get_properties_with_bit(self.bit)
    }
}

#[derive(Deserialize, Debug)]
pub struct SetBit {
    bit: u32,
    properties: Vec<String>,
}

impl Operation for SetBit {
    type Output = bool;

    #[inline]
    fn run(self, index: &RwLock<Index>) -> Self::Output {
        index.write().set_properties_with_bit(self.bit, &self.properties)
    }
}

#[derive(Deserialize, Debug)]
pub struct DeleteBits {
    bits: Vec<u32>,
}

impl Operation for DeleteBits {
    type Output = ();

    #[inline]
    fn run(self, index: &RwLock<Index>) {
        index.write().unset_all(&self.bits);
    }
}

// #[derive(Deserialize, Debug)]
// #[serde(tag = "type")]
// pub enum Op {
//     Query(Query),
//     Count(Count),
//     Stats(Stats),
//     Set(Set),
//     SetMany(SetMany),
//     Unset(Unset),
//     UnsetMany(UnsetMany),
//     GetBit(GetBit),
//     SetBit(SetBit),
//     DeleteBits(DeleteBits),
// }
