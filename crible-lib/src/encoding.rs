use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::str::FromStr;

use serde_derive::{Deserialize, Serialize};
use thiserror::Error;

use crate::expression::validate_property_name;
use crate::index::Index;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid json line")]
    Json(#[from] serde_json::Error),
    #[error("invalid bincode data")]
    Bincode(#[from] bincode::Error),
    #[error("io error")]
    IO(#[from] std::io::Error),
    #[error("duplicate property {0:?}")]
    DuplicateProperty(String),
    #[error("invalid property {0:?}")]
    InvalidProperty(String),
    #[error("invalid bitmap for property {0:?}")]
    InvalidBitmap(String),
    #[error("unknown encoder {0}")]
    UnknownEncoder(String),
}

type Result<T> = std::result::Result<T, Error>;

/// Encoding formats for the index.
/// WARN: There are currently no backwards compatibility guarantees although the
/// Json encoding should be more stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoder {
    /// The `Json` format is a new line delimited json encoded file where every
    /// line is an object containing the `property` as a string and the
    /// `values` as an array of numbers.
    ///
    /// It's not ideal for compression but should be easy to inspect and
    /// manipulate from any environment. Given that it's also independent from
    /// the underlying Bitmap representation it should be mostly safe to use
    /// across updates (only structural changes would impact it and we can make
    /// those backwards compatible).
    Json,
    /// The `Bin` format is the internal representation used by this library
    /// and is suitable to ship an index across machines independent of the
    /// backend used.
    // TODO: Bincode might be hard to evolve over time, we should consider some
    // versioning scheme here.
    Bin,
}

impl Encoder {
    pub fn decode<R: Read>(self, r: R) -> Result<Index> {
        match self {
            Self::Json => decode_ndjson(r),
            Self::Bin => decode_bincode(r),
        }
    }

    pub fn encode<W: Write>(self, w: W, index: &Index) -> Result<()> {
        match self {
            Self::Json => encode_ndjson(w, index),
            Self::Bin => encode_bincode(w, index),
        }
    }

    // Convenience for use internal to this crate. Real usage for crible should
    // go through the FsBackend.

    pub fn load_index_from_file<P: AsRef<Path>>(
        self,
        path: P,
    ) -> Result<Index> {
        let f = std::fs::OpenOptions::new().read(true).open(path.as_ref())?;
        self.decode(f)
    }

    pub fn save_index_from_file<P: AsRef<Path>>(
        self,
        path: P,
        index: &Index,
    ) -> Result<()> {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(path.as_ref())?;
        self.encode(f, index)
    }
}

impl FromStr for Encoder {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "" | "bin" | "crible" => Ok(Encoder::Bin),
            "json" | "ndjson" | "ljson" => Ok(Encoder::Json),
            x => Err(Error::UnknownEncoder(x.to_owned())),
        }
    }
}

#[derive(Debug, Deserialize)]
struct JsonLineRecordIn {
    property: String,
    values: Vec<u32>,
}

#[derive(Debug, Serialize)]
struct JsonLineRecordOut<'a> {
    property: &'a String,
    values: Vec<u32>,
}

fn decode_ndjson_line(index: &mut Index, bytes: &[u8]) -> Result<()> {
    let record: JsonLineRecordIn = serde_json::from_slice(bytes)?;

    if !validate_property_name(record.property.as_ref()) {
        return Err(Error::InvalidProperty(record.property.clone()));
    }

    match index.get_property(&record.property) {
        None => {
            index.set_many(record.property.as_ref(), &record.values);
            Ok(())
        }
        Some(_) => Err(Error::DuplicateProperty(record.property)),
    }
}

fn decode_ndjson<R: Read>(r: R) -> Result<Index> {
    let mut index = Index::default();
    for x in BufReader::new(r).lines() {
        let ln = x?;
        if ln.is_empty() {
            continue;
        }
        decode_ndjson_line(&mut index, ln.as_ref())?;
    }
    Ok(index)
}

fn encode_ndjson<W: Write>(mut w: W, index: &Index) -> Result<()> {
    let mut sorted_pairs = index.inner().iter().collect::<Vec<_>>();
    sorted_pairs.sort_by_key(|(k, _)| *k);
    for (property, bm) in sorted_pairs {
        let data = serde_json::to_vec(&JsonLineRecordOut {
            property,
            values: bm.to_vec(),
        })?;
        w.write_all(&data)?;
        writeln!(&mut w)?;
    }
    Ok(())
}

type BincodeIntermediate = Vec<(String, Vec<u8>)>;

fn decode_bincode_intermediate(data: BincodeIntermediate) -> Result<Index> {
    let mut index = Index::default();
    for (property, bytes) in data {
        match index.get_property(&property) {
            None => match croaring::Bitmap::try_deserialize(&bytes) {
                None => {
                    return Err(Error::InvalidBitmap(property));
                }
                Some(bm) => {
                    index.set_property(property.as_ref(), bm);
                }
            },
            Some(_) => {
                return Err(Error::DuplicateProperty(property));
            }
        }
    }
    Ok(index)
}

fn decode_bincode<R: Read>(r: R) -> Result<Index> {
    let data: BincodeIntermediate = bincode::deserialize_from(r)?;
    decode_bincode_intermediate(data)
}

fn encode_bincode_intermediate(index: &Index) -> Result<Vec<u8>> {
    let mut sorted_pairs: BincodeIntermediate = index
        .inner()
        .iter()
        .map(|(k, bm)| (k.to_owned(), bm.serialize()))
        .collect::<Vec<_>>();
    sorted_pairs.sort_by_key(|(k, _)| k.clone());
    Ok(bincode::serialize(&sorted_pairs)?)
}

fn encode_bincode<W: Write>(mut w: W, index: &Index) -> Result<()> {
    w.write_all(&encode_bincode_intermediate(index)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str;

    use super::Encoder;
    use crate::Index;

    macro_rules! test_index {
        () => {
            Index::of([
                ("foo", vec![1, 2, 3, 4, 9]),
                ("bar", vec![1, 3, 5, 6, 7]),
                ("baz", vec![4, 6, 8, 9]),
            ])
        };
    }

    const TEST_JSON_ENCODED: &str = "\
{\"property\":\"bar\",\"values\":[1,3,5,6,7]}
{\"property\":\"baz\",\"values\":[4,6,8,9]}
{\"property\":\"foo\",\"values\":[1,2,3,4,9]}
";

    #[test]
    fn test_ndjson_decode_empty() {
        let index = Encoder::Json.decode("".as_bytes()).unwrap();
        assert!(index.is_empty());
    }

    #[test]
    fn test_ndjson_encode_empty() {
        let index = Index::default();
        let mut out: Vec<u8> = Vec::new();
        Encoder::Json.encode(&mut out, &index).unwrap();

        assert_eq!(str::from_utf8(&out).unwrap(), "");
    }

    #[test]
    fn test_ndjson_encode() {
        let index = test_index!();
        let mut out: Vec<u8> = Vec::new();
        Encoder::Json.encode(&mut out, &index).unwrap();
        assert_eq!(str::from_utf8(&out).unwrap(), TEST_JSON_ENCODED);
    }

    #[test]
    fn test_ndjson_decode() {
        let index = Encoder::Json.decode(TEST_JSON_ENCODED.as_bytes()).unwrap();

        assert_eq!(index, test_index!());

        let mut out: Vec<u8> = Vec::new();
        Encoder::Json.encode(&mut out, &index).unwrap();

        assert_eq!(str::from_utf8(&out).unwrap(), TEST_JSON_ENCODED);
    }

    #[test]
    fn test_bincode_encode_decode_loop_empty() {
        let index = Index::default();
        let mut out: Vec<u8> = Vec::new();
        Encoder::Bin.encode(&mut out, &index).unwrap();

        let decoded = Encoder::Bin.decode(out.as_slice()).unwrap();

        assert_eq!(index, decoded);
    }

    #[test]
    fn test_bincode_encode_decode_loop() {
        let index = test_index!();

        let mut out: Vec<u8> = Vec::new();
        Encoder::Bin.encode(&mut out, &index).unwrap();

        let decoded = Encoder::Bin.decode(out.as_slice()).unwrap();

        assert_eq!(index, decoded);
    }
}
