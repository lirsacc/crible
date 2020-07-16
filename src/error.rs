use std::sync::PoisonError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CribleError {
    #[error("facet {0} does not exist")]
    FacetDoesNotExist(String),
    // TODO: This may not need to be an error kind as I think there is no
    // recovery path.
    #[error("concurrency error when trying to acquire lock")]
    ConcurrencyError,
    #[error("csv error")]
    CSVError(#[from] csv::Error),
    #[error("integer parsing error error")]
    IntegerParsingError(#[from] std::num::ParseIntError),
    #[error("io error")]
    IOError(#[from] std::io::Error),
}

impl<T> From<PoisonError<T>> for CribleError {
    fn from(_e: PoisonError<T>) -> CribleError {
        CribleError::ConcurrencyError
    }
}

pub type Result<T> = std::result::Result<T, CribleError>;
