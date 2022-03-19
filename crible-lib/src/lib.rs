#![deny(unstable_features)]
#![forbid(unsafe_code)]
#![warn(
    clippy::print_stdout,
    clippy::mut_mut,
    clippy::large_types_passed_by_value,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications
)]

pub mod expression;
pub mod index;
