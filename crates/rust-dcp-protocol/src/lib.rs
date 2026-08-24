//! Wire-level primitives for the Couchbase DCP protocol.
//!
//! This crate deliberately has no networking or checkpoint responsibilities.

#![forbid(unsafe_code)]

/// Size of a classic Memcached binary protocol header.
pub const HEADER_LEN: usize = 24;
