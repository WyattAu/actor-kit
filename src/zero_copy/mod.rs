//! Zero-Copy Message Path via rkyv
//!
//! Provides zero-copy serialization/deserialization for inter-actor messaging
//! on the same node, avoiding the allocation overhead of the postcard (CBOR)
//! path.
//!
//! # Usage
//!
//! ```ignore
//! use actor_kit::zero_copy::{encode_rkyv, decode_rkyv};
//!
//! let value = MyMessage { id: 42, data: vec![1, 2, 3] };
//! let bytes = encode_rkyv(&value)?;
//! let restored: MyMessage = decode_rkyv(&bytes)?;
//! ```

mod rkyv_impl;
pub use rkyv_impl::*;
