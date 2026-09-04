//! rkyv-backed zero-copy message implementation.

use crate::error::{Error, Result};
use std::marker::PhantomData;

type HighSer<'a> = rkyv::api::high::HighSerializer<
    rkyv::util::AlignedVec,
    rkyv::ser::allocator::ArenaHandle<'a>,
    rkyv::rancor::Error,
>;
type HighVal<'a> = rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>;

/// Encode a value into rkyv bytes.
///
/// Returns the serialized byte representation that can be sent across actor
/// boundaries without further allocation.
///
/// # Errors
///
/// Returns [`Error::serialization`] when encoding fails.
pub fn encode_rkyv<T>(value: &T) -> Result<Vec<u8>>
where
    T: for<'a> rkyv::Serialize<HighSer<'a>>,
{
    rkyv::to_bytes::<rkyv::rancor::Error>(value)
        .map(|b| b.to_vec())
        .map_err(|e| Error::serialization(format!("rkyv encode failed: {e}")))
}

/// Decode rkyv bytes into an owned value.
///
/// This performs a full deserialization, producing an owned `T`. For
/// zero-copy access see [`decode_rkyv_ref`].
///
/// # Errors
///
/// Returns [`Error::serialization`] when decoding fails.
pub fn decode_rkyv<T>(bytes: &[u8]) -> Result<T>
where
    T: rkyv::Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<HighVal<'a>>
        + rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
    rkyv::from_bytes::<T, rkyv::rancor::Error>(bytes)
        .map_err(|e| Error::serialization(format!("rkyv decode failed: {e}")))
}

/// Decode rkyv bytes as a zero-copy reference.
///
/// The returned reference borrows from `bytes`, so the byte buffer must
/// outlive the reference. This avoids all allocation during deserialization.
///
/// The `bytecheck` feature (enabled in the workspace `rkyv` dependency)
/// validates the archive at runtime.
///
/// # Errors
///
/// Returns [`Error::serialization`] when validation or access fails.
pub fn decode_rkyv_ref<T>(bytes: &[u8]) -> Result<&T::Archived>
where
    T: rkyv::Archive,
    T::Archived: rkyv::Portable + for<'a> rkyv::bytecheck::CheckBytes<HighVal<'a>>,
{
    rkyv::access::<T::Archived, rkyv::rancor::Error>(bytes)
        .map_err(|e| Error::serialization(format!("rkyv zero-copy access failed: {e}")))
}

/// Wrapper holding rkyv-serialized bytes for an actor message.
///
/// `ZeroCopyMessage<T>` is the transport type used when sending messages
/// between actors on the same node. It stores the serialized byte
/// representation and provides methods for zero-copy access or full
/// deserialization on the receiving side.
///
/// # Type Parameters
///
/// * `T` - The original message type (used only as a type marker; the
///   archived bytes are self-contained).
pub struct ZeroCopyMessage<T> {
    bytes: Vec<u8>,
    _marker: PhantomData<T>,
}

impl<T> ZeroCopyMessage<T> {
    /// Create a new zero-copy message by serializing `value` with rkyv.
    ///
    /// # Errors
    ///
    /// Returns [`Error::serialization`] if rkyv encoding fails.
    pub fn new(value: &T) -> Result<Self>
    where
        T: for<'a> rkyv::Serialize<HighSer<'a>>,
    {
        Ok(Self {
            bytes: encode_rkyv(value)?,
            _marker: PhantomData,
        })
    }

    /// Access the serialized byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consume the wrapper and return the underlying byte buffer.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Reconstruct the wrapper from a previously serialized byte slice.
    ///
    /// Validates the bytes via bytecheck before accepting them.
    ///
    /// # Errors
    ///
    /// Returns [`Error::serialization`] if the bytes fail validation.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self>
    where
        T: rkyv::Archive,
        T::Archived: rkyv::Portable + for<'a> rkyv::bytecheck::CheckBytes<HighVal<'a>>,
    {
        rkyv::access::<T::Archived, rkyv::rancor::Error>(&bytes)
            .map_err(|e| Error::serialization(format!("rkyv validation failed: {e}")))?;
        Ok(Self {
            bytes,
            _marker: PhantomData,
        })
    }

    /// Deserialize into an owned value.
    ///
    /// # Errors
    ///
    /// Returns [`Error::serialization`] if deserialization fails.
    pub fn decode(&self) -> Result<T>
    where
        T: rkyv::Archive,
        T::Archived: for<'a> rkyv::bytecheck::CheckBytes<HighVal<'a>>
            + rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
    {
        decode_rkyv(&self.bytes)
    }

    /// Obtain a zero-copy reference to the archived value.
    ///
    /// The returned reference borrows from `self`, so the `ZeroCopyMessage`
    /// must remain alive.
    ///
    /// # Errors
    ///
    /// Returns [`Error::serialization`] if bytecheck validation fails.
    pub fn decode_ref(&self) -> Result<&T::Archived>
    where
        T: rkyv::Archive,
        T::Archived: rkyv::Portable + for<'a> rkyv::bytecheck::CheckBytes<HighVal<'a>>,
    {
        decode_rkyv_ref::<T>(&self.bytes)
    }

    /// Returns the size of the serialized payload in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns `true` if the serialized payload is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl<T> std::fmt::Debug for ZeroCopyMessage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZeroCopyMessage")
            .field("size", &self.bytes.len())
            .finish()
    }
}

/// Result of a serialization latency measurement.
#[derive(Debug, Clone, Copy)]
pub struct SerializationLatency {
    /// Time spent encoding, in nanoseconds.
    pub encode_ns: u64,
    /// Time spent decoding (owned), in nanoseconds.
    pub decode_ns: u64,
    /// Time spent decoding (zero-copy ref access), in nanoseconds.
    pub decode_ref_ns: u64,
    /// Size of the serialized payload in bytes.
    pub payload_bytes: usize,
}

/// Measure the encoding/decoding latency for a value.
///
/// Runs one encode, one owned decode, and one zero-copy decode, returning
/// wall-clock timings via [`std::time::Instant`].
///
/// # Errors
///
/// Returns [`Error::serialization`] if any step fails.
pub fn measure_latency<T>(value: &T) -> Result<SerializationLatency>
where
    T: rkyv::Archive + for<'a> rkyv::Serialize<HighSer<'a>>,
    T::Archived: rkyv::Portable
        + rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<HighVal<'a>>,
{
    use std::time::Instant;

    let start = Instant::now();
    let bytes = encode_rkyv(value)?;
    let encode_ns = start.elapsed().as_nanos() as u64;
    let payload_bytes = bytes.len();

    let start = Instant::now();
    let _owned: T = decode_rkyv(&bytes)?;
    let decode_ns = start.elapsed().as_nanos() as u64;

    let start = Instant::now();
    let _ref: &T::Archived = decode_rkyv_ref::<T>(&bytes)?;
    let decode_ref_ns = start.elapsed().as_nanos() as u64;

    Ok(SerializationLatency {
        encode_ns,
        decode_ns,
        decode_ref_ns,
        payload_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[rkyv(compare(PartialEq), derive(Debug))]
    struct SimpleMessage {
        id: u64,
        label: String,
        flag: bool,
    }

    #[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[rkyv(compare(PartialEq), derive(Debug))]
    struct InnerData {
        values: Vec<u32>,
        name: String,
    }

    #[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[rkyv(compare(PartialEq), derive(Debug))]
    struct NestedMessage {
        id: u64,
        inner: InnerData,
        extra: Option<String>,
    }

    #[test]
    fn roundtrip_simple_struct() {
        let original = SimpleMessage {
            id: 42,
            label: "hello".to_string(),
            flag: true,
        };

        let bytes = encode_rkyv(&original).expect("encode failed");
        let restored: SimpleMessage = decode_rkyv(&bytes).expect("decode failed");

        assert_eq!(restored, original);
    }

    #[test]
    fn roundtrip_nested_struct() {
        let original = NestedMessage {
            id: 99,
            inner: InnerData {
                values: vec![1, 2, 3, 4, 5],
                name: "nested".to_string(),
            },
            extra: Some("optional data".to_string()),
        };

        let bytes = encode_rkyv(&original).expect("encode failed");
        let restored: NestedMessage = decode_rkyv(&bytes).expect("decode failed");

        assert_eq!(restored, original);
    }

    #[test]
    fn zero_copy_ref_access() {
        let original = SimpleMessage {
            id: 7,
            label: "zero-copy".to_string(),
            flag: false,
        };

        let bytes = encode_rkyv(&original).expect("encode failed");
        let archived: &<SimpleMessage as rkyv::Archive>::Archived =
            decode_rkyv_ref::<SimpleMessage>(&bytes).expect("zero-copy access failed");

        assert_eq!(archived.id, original.id);
        assert_eq!(archived.label.as_str(), original.label.as_str());
        assert_eq!(archived.flag, original.flag);
    }

    #[test]
    fn zero_copy_message_wrapper_roundtrip() {
        let original = NestedMessage {
            id: 1,
            inner: InnerData {
                values: vec![10, 20],
                name: "wrapper-test".to_string(),
            },
            extra: None,
        };

        let msg = ZeroCopyMessage::new(&original).expect("new failed");
        assert!(!msg.is_empty());

        let restored: NestedMessage = msg.decode().expect("decode failed");
        assert_eq!(restored, original);
    }

    #[test]
    fn zero_copy_message_ref_access() {
        let original = SimpleMessage {
            id: 123,
            label: "ref-access".to_string(),
            flag: true,
        };

        let msg = ZeroCopyMessage::new(&original).expect("new failed");
        let archived: &<SimpleMessage as rkyv::Archive>::Archived =
            msg.decode_ref().expect("decode_ref failed");

        assert_eq!(archived.id, original.id);
        assert_eq!(archived.label.as_str(), original.label.as_str());
    }

    #[test]
    fn zero_copy_message_from_bytes_roundtrip() {
        let original = SimpleMessage {
            id: 55,
            label: "from-bytes".to_string(),
            flag: false,
        };

        let msg = ZeroCopyMessage::new(&original).expect("new failed");
        let raw = msg.into_bytes();

        let msg2 = ZeroCopyMessage::from_bytes(raw).expect("from_bytes failed");
        let restored: SimpleMessage = msg2.decode().expect("decode failed");
        assert_eq!(restored, original);
    }

    #[test]
    fn measure_latency_does_not_panic() {
        let value = NestedMessage {
            id: 1,
            inner: InnerData {
                values: (0..1000).collect(),
                name: "bench".to_string(),
            },
            extra: Some("data".to_string()),
        };

        let latency = measure_latency(&value).expect("measure_latency failed");
        assert!(latency.encode_ns > 0);
        assert!(latency.decode_ns > 0);
        assert!(latency.decode_ref_ns > 0);
        assert!(latency.payload_bytes > 0);
    }
}
