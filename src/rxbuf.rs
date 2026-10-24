//! Buffers for what arrives over the data channel. By default they are plain
//! `Vec`s; an application can plug in a pool with `set_rx_pool`, so the
//! decrypted packet is handed to SCTP as `Bytes` of pooled memory, and a
//! message merged from its chunks (`Delivery::Merged`) is one pooled buffer.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;

/// A decrypted SCTP packet (a DTLS record is at most an MTU).
pub(crate) const PACKET: usize = 2048;
/// A merged message; larger ones get a buffer of their own.
pub(crate) const MESSAGE: usize = 17 * 1024;

/// A source of receive buffers.
pub trait RxPool: Send + Sync + 'static {
    /// A buffer of at least `min` bytes, whose contents are unspecified; `None`
    /// if the pool has none that large (a `Vec` is used then).
    fn get(&self, min: usize) -> Option<Box<dyn RxBuf>>;
}

/// A buffer on loan from an `RxPool`.
pub trait RxBuf: Send {
    /// The whole buffer, to write into.
    fn spare(&mut self) -> &mut [u8];
    /// The first `len` bytes as `Bytes`, without a copy where the pool can.
    /// The memory goes back to the pool when the last clone is dropped.
    fn into_bytes(self: Box<Self>, len: usize) -> Bytes;
}

struct VecBuf(Vec<u8>);

impl VecBuf {
    fn new(min: usize) -> Box<dyn RxBuf> {
        Box::new(VecBuf(vec![0; min]))
    }
}

impl RxBuf for VecBuf {
    fn spare(&mut self) -> &mut [u8] {
        &mut self.0
    }

    fn into_bytes(mut self: Box<Self>, len: usize) -> Bytes {
        self.0.truncate(len);
        Bytes::from(self.0)
    }
}

static POOL: OnceLock<Arc<dyn RxPool>> = OnceLock::new();

/// Use `pool` for receive buffers, from now on and for every `Rtc`. Only the
/// first call has an effect; returns whether it was this one.
pub fn set_rx_pool(pool: Arc<dyn RxPool>) -> bool {
    POOL.set(pool).is_ok()
}

pub(crate) fn get(min: usize) -> Box<dyn RxBuf> {
    POOL.get().and_then(|pool| pool.get(min)).unwrap_or_else(|| VecBuf::new(min))
}
