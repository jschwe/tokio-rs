//! Contains metadata for the block configuration

use crate::loom::sync::atomic::{AtomicUsize};


fn index_mask(num_idx_bits: usize) -> usize {
    (1 << num_idx_bits ) - 1
}

pub(crate) fn unpack(idx_and_version: usize, num_idx_bits: usize) -> (usize, usize) {
    let idx = idx_and_version & index_mask(num_idx_bits);
    let vsn = (idx_and_version & !(index_mask(num_idx_bits))) >> num_idx_bits;
    (idx, vsn)
}

pub(crate) fn pack(index: usize, version: usize, num_idx_bits: usize) -> usize {
    let shifted_version = version << num_idx_bits;
    debug_assert_eq!(index & !index_mask(num_idx_bits), 0);
    shifted_version | index
}

/// Creates a new instance for an `Owner` field (producer or consumer)
pub(crate) fn new_owner(is_queue_head: bool, num_idx_bits: usize) -> AtomicUsize {
    let (index, version) = if is_queue_head {
        // The first block (head) starts at version one and with an empty index
        // to indicate readiness to produce/consume once values where produced.
        (0, 1)
    } else {
        // The remaining blocks start one version behind and are marked as fully
        // produced/consumed.
        // FIXME: Should be entries per block. Only matches for powers of two!
        (index_mask(num_idx_bits), 0)
    };
    let packed = pack(index, version, num_idx_bits);
    AtomicUsize::new(packed)
}

/// Creates a new instance for a `Stealer` field. The main difference to
/// [new_owner](Self::new_owner) is that the stealer is always initialized as full,
/// i.e. not ready for stealing. This is because the queue head is reserved for the
/// consumer and the stealer may not steal from the same block the consumer is on.
pub(crate) fn new_stealer(is_queue_head: bool, num_idx_bits: usize) -> AtomicUsize {
    let packed = pack(index_mask(num_idx_bits), is_queue_head as usize, num_idx_bits);
    AtomicUsize::new(packed)
}