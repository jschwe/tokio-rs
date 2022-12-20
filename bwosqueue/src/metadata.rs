//! Contains metadata for the block configuration

use crate::loom::sync::atomic::{AtomicUsize};


fn index_mask<const NE: usize>() -> usize {
    (1 << index_num_bits::<NE>() ) - 1
}

fn index_num_bits<const NE: usize>() -> usize {
    ((NE+1).next_power_of_two() as f32).log2() as usize
}

pub(crate) fn unpack<const NE: usize>(idx_and_version: usize) -> (usize, usize) {
    let idx = idx_and_version & index_mask::<NE>();
    let vsn = (idx_and_version & !(index_mask::<NE>())) >> index_num_bits::<NE>();
    (idx, vsn)
}

pub(crate) fn pack<const NE: usize>(index: usize, version: usize) -> usize {
    let shifted_version = version << index_num_bits::<NE>();
    debug_assert_eq!(index & !index_mask::<NE>(), 0);
    shifted_version | index
}

/// Creates a new instance for an `Owner` field (producer or consumer)
pub(crate) fn new_owner<const NE: usize>(is_queue_head: bool) -> AtomicUsize {
    let (index, version) = if is_queue_head {
        // The first block (head) starts at version one and with an empty index
        // to indicate readiness to produce/consume once values where produced.
        (0, 1)
    } else {
        // The remaining blocks start one version behind and are marked as fully
        // produced/consumed.
        (NE, 0)
    };
    let packed = pack::<NE>(index, version);
    AtomicUsize::new(packed)
}

/// Creates a new instance for a `Stealer` field. The main difference to
/// [new_owner](Self::new_owner) is that the stealer is always initialized as full,
/// i.e. not ready for stealing. This is because the queue head is reserved for the
/// consumer and the stealer may not steal from the same block the consumer is on.
pub(crate) fn new_stealer<const NE: usize>(is_queue_head: bool) -> AtomicUsize {
    let packed = pack::<NE>(index_mask::<NE>(), is_queue_head as usize);
    AtomicUsize::new(packed)
}