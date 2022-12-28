//! The BWoS queue is a fast block-based work stealing queue for parallel processing.
//!
//! The BWoS queue is based on the [BBQ] (Block-based Bounded Queue) and is specially designed for the
//! workstealing scenario. Based on the real-world observation that the "stealing" operation is
//! rare and most of the operations are local enqueues and dequeues this queue implementation
//! offers a single [Owner] which can enqueue and dequeue without any heavy synchronization mechanisms
//! on the fast path. Concurrent stealing is possible and does not slow done the Owner too much.
//! This allows stealing policies which steal single items or in small batches.
//!
//! # Queue Semantics
//!
//! - The block-based design reduces the synchronization requirements on the fast-path
//!   inside a block and moves the heavy synchronization operations necessary to support
//!   multiple stealers to the slow-path when transitioning to the next block.
//! - The producer (enqueue) may not advance to the next block if the consumer or a stealer
//!   is still operating on that block. This allows the producer to remove producer-consumer/stealer
//!   synchronization from its fast-path operations, but reduces the queue capacity by
//!   at most one block.
//! - Stealers may not steal from the same block as the consumer. This allows the consumer
//!   to remove consumer-stealer synchronization from its fast-path operations, but means
//!   one block is not available for stealing.
//! - Consumers may "take-over" the next block preventing stealers from stealing in that
//!   block after the take-over. Stealers will still proceed with already in-progress steal
//!   operations in this block.
//! - This queue implementation puts the producer and consumer into a shared Owner struct,
//!
//! # Examples
//!
//!  todo
//!
//! [BBQ]: https://www.usenix.org/conference/atc22/presentation/wang-jiawei
//!
//! # Todo:
//! - Instead of const generics we could use a boxed slice for a dynamically sized array.
//!   The performance impact be benchmarked though, since this will result in multiple operations
//!   not being able to be calculated at compile-time anymore.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(unreachable_pub)]

use cache_padded::CachePadded;
use core::{
    marker::{Send, Sync},
    pin::Pin,
};
use std::fmt::Formatter;
use std::mem::MaybeUninit;

mod bwos_queue;
mod loom;
mod metadata;

use crate::loom::cell::UnsafeCell;
use crate::loom::sync::atomic::{
    AtomicUsize,
    Ordering::{Acquire, Relaxed, Release},
};
use crate::loom::sync::Arc;
use bwos_queue::{Block, BwsQueue};
use crate::metadata::{pack, unpack};

/// The Owner interface to the BWoS queue
///
/// The owner is both the single producer and single consumer.
#[repr(align(128))]
pub struct Owner<E, const ENTRIES_PER_BLOCK: usize> {
    /// Producer cache (single producer）- points to block in self.queue.
    pcache: CachePadded<*const Block<E, { ENTRIES_PER_BLOCK }>>,
    /// Consumer cache (single consumer) - points to block in self.queue.
    ccache: CachePadded<*const Block<E, { ENTRIES_PER_BLOCK }>>,
    /// Stealer position cache - Allows the owner to quickly check if there are any stealers
    spos: CachePadded<Arc<AtomicUsize>>,
    /// `Arc` to the actual queue to ensure the queue lives at least as long as the Owner.
    #[allow(dead_code)]
    queue: Pin<Arc<BwsQueue<E, ENTRIES_PER_BLOCK>>>,
}

/// A Stealer interface to the BWoS queue
///
/// There may be multiple stealers. Stealers share the stealer position which is used to quickly look up
/// the next block for attempted stealing.
#[repr(align(128))]
pub struct Stealer<E, const ENTRIES_PER_BLOCK: usize> {
    /// The actual stealer position is `self.spos % NUM_BLOCKS`. The position is incremented beyond
    /// `NUM_BLOCKS` to detect ABA problems.
    spos: CachePadded<Arc<AtomicUsize>>,
    queue: Pin<Arc<BwsQueue<E, ENTRIES_PER_BLOCK>>>,
}

/// An iterator over elements of one Block
pub struct BlockIter<'a, E, const ENTRIES_PER_BLOCK: usize> {
    buffer: &'a [UnsafeCell<MaybeUninit<E>>; ENTRIES_PER_BLOCK],
    i: usize,
}

/// An iterator over elements of one Block of a stealer
///
/// Marks the stolen entries as stolen once the iterator has been consumed.
pub struct StealerBlockIter<'a, E, const ENTRIES_PER_BLOCK: usize> {
    /// Stealer Block
    stealer_block: &'a Block<E, ENTRIES_PER_BLOCK>,
    /// Remember how many entries where reserved for the Drop implementation
    num_reserved: usize,
    /// reserved index of the block. We own the entries from `i..block_reserved`
    block_reserved: usize,
    /// curr index in the block
    i: usize,
}

unsafe impl<E, const ENTRIES_PER_BLOCK: usize> Send
    for Owner<E, ENTRIES_PER_BLOCK>
{
}

unsafe impl<E, const ENTRIES_PER_BLOCK: usize> Send
    for Stealer<E, ENTRIES_PER_BLOCK>
{
}

unsafe impl<E, const ENTRIES_PER_BLOCK: usize> Sync
    for Stealer<E, ENTRIES_PER_BLOCK>
{
}

impl<E, const ENTRIES_PER_BLOCK: usize>
    Owner<E, ENTRIES_PER_BLOCK>
{
    /// Try to enqueue `t` into the FIFO queue.
    ///
    /// If the queue is full, `Err(t)` is returned to the caller.
    #[inline(always)]
    pub fn enqueue(&mut self, t: E) -> Result<(), E> {
        loop {
            // SAFETY: `pcache` always points to a valid `Block` in the queue. We never create a mutable reference
            // to a Block, so it is safe to construct a shared reference here.
            let blk = unsafe { &**self.pcache };

            // Load the index of the next free queue entry for the producer. `committed` is only written to by the
            // single producer, so `Relaxed` reading is fine.
            let committed = blk.committed.load(Relaxed);
            let (committed_idx, committed_vsn) = unpack(committed, self.queue.num_index_bits);

            // Fastpath (the block is not full): Due to the slowpath checks we know that the entire remaining block
            // is available to the producer and do not need to check the consumed index in the fastpath.
            if let Some(entry_cell) = blk.entries.get(committed_idx) {
                // SAFETY: We checked the entry is available for writing and the index can be
                // post-incremented unconditionally since `index == NE` is valid and means the block
                // is full.
                let committed_new = unsafe {
                    entry_cell.with_mut(|uninit_entry| uninit_entry.write(MaybeUninit::new(t)));
                    committed + 1
                };
                // Synchronizes with `Acquire` ordering on the stealer side.
                blk.committed.store(committed_new, Release);
                #[cfg(feature = "stats")]
                self.queue.stats.increment_enqueued(1);
                return Ok(());
            }

            /* slow path, move to the next block */
            let nblk = unsafe { &*blk.next() };
            let next_version = committed_vsn + nblk.is_head() as usize;

            /* check if next block is ready */
            if !self.is_next_block_writable(nblk, next_version) {
                return Err(t);
            };

            let next = pack(0, next_version, self.queue.num_index_bits);
            /* reset cursor and advance block */
            nblk.committed.store(next, Relaxed);
            nblk.stolen.store(next, Relaxed);
            // Ensures the writes to `committed` and `stolen` are visible when `reserved` is loaded.
            nblk.reserved.store(next, Release);
            *self.pcache = nblk;
        }
    }

    pub fn enqueue_stolen_block<'a>(
        &mut self,
        mut iter: StealerBlockIter<'a, E, ENTRIES_PER_BLOCK>,
    ) -> Result<(), StealerBlockIter<'a, E, ENTRIES_PER_BLOCK>> {
        loop {
            let num_items = iter.len();
            if num_items == 0 {
                return Ok(());
            }

            // SAFETY: `pcache` always points to a valid `Block` in the queue. We never create a mutable reference
            // to a Block, so it is safe to construct a shared reference here.
            let blk = unsafe { &**self.pcache };

            // Load the index of the next free queue entry for the producer. `committed` is only written to by the
            // single producer, so `Relaxed` reading is fine.
            let committed = blk.committed.load(Relaxed);
            let (committed_idx, committed_vsn) = unpack(committed, self.queue.num_index_bits);

            if committed_idx < ENTRIES_PER_BLOCK {
                // Fastpath (the block is not full): Due to the slowpath checks we know that the entire remaining block
                // is available to the producer and do not need to check the consumed index in the
                // fastpath.
                let max_index = core::cmp::min(ENTRIES_PER_BLOCK, committed_idx + num_items);
                for i in committed_idx..max_index {
                    let entry = iter
                        .next()
                        .expect("Impossible: Iterator magically lost item");
                    blk.entries[i].with_mut(|uninit_entry| unsafe {
                        uninit_entry.write(MaybeUninit::new(entry))
                    });
                }
                let committed_new = committed + (max_index - committed_idx);
                blk.committed.store(committed_new, Release);
                #[cfg(feature = "stats")]
                self.queue
                    .stats
                    .increment_enqueued(max_index - committed_idx);
                continue;
            }
            /* slow path, move to the next block */
            let nblk = unsafe { &*blk.next() };
            let next_version = committed_vsn + nblk.is_head() as usize;

            /* check if next block is ready */
            if !self.is_next_block_writable(nblk, next_version) {
                return Err(iter);
            };

            let next = pack(0, next_version, self.queue.num_index_bits);

            /* reset cursor and advance block */
            nblk.committed.store(next, Relaxed);
            nblk.stolen.store(next, Relaxed);
            // The changes to committed and stolen must be visible when reserved is changed.
            nblk.reserved.store(next, Release);
            *self.pcache = nblk;
        }
    }
    /// true if the next block is ready for the producer to start writing.
    fn is_next_block_writable(
        &self,
        next_blk: &Block<E, ENTRIES_PER_BLOCK>,
        next_block_version: usize,
    ) -> bool {
        let expected_version = next_block_version.wrapping_sub(1);
        let consumed = next_blk.consumed.load(Relaxed);
        let (consumed_idx, consumed_vsn) = unpack(consumed, self.queue.num_index_bits);
        let is_consumed = consumed_idx >= ENTRIES_PER_BLOCK && expected_version == consumed_vsn;

        // The next block must be already _fully_ consumed, since we do not want to checked the `consumed` index
        // in the enqueue fastpath!
        if !is_consumed {
            return false;
        }
        // The producer must wait until the next block has no active stealers.
        let stolen = next_blk.stolen.load(Acquire);
        let (stolen_idx, stolen_vsn) = unpack(stolen, self.queue.num_index_bits);
        if stolen_idx < ENTRIES_PER_BLOCK || stolen_vsn != expected_version {
            return false;
        }
        true
    }
}

impl<E, const ENTRIES_PER_BLOCK: usize>
    Owner<E, ENTRIES_PER_BLOCK>
{
    /// Try to dequeue the oldest element in the queue.
    #[inline(always)]
    pub fn dequeue(&mut self) -> Option<E> {
        loop {
            // SAFETY: `ccache` always points to a valid `Block` in the queue. We never create a mutable reference
            // to a Block, so it is safe to construct a shared reference here.
            let blk = unsafe { &**self.ccache };

            // check if the block is fully consumed already
            let consumed = blk.consumed.load(Relaxed);
            let (consumed_idx, consumed_vsn) = unpack(consumed, self.queue.num_index_bits);

            // Fastpath (Block is not fully consumed yet)
            if let Some(entry_cell) = blk.entries.get(consumed_idx) {
                // we know the block is not full, but most first check if there is an entry to
                // dequeue.
                let (committed_idx, _) = unpack(blk.committed.load(Relaxed), self.queue.num_index_bits);
                if consumed_idx == committed_idx {
                    return None;
                }

                /* There is an entry to dequeue */

                // SAFETY: We know there is an entry to dequeue, so we know the entry is a valid initialized `E`.
                let item = unsafe { entry_cell.with(|entry| entry.read().assume_init()) };

                blk.consumed.store(consumed + 1, Relaxed);
                #[cfg(feature = "stats")]
                self.queue.stats.increment_dequeued(1);
                return Some(item);
            }

            /* Slow-path */

            /* Consumer head may never pass the Producer head and Consumer/Stealer tail */
            let nblk = unsafe { &*blk.next() };
            if self.try_advance_consumer_block(nblk, consumed_vsn).is_err() {
                return None;
            }
            /* We advanced to the next block - loop around and try again */
        }
    }

    /// Tru to dequeue a whole block
    pub fn dequeue_block(&mut self) -> Option<BlockIter<'_, E, ENTRIES_PER_BLOCK>> {
        loop {
            // SAFETY: `ccache` always points to a valid `Block` in the queue. We never create a mutable reference
            // to a Block, so it is safe to construct a shared reference here.
            let blk = unsafe { &**self.ccache };

            // check if the block is fully consumed already
            let consumed = blk.consumed.load(Relaxed);
            let (consumed_idx, consumed_vsn) = unpack(consumed, self.queue.num_index_bits);

            if consumed_idx < ENTRIES_PER_BLOCK {
                let committed = blk.committed.load(Relaxed);
                let (committed_idx, _) = unpack(committed, self.queue.num_index_bits);
                // for now just return none. We could also return  consumed_idx..committed_idx
                if committed_idx < ENTRIES_PER_BLOCK {
                    return None;
                }

                // We are claiming the tasks **before** reading them out of the buffer.
                // This is safe because only the **current** thread is able to push new
                // tasks.
                //
                // There isn't really any need for memory ordering... Relaxed would
                // work. This is because all tasks are pushed into the queue from the
                // current thread (or memory has been acquired if the local queue handle
                // moved).
                let new_consumed = pack(ENTRIES_PER_BLOCK, consumed_vsn, self.queue.num_index_bits);
                // todo: check if the rhs calculation is faster than the pack.
                debug_assert_eq!(new_consumed, consumed + (ENTRIES_PER_BLOCK - consumed_idx));
                blk.consumed.store(new_consumed, Relaxed);
                #[cfg(feature = "stats")]
                self.queue
                    .stats
                    .increment_dequeued(ENTRIES_PER_BLOCK - consumed_idx);

                // Pre-advance ccache for the next time
                let nblk = unsafe { &*blk.next() };
                // We don't care if this fails. The consumer can try again next time.
                let _ = self.try_advance_consumer_block(nblk, new_consumed);

                return Some(BlockIter {
                    buffer: &blk.entries,
                    i: consumed_idx,
                });
            }

            /* Slow-path */

            /* Consumer head may never pass the Producer head and Consumer/Stealer tail */
            let nblk = unsafe { &*blk.next() };
            if self.try_advance_consumer_block(nblk, consumed_vsn).is_err() {
                return None;
            }

            /* We advanced to the next block - loop around and try again */
        }
    }
    /// Advance consumer to the next block, unless the producer has not reached the block yet.
    fn try_advance_consumer_block(
        &mut self,
        next_block: &Block<E, ENTRIES_PER_BLOCK>,
        curr_consumed_version: usize,
    ) -> Result<(), ()> {
        let next_cons_vsn = curr_consumed_version + next_block.is_head() as usize;
        // The reserved field is updated last in `enqueue()`. It is only updated by the producer
        // (`Owner`), so `Relaxed` is sufficient. If the actual reserved version is not equal to the
        // expected next consumer version, then the producer has not advanced to the next block yet
        // and we must wait.
        let (_, next_reserved_vsn) = unpack(next_block.reserved.load(Relaxed), self.queue.num_index_bits);
        if next_reserved_vsn != next_cons_vsn {
            return Err(());
        }

        /* stop stealers */
        let reserved_new = pack(ENTRIES_PER_BLOCK, next_cons_vsn, self.queue.num_index_bits);
        // todo: Why can this be Relaxed?
        let reserved_old = next_block.reserved.swap(reserved_new, Relaxed);
        let (reserved_old_idx, reserved_old_vsn) = unpack(reserved_old, self.queue.num_index_bits);
        debug_assert_eq!(reserved_old_vsn, next_cons_vsn);

        // Number of entries that can't be stolen anymore because we stopped stealing.
        let num_consumer_owned = ENTRIES_PER_BLOCK.saturating_sub(reserved_old_idx);
        // Increase `stolen`, by the number of entries that can't be stolen anymore and are now up to the
        // consumer to deqeuue. This ensures that, once the stealers have finished stealing the already reserved
        // entries, `nblk.stolen == ENTRIES_PER_BLOCK` holds, i.e. this block is marked as having no active
        // stealers, which will allow the producer to the enter this block again (in the next round).
        next_block.stolen.fetch_add(num_consumer_owned, Relaxed);

        /* advance the block and try again */
        // The consumer must skip already reserved entries.
        next_block.consumed.store(reserved_old, Relaxed);
        *self.ccache = next_block;
        Ok(())
    }

    pub fn has_stealers(&self) -> bool {
        let curr_spos = self.spos.load(Relaxed);
        let num_blocks = self.queue.num_blocks;
        // spos increments beyond NUM_BLOCKS to prevent ABA problems.
        let start_block_idx = curr_spos % num_blocks;
        for i in 0..num_blocks {
            let block_idx = (start_block_idx + i) % num_blocks;
            let blk: &Block<E, ENTRIES_PER_BLOCK> = &self.queue.blocks[block_idx];
            let stolen = blk.stolen.load(Relaxed);
            let reserved = blk.reserved.load(Relaxed);
            let (reserved_idx, _) = unpack(reserved, self.queue.num_index_bits);
            if reserved != stolen {
                return true;
            } else if reserved_idx < ENTRIES_PER_BLOCK {
                return false;
            }
        }
        false
    }

    /// Check if there is a block available for stealing in the queue.
    ///
    /// Note that stealing may still fail for a number of reasons even if this function returned true
    #[cfg(feature = "stats")]
    pub fn has_stealable_block(&self) -> bool {
        let n = self.queue.stats.curr_enqueued();
        // SAFETY: self.ccache always points to a valid Block.
        let (committed_idx, _) = unsafe { unpack((**self.ccache).committed.load(Relaxed), self.queue.num_index_bits) };
        // SAFETY: self.ccache always points to a valid Block.
        let (consumed_idx, _) = unsafe { unpack((**self.ccache).consumed.load(Relaxed), self.queue.num_index_bits) };
        // true if there are more items enqueued in total than enqueued in the current block.
        n > (committed_idx - consumed_idx)
    }

    /// Check if there are items in the queue available for the consumer.
    ///
    /// This function may sporadically provide a wrong result.
    #[cfg(feature = "stats")]
    pub fn can_consume(&self) -> bool {
        self.queue.stats.curr_enqueued() > 0
    }
}

impl<E, const ENTRIES_PER_BLOCK: usize> Clone
    for Stealer<E, ENTRIES_PER_BLOCK>
{
    fn clone(&self) -> Self {
        Self {
            spos: self.spos.clone(),
            queue: self.queue.clone(),
        }
    }
}

impl<E, const ENTRIES_PER_BLOCK: usize>
    Stealer<E, ENTRIES_PER_BLOCK>
{
    /// Try to steal a single item from the queue
    #[inline]
    pub fn steal(&self) -> Option<E> {
        loop {
            let (blk, curr_spos) = self.curr_block();

            /* check if the block is fully reserved */
            let reserved = blk.reserved.load(Acquire);
            let (reserved_idx, reserved_vsn) = unpack(reserved, self.queue.num_index_bits);

            if reserved_idx < ENTRIES_PER_BLOCK {
                /* check if we have an entry to occupy */
                let committed = blk.committed.load(Acquire);
                let (committed_idx, _) = unpack(committed, self.queue.num_index_bits);
                if reserved_idx == committed_idx {
                    return None;
                }
                // SAFETY: We checked before that `reserved_idx` < ENTRIES_PER_BLOCK, so the index
                // can't overflow.
                let new_reserved = reserved + 1;
                let reserve_res =
                    blk.reserved
                        .compare_exchange_weak(reserved, new_reserved, Release, Relaxed);
                if reserve_res.is_err() {
                    return None;
                }

                /* we got the entry */

                #[cfg(feature = "stats")]
                self.queue.stats.increment_stolen(1);

                // SAFETY: We know the entry is a valid and initialized `E` and is now exclusively owned by us.
                let t =
                    unsafe { blk.entries[reserved_idx].with(|entry| entry.read().assume_init()) };
                // `t` is now owned by us so we mark the stealing as finished. Synchronizes with the Owner Acquire.
                let old_stolen = blk.stolen.fetch_add(1, Release);
                debug_assert!(unpack(old_stolen, self.queue.num_index_bits).0 < ENTRIES_PER_BLOCK);
                return Some(t);
            }

            // Slow-path: The current block is already fully reserved. Try to advance to the next block
            if !self.can_advance(blk, reserved_vsn) {
                return None;
            }
            self.try_advance_spos(curr_spos);
        }
    }

    /// Get the current stealer `Block` and the corresponding stealer position (`spos`)
    ///
    /// The returned `spos` can be larger than `NUM_BLOCKS` to detect [ABA](https://en.wikipedia.org/wiki/ABA_problem)
    /// situations.
    fn curr_block(&self) -> (&Block<E, ENTRIES_PER_BLOCK>, usize) {
        let curr_spos = self.spos.load(Relaxed);
        // spos increments beyond NUM_BLOCKS to prevent ABA problems.
        let block_idx = curr_spos % self.queue.num_blocks;
        let blk: &Block<E, ENTRIES_PER_BLOCK> = &self.queue.blocks[block_idx];
        (blk, curr_spos)
    }

    /// Try to steal a block from `self`.
    ///
    /// Tries to steal a full block from `self`. If the block is not fully
    /// committed yet it will steal up to and including the last committed entry
    /// of that block.
    #[inline]
    pub fn steal_block(&self) -> Option<StealerBlockIter<'_, E, ENTRIES_PER_BLOCK>> {
        loop {
            let (blk, curr_spos) = self.curr_block();

            /* check if the block is fully reserved */
            let reserved = blk.reserved.load(Acquire);
            let (reserved_idx, reserved_version) = unpack(reserved, self.queue.num_index_bits);

            if reserved_idx < ENTRIES_PER_BLOCK {
                /* check if we have an entry to occupy */
                let committed = blk.committed.load(Acquire);
                let (committed_idx, _) = unpack(committed, self.queue.num_index_bits);
                if reserved_idx == committed_idx {
                    return None;
                }

                // Try to steal the block up to the latest committed entry
                let reserve_res = blk
                    .reserved
                    .compare_exchange_weak(reserved, committed, Release, Relaxed);

                if reserve_res.is_err() {
                    return None;
                }

                let num_reserved = committed_idx - reserved_idx;
                // From the statistics perspective we consider the reserved range to already be
                // stolen, since it is not available for the consumer or other stealers anymore.
                #[cfg(feature = "stats")]
                self.queue.stats.increment_stolen(num_reserved);
                return Some(StealerBlockIter {
                    stealer_block: blk,
                    block_reserved: committed_idx,
                    i: reserved_idx,
                    num_reserved,
                });
            }

            // Slow-path: The current block is already fully reserved. Try to advance to next block
            if !self.can_advance(blk, reserved_version) {
                return None;
            }
            self.try_advance_spos(curr_spos);
        }
    }

    /// True if the stealer can advance to the next block
    fn can_advance(
        &self,
        curr_block: &Block<E, ENTRIES_PER_BLOCK>,
        curr_reserved_version: usize,
    ) -> bool {
        /* r_head never pass the w_head and r_tail */
        let nblk = unsafe { &*curr_block.next() };
        let next_expect_vsn = curr_reserved_version + nblk.is_head() as usize;
        let next_reserved = nblk.reserved.load(Relaxed);
        let (_, next_actual_vsn) = unpack(next_reserved, self.queue.num_index_bits);
        next_expect_vsn == next_actual_vsn
    }

    /// Try and advance `spos` to the next block.
    ///
    /// We are not interested in the failure case, since the next stealer can just try again.
    fn try_advance_spos(&self, curr_spos: usize) {
        // Ignore result. Failure means a different stealer succeeded in updating
        // the stealer block index. In case of a sporadic failure the next stealer will try again.
        let _ =
            self.spos
                .compare_exchange_weak(curr_spos, curr_spos.wrapping_add(1), Relaxed, Relaxed);
    }

    /// The estimated number of entries currently enqueued.
    #[cfg(feature = "stats")]
    pub fn estimated_queue_entries(&self) -> usize {
        self.queue.estimated_len()
    }
}

impl<'a, E, const ENTRIES_PER_BLOCK: usize> Iterator for BlockIter<'a, E, ENTRIES_PER_BLOCK> {
    type Item = E;

    #[inline]
    fn next(&mut self) -> Option<E> {
        let i = self.i;
        self.i += 1;
        self.buffer.get(i).map(|entry_cell| {
            entry_cell.with(|entry| {
                // SAFETY: we claimed the entries
                unsafe { entry.read().assume_init() }
            })
        })
    }
}

impl<'a, E, const ENTRIES_PER_BLOCK: usize> Iterator
    for StealerBlockIter<'a, E, ENTRIES_PER_BLOCK>
{
    type Item = E;

    #[inline]
    fn next(&mut self) -> Option<E> {
        if self.i < self.block_reserved {
            let entry = self.stealer_block.entries[self.i].with(|entry| {
                // SAFETY: we claimed the entries
                unsafe { entry.read().assume_init() }
            });
            self.i += 1;
            Some(entry)
        } else {
            None
        }
    }
}

impl<'a, E, const ENTRIES_PER_BLOCK: usize> Drop for StealerBlockIter<'a, E, ENTRIES_PER_BLOCK> {
    fn drop(&mut self) {
        // Ensure `Drop` is called on any items that where not consumed, by consuming the iterator,
        // which implicitly dequeues all items
        while self.next().is_some() {}
        self.stealer_block
            .stolen
            .fetch_add(self.num_reserved, Release);
    }
}

impl<'a, E, const ENTRIES_PER_BLOCK: usize> StealerBlockIter<'a, E, ENTRIES_PER_BLOCK> {
    pub fn len(&self) -> usize {
        self.block_reserved - self.i
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a, E, const ENTRIES_PER_BLOCK: usize> core::fmt::Debug
    for StealerBlockIter<'a, E, ENTRIES_PER_BLOCK>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "StealerBlockIter over {} entries",
            self.block_reserved - self.i
        ))
    }
}

/// Create a new BWoS queue and return the [Owner] and a [Stealer] instance
///
/// `NUM_BLOCKS` must be a power two and at least 2. `ENTRIES_PER_BLOCK` can be freely chosen (non-zero).
/// The total length of the queue is `NUM_BLOCKS * ENTRIES_PER_BLOCK` and must not be more than `usize::MAX`.
///
/// ## Performance considerations
///
/// The Owner throughput will improve with a larger `ENTRIES_PER_BLOCK` value.
/// Thieves however will prefer a higher `NUM_BLOCKS` count since it makes it easier to
/// steal a whole block.
pub fn new<E, const NUM_BLOCKS: usize, const ENTRIES_PER_BLOCK: usize>() -> (
    Owner<E, { ENTRIES_PER_BLOCK }>,
    Stealer<E, { ENTRIES_PER_BLOCK }>,
) {
    assert!(NUM_BLOCKS.checked_mul(ENTRIES_PER_BLOCK).is_some());
    assert!(NUM_BLOCKS.is_power_of_two());
    assert!(NUM_BLOCKS >= 1);
    assert!(ENTRIES_PER_BLOCK >= 1);

    let q: Pin<Arc<BwsQueue<E, ENTRIES_PER_BLOCK>>> = BwsQueue::new(NUM_BLOCKS);
    let first_block = &q.blocks[0];

    let stealer_position = Arc::new(AtomicUsize::new(0));

    (
        Owner {
            pcache: CachePadded::new(first_block),
            ccache: CachePadded::new(first_block),
            spos: CachePadded::new(stealer_position.clone()),
            queue: q.clone(),
        },
        Stealer {
            spos: CachePadded::new(stealer_position),
            queue: q,
        },
    )
}
