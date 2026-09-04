//! Aggressive Memory Pooling for Edge/IoT Deployments
//!
//! Provides bump-allocated memory pools designed for minimal per-actor overhead.
//! Target: <8KB idle footprint per actor.
//!
//! # ⚠️ UNSAFE CODE — the only `unsafe` in this crate
//!
//! This module contains hand-rolled arena allocation, raw-pointer free lists,
//! and manual `Send`/`Sync` implementations. It is compiled **only** when the
//! `unsafe-pool` feature is enabled; the crate otherwise carries
//! `#![deny(unsafe_code)]`. Every `unsafe` block below carries a SAFETY
//! comment explaining its invariant. Review this module before enabling the
//! feature — correctness depends on the following invariants:
//!
//! 1. `BumpAllocatorInner` is only ever accessed through `Mutex`, so arena
//!    bump-pointer mutation and freed-list mutation are serialized.
//! 2. `MemoryBlock` is an RAII guard: the pool (`BumpAllocator`) **must**
//!    outlive every block it issued. Dropping a block pushes its pointer onto
//!    the pool's freed list; a dangling pool pointer would be UB.
//! 3. Blocks are never freed twice (each block is dropped exactly once), and
//!    `reset()` must only be called when no outstanding blocks exist.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────┐
//! │           BumpAllocator             │
//! │  ┌──────────────────────────────┐   │
//! │  │      Fixed-size Arena       │   │
//! │  │  [block][block][block]...    │   │
//! │  └──────────────────────────────┘   │
//! │  bump_ptr ──► next free offset      │
//! │  freed_list ──► returned blocks     │
//! └──────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use actor_kit::memory_pool::{BumpAllocator, MemoryPoolConfig};
//!
//! let config = MemoryPoolConfig::default();
//! let pool = BumpAllocator::new(config);
//!
//! {
//!     let block = pool.borrow(256).expect("pool has capacity");
//!     // use block.data() ...
//! } // block dropped, memory returned to pool
//! ```

// Crate-level policy is `#![deny(unsafe_code)]`; this module is the single,
// loudly documented exception and only exists behind the `unsafe-pool`
// feature. See the module docs for the safety invariants.
#![allow(unsafe_code)]
#![allow(missing_docs)]

use parking_lot::Mutex;
use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_ARENA_SIZE: usize = 8 * 1024;
const DEFAULT_PAGE_SIZE: usize = 64;
const DEFAULT_MAX_POOLS: usize = 1024;
const MIN_BLOCK_SIZE: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MemoryPoolConfig {
    pub arena_size_bytes: usize,
    pub page_size: usize,
    pub max_pools: usize,
    pub preallocate: bool,
}

impl Default for MemoryPoolConfig {
    fn default() -> Self {
        Self {
            arena_size_bytes: DEFAULT_ARENA_SIZE,
            page_size: DEFAULT_PAGE_SIZE,
            max_pools: DEFAULT_MAX_POOLS,
            preallocate: true,
        }
    }
}

impl MemoryPoolConfig {
    pub fn arena_size_bytes(mut self, bytes: usize) -> Self {
        self.arena_size_bytes = bytes;
        self
    }

    pub fn page_size(mut self, size: usize) -> Self {
        self.page_size = size.max(MIN_BLOCK_SIZE);
        self
    }

    pub fn max_pools(mut self, max: usize) -> Self {
        self.max_pools = max;
        self
    }

    pub fn preallocate(mut self, yes: bool) -> Self {
        self.preallocate = yes;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PoolStats {
    pub total_allocated: u64,
    pub total_freed: u64,
    pub active_pools: u64,
    pub fragmentation_ratio: f64,
}

struct FreeBlock {
    size: usize,
    next: *mut FreeBlock,
}

pub struct MemoryBlock {
    ptr: NonNull<u8>,
    size: usize,
    pool: *const BumpAllocatorInner,
}

impl MemoryBlock {
    pub fn data(&self) -> &[u8] {
        unsafe {
            let len = self.size;
            if len == 0 {
                return &[];
            }
            // SAFETY: ptr was allocated with the given size by the pool allocator
            std::slice::from_raw_parts(self.ptr.as_ptr(), len)
        }
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        unsafe {
            let len = self.size;
            if len == 0 {
                return &mut [];
            }
            // SAFETY: ptr was allocated with the given size by the pool allocator
            std::slice::from_raw_parts_mut(self.ptr.as_ptr(), len)
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

impl Drop for MemoryBlock {
    fn drop(&mut self) {
        if self.size == 0 {
            return;
        }
        // SAFETY: pool pointer is always valid as long as the BumpAllocator exists.
        // MemoryBlock is an RAII guard; the pool outlives all blocks it issued.
        unsafe {
            let inner = &*self.pool;
            let block_ptr = self.ptr.as_ptr() as *mut FreeBlock;
            // SAFETY: block_ptr points to a valid allocation of at least self.size bytes
            (*block_ptr).size = self.size;
            let mut freed_list = inner.freed_list.lock();
            (*block_ptr).next = *freed_list;
            *freed_list = block_ptr;
            inner
                .total_freed
                .fetch_add(self.size as u64, Ordering::Relaxed);
        }
    }
}

// SAFETY: MemoryBlock contains only a raw pointer to pool memory and a size.
// Access is synchronized through the pool's internal mutex for the freed list.
unsafe impl Send for MemoryBlock {}
unsafe impl Sync for MemoryBlock {}

// SAFETY: BumpAllocatorInner is only accessed through a Mutex<BumpAllocatorInner>,
// so synchronized access is guaranteed. The raw pointers (arena, freed_list)
// point to memory that is either from the global allocator (thread-safe) or
// within the arena (protected by the mutex).
unsafe impl Send for BumpAllocatorInner {}
unsafe impl Sync for BumpAllocatorInner {}

struct BumpAllocatorInner {
    arena: Option<NonNull<u8>>,
    arena_size: usize,
    bump_offset: usize,
    freed_list: Mutex<*mut FreeBlock>,
    total_allocated: AtomicU64,
    total_freed: AtomicU64,
    active_pools: AtomicU64,
    _config: MemoryPoolConfig,
}

impl BumpAllocatorInner {
    fn new(config: MemoryPoolConfig) -> Self {
        if config.preallocate {
            let size = config.arena_size_bytes;
            let Ok(layout) = Layout::from_size_align(size, 8) else {
                return Self::no_arena();
            };
            // SAFETY: layout size > 0 and alignment is a power of two
            let ptr = unsafe { std::alloc::alloc(layout) };
            match NonNull::new(ptr) {
                Some(p) => Self {
                    arena: Some(p),
                    arena_size: size,
                    bump_offset: 0,
                    freed_list: Mutex::new(std::ptr::null_mut()),
                    total_allocated: AtomicU64::new(0),
                    total_freed: AtomicU64::new(0),
                    active_pools: AtomicU64::new(1),
                    _config: config,
                },
                None => Self::no_arena(),
            }
        } else {
            Self::no_arena()
        }
    }

    fn no_arena() -> Self {
        Self {
            arena: None,
            arena_size: 0,
            bump_offset: 0,
            freed_list: Mutex::new(std::ptr::null_mut()),
            total_allocated: AtomicU64::new(0),
            total_freed: AtomicU64::new(0),
            active_pools: AtomicU64::new(1),
            _config: MemoryPoolConfig::default(),
        }
    }

    fn try_borrow_from_freed_list(&self, size: usize) -> Option<NonNull<u8>> {
        let mut freed_list = self.freed_list.lock();
        let mut current = *freed_list;
        let mut prev: *mut FreeBlock = std::ptr::null_mut();

        while !current.is_null() {
            // SAFETY: current points to a valid FreeBlock previously allocated by this pool
            let block = unsafe { &mut *current };
            if block.size >= size {
                if prev.is_null() {
                    *freed_list = block.next;
                } else {
                    // SAFETY: prev points to a valid FreeBlock in the same list
                    unsafe { (*prev).next = block.next };
                }
                let ptr = current as *mut u8;
                // SAFETY: ptr was derived from `current` which points to a valid FreeBlock in this pool's freed list
                return Some(unsafe { NonNull::new_unchecked(ptr) });
            }
            prev = current;
            current = block.next;
        }
        None
    }

    fn try_bump_allocate(&mut self, size: usize) -> Option<NonNull<u8>> {
        let arena_ptr = self.arena?;
        let aligned_size = (size + 7) & !7; // align to 8 bytes
        let new_offset = self.bump_offset.checked_add(aligned_size)?;
        if new_offset > self.arena_size {
            return None;
        }
        // SAFETY: arena_ptr points to a valid allocation of arena_size bytes,
        // and bump_offset + aligned_size <= arena_size
        let ptr = unsafe { arena_ptr.as_ptr().add(self.bump_offset) };
        self.bump_offset = new_offset;
        // SAFETY: ptr was computed from arena_ptr.as_ptr().add(bump_offset) which is within the valid arena allocation
        Some(unsafe { NonNull::new_unchecked(ptr) })
    }

    fn fragmentation_ratio(&self) -> f64 {
        let allocated = self.total_allocated.load(Ordering::Relaxed);
        let freed = self.total_freed.load(Ordering::Relaxed);
        if allocated == 0 {
            return 0.0;
        }
        freed as f64 / allocated as f64
    }
}

impl Drop for BumpAllocatorInner {
    fn drop(&mut self) {
        if let Some(arena) = self.arena.take() {
            let layout = match Layout::from_size_align(self.arena_size, 8) {
                Ok(l) => l,
                Err(_) => return,
            };
            // SAFETY: arena was allocated with this layout in `new`
            unsafe {
                std::alloc::dealloc(arena.as_ptr(), layout);
            }
        }
    }
}

pub struct BumpAllocator {
    inner: Mutex<BumpAllocatorInner>,
}

impl BumpAllocator {
    pub fn new(config: MemoryPoolConfig) -> Self {
        Self {
            inner: Mutex::new(BumpAllocatorInner::new(config)),
        }
    }

    pub fn borrow(&self, size: usize) -> Option<MemoryBlock> {
        if size == 0 {
            return Some(MemoryBlock {
                ptr: NonNull::dangling(),
                size: 0,
                pool: std::ptr::null(),
            });
        }

        let adjusted_size = size.max(MIN_BLOCK_SIZE);
        let mut inner = self.inner.lock();

        let ptr = inner
            .try_borrow_from_freed_list(adjusted_size)
            .or_else(|| inner.try_bump_allocate(adjusted_size))?;

        inner
            .total_allocated
            .fetch_add(adjusted_size as u64, Ordering::Relaxed);

        Some(MemoryBlock {
            ptr,
            size: adjusted_size,
            pool: &*inner as *const BumpAllocatorInner,
        })
    }

    pub fn stats(&self) -> PoolStats {
        let inner = self.inner.lock();
        PoolStats {
            total_allocated: inner.total_allocated.load(Ordering::Relaxed),
            total_freed: inner.total_freed.load(Ordering::Relaxed),
            active_pools: inner.active_pools.load(Ordering::Relaxed),
            fragmentation_ratio: inner.fragmentation_ratio(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.lock().arena_size
    }

    pub fn available(&self) -> usize {
        let inner = self.inner.lock();
        inner.arena_size.saturating_sub(inner.bump_offset)
    }

    pub fn reset(&self) {
        let mut inner = self.inner.lock();
        inner.bump_offset = 0;
        *inner.freed_list.lock() = std::ptr::null_mut();
        inner.total_allocated.store(0, Ordering::Relaxed);
        inner.total_freed.store(0, Ordering::Relaxed);
    }
}

impl Default for BumpAllocator {
    fn default() -> Self {
        Self::new(MemoryPoolConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MemoryPoolConfig::default();
        assert_eq!(config.arena_size_bytes, DEFAULT_ARENA_SIZE);
        assert_eq!(config.page_size, DEFAULT_PAGE_SIZE);
        assert_eq!(config.max_pools, DEFAULT_MAX_POOLS);
        assert!(config.preallocate);
    }

    #[test]
    fn test_borrow_returns_block() {
        let pool = BumpAllocator::new(MemoryPoolConfig::default());
        let block = pool.borrow(64);
        assert!(block.is_some());
        let block = block.unwrap();
        assert_eq!(block.size(), 64);
        assert_eq!(block.data().len(), 64);
    }

    #[test]
    fn test_borrow_zero_size() {
        let pool = BumpAllocator::default();
        let block = pool.borrow(0);
        assert!(block.is_some());
        assert_eq!(block.unwrap().size(), 0);
    }

    #[test]
    fn test_borrow_minimum_block_size() {
        let pool = BumpAllocator::default();
        let block = pool.borrow(1).unwrap();
        assert!(block.size() >= MIN_BLOCK_SIZE);
    }

    #[test]
    fn test_borrow_multiple_blocks() {
        let pool = BumpAllocator::new(MemoryPoolConfig::default().arena_size_bytes(4096));
        let b1 = pool.borrow(64).unwrap();
        let b2 = pool.borrow(128).unwrap();
        let b3 = pool.borrow(32).unwrap();
        assert_eq!(b1.size(), 64);
        assert_eq!(b2.size(), 128);
        assert_eq!(b3.size(), 32);
    }

    #[test]
    fn test_block_data_mutable() {
        let pool = BumpAllocator::default();
        let mut block = pool.borrow(16).unwrap();
        block
            .data_mut()
            .copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        assert_eq!(
            block.data(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
    }

    #[test]
    fn test_drop_returns_memory() {
        let pool = BumpAllocator::new(MemoryPoolConfig::default().arena_size_bytes(256));
        {
            let _block = pool.borrow(64).unwrap();
        }
        let stats = pool.stats();
        assert_eq!(stats.total_allocated, 64);
        assert_eq!(stats.total_freed, 64);
    }

    #[test]
    fn test_freed_blocks_reused() {
        let pool = BumpAllocator::new(MemoryPoolConfig::default().arena_size_bytes(256));
        let ptr1 = {
            let block = pool.borrow(64).unwrap();
            block.ptr
        };
        let block2 = pool.borrow(64).unwrap();
        assert_eq!(ptr1, block2.ptr, "freed block should be reused");
    }

    #[test]
    fn test_pool_exhaustion_returns_none() {
        let pool = BumpAllocator::new(
            MemoryPoolConfig::default()
                .arena_size_bytes(128)
                .preallocate(true),
        );
        let _b1 = pool.borrow(64).unwrap();
        let _b2 = pool.borrow(64).unwrap();
        let b3 = pool.borrow(64);
        assert!(b3.is_none(), "pool should be exhausted");
    }

    #[test]
    fn test_arena_full_returns_none() {
        let pool = BumpAllocator::new(
            MemoryPoolConfig::default()
                .arena_size_bytes(64)
                .preallocate(true),
        );
        let _b1 = pool.borrow(32).unwrap();
        let _b2 = pool.borrow(32).unwrap();
        let b3 = pool.borrow(64);
        assert!(b3.is_none());
    }

    #[test]
    fn test_no_arena_mode_returns_none() {
        let config = MemoryPoolConfig::default().preallocate(false);
        let pool = BumpAllocator::new(config);
        assert_eq!(pool.capacity(), 0);
        let block = pool.borrow(64);
        assert!(block.is_none());
    }

    #[test]
    fn test_stats_after_operations() {
        let pool = BumpAllocator::new(MemoryPoolConfig::default().arena_size_bytes(512));
        let stats_initial = pool.stats();
        assert_eq!(stats_initial.total_allocated, 0);
        assert_eq!(stats_initial.total_freed, 0);

        let _b1 = pool.borrow(64).unwrap();
        let _b2 = pool.borrow(128).unwrap();
        let stats_after = pool.stats();
        assert_eq!(stats_after.total_allocated, 192);
        assert_eq!(stats_after.active_pools, 1);
    }

    #[test]
    fn test_fragmentation_ratio() {
        let pool = BumpAllocator::new(MemoryPoolConfig::default().arena_size_bytes(1024));
        let _b1 = pool.borrow(128).unwrap();
        let _b2 = pool.borrow(128).unwrap();
        drop(_b1);
        let stats = pool.stats();
        assert!((stats.fragmentation_ratio - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_reset_clears_pool() {
        let pool = BumpAllocator::new(MemoryPoolConfig::default().arena_size_bytes(256));
        let _b1 = pool.borrow(64).unwrap();
        let _b2 = pool.borrow(64).unwrap();
        drop(_b1);

        pool.reset();
        let stats = pool.stats();
        assert_eq!(stats.total_allocated, 0);
        assert_eq!(stats.total_freed, 0);

        let b3 = pool.borrow(64);
        assert!(b3.is_some());
    }

    #[test]
    fn test_capacity_and_available() {
        let pool = BumpAllocator::new(MemoryPoolConfig::default().arena_size_bytes(1024));
        assert_eq!(pool.capacity(), 1024);
        assert_eq!(pool.available(), 1024);

        let _b = pool.borrow(256).unwrap();
        assert!(pool.available() < 1024);
    }

    #[test]
    fn test_builder_pattern_config() {
        let config = MemoryPoolConfig::default()
            .arena_size_bytes(4096)
            .page_size(128)
            .max_pools(512)
            .preallocate(false);
        assert_eq!(config.arena_size_bytes, 4096);
        assert_eq!(config.page_size, 128);
        assert_eq!(config.max_pools, 512);
        assert!(!config.preallocate);
    }

    #[test]
    fn test_no_preallocate_mode() {
        let config = MemoryPoolConfig::default().preallocate(false);
        let pool = BumpAllocator::new(config);
        assert_eq!(pool.capacity(), 0);
        assert_eq!(pool.available(), 0);
        let block = pool.borrow(64);
        assert!(block.is_none());
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let pool = Arc::new(BumpAllocator::new(
            MemoryPoolConfig::default().arena_size_bytes(64 * 1024),
        ));
        let mut handles = vec![];

        for _ in 0..4 {
            let p = Arc::clone(&pool);
            handles.push(thread::spawn(move || {
                let mut blocks = vec![];
                for _ in 0..100 {
                    if let Some(b) = p.borrow(32) {
                        blocks.push(b);
                    }
                }
                drop(blocks);
            }));
        }

        for h in handles {
            h.join().expect("thread should not panic");
        }

        let stats = pool.stats();
        assert_eq!(stats.total_freed, stats.total_allocated);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_pool_stats_serialization() {
        let stats = PoolStats {
            total_allocated: 1024,
            total_freed: 512,
            active_pools: 1,
            fragmentation_ratio: 0.5,
        };
        let json = serde_json::to_string(&stats).expect("serialization should succeed");
        let deserialized: PoolStats =
            serde_json::from_str(&json).expect("deserialization should succeed");
        assert_eq!(deserialized.total_allocated, 1024);
        assert_eq!(deserialized.total_freed, 512);
    }
}
