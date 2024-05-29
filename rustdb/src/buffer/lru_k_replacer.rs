use crate::buffer::{Error, FrameId, KeyRef, KeyWrapper};

use std::collections::HashMap;

use std::ptr::NonNull;
use std::{mem, ptr};

use std::sync::atomic::{AtomicUsize, Ordering};

struct LruEntry {
    frame_id: mem::MaybeUninit<FrameId>,
    is_evictable: bool,
    access_count: usize,
    prev: *mut LruEntry,
    next: *mut LruEntry,
}

impl LruEntry {
    pub fn new(frame_id: FrameId) -> LruEntry {
        LruEntry {
            frame_id: mem::MaybeUninit::new(frame_id),
            is_evictable: true,
            access_count: 1,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
    pub fn new_sigil() -> LruEntry {
        LruEntry {
            frame_id: mem::MaybeUninit::uninit(),
            is_evictable: true,
            access_count: 1,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

pub struct LruKReplacer {
    node_store: HashMap<KeyRef<FrameId>, NonNull<LruEntry>>,
    history_list_head: *mut LruEntry,
    history_list_tail: *mut LruEntry,
    cache_list_head: *mut LruEntry,
    cache_list_tail: *mut LruEntry,
    current_size: AtomicUsize,
    replacer_size: usize,
    k: usize,
}
unsafe impl Send for LruKReplacer {}
unsafe impl Sync for LruKReplacer {}

impl LruKReplacer {
    pub fn new(cap: usize, k: usize) -> Self {
        let replacer = Self {
            node_store: HashMap::new(),
            history_list_head: Box::into_raw(Box::new(LruEntry::new_sigil())),
            history_list_tail: Box::into_raw(Box::new(LruEntry::new_sigil())),
            cache_list_head: Box::into_raw(Box::new(LruEntry::new_sigil())),
            cache_list_tail: Box::into_raw(Box::new(LruEntry::new_sigil())),
            current_size: AtomicUsize::new(0),
            replacer_size: cap,
            k,
        };
        unsafe {
            (*replacer.history_list_head).next = replacer.history_list_tail;
            (*replacer.history_list_tail).prev = replacer.history_list_head;
            (*replacer.cache_list_head).next = replacer.cache_list_tail;
            (*replacer.cache_list_tail).prev = replacer.cache_list_head;
        }

        replacer
    }
    pub fn evict(&mut self) -> Option<FrameId> {
        if self.current_size.load(Ordering::SeqCst) == 0 {
            return None;
        }
        let mut node = unsafe { (*self.history_list_tail).prev };
        while node != self.history_list_head {
            let old_key = unsafe {
                KeyRef {
                    k: (*node).frame_id.as_ptr(),
                }
            };
            if unsafe { (*node).is_evictable } {
                Self::detach(node);
                let old_node = self.node_store.remove(&old_key).unwrap();
                self.current_size.fetch_sub(1, Ordering::SeqCst);
                return Some(unsafe { Box::from_raw(old_node.as_ptr()).frame_id.assume_init() });
            }
            unsafe { node = (*node).prev };
        }
        let mut node = unsafe { (*self.cache_list_tail).prev };
        while node != self.cache_list_head {
            let old_key = unsafe {
                KeyRef {
                    k: (*node).frame_id.as_ptr(),
                }
            };
            if unsafe { (*node).is_evictable } {
                Self::detach(node);
                let old_node = self.node_store.remove(&old_key).unwrap();
                self.current_size.fetch_sub(1, Ordering::SeqCst);
                return Some(unsafe { Box::from_raw(old_node.as_ptr()).frame_id.assume_init() });
            }
            unsafe { node = (*node).prev };
        }
        None
    }

    pub fn record_access(&mut self, frame_id: FrameId) {
        assert!(frame_id.lt(&(self.replacer_size)));
        if let Some(node) = self.node_store.get(KeyWrapper::from_ref(&frame_id)) {
            let node_ptr = node.as_ptr();
            unsafe {
                if (*node_ptr).access_count >= self.k - 1 {
                    Self::detach(node_ptr);
                    Self::attach(self.cache_list_head, node_ptr);
                    (*node_ptr).access_count += 1;
                }
            }
        } else {
            let node =
                unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(LruEntry::new(frame_id)))) };

            let node_ptr = node.as_ptr();
            Self::attach(self.history_list_head, node_ptr);
            let keyref = unsafe { (*node.as_ptr()).frame_id.as_ptr() };
            self.node_store.insert(KeyRef { k: keyref }, node);
            self.current_size.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        assert!(frame_id.lt(&(self.replacer_size)));
        if let Some(node) = self.node_store.get(KeyWrapper::from_ref(&frame_id)) {
            let node_ptr = node.as_ptr();
            unsafe {
                if (*node_ptr).is_evictable != evictable {
                    (*node_ptr).is_evictable = evictable;
                    if evictable {
                        self.current_size.fetch_add(1, Ordering::SeqCst);
                    } else {
                        self.current_size.fetch_sub(1, Ordering::SeqCst);
                    }
                }
            }
        }
    }

    pub fn size(&self) -> usize {
        self.current_size.load(Ordering::SeqCst)
    }

    pub fn remove(&mut self, frame_id: FrameId) -> Result<(), Error> {
        assert!(frame_id.lt(&(self.replacer_size)));
        if let Some(node) = self.node_store.get(KeyWrapper::from_ref(&frame_id)) {
            let node_ptr = node.as_ptr();
            if unsafe { (*node_ptr).is_evictable } {
                return Err(Error::UnEvictableFrame(frame_id));
            }
            Self::detach(node_ptr);
            self.node_store.remove(KeyWrapper::from_ref(&frame_id));
        }
        Ok(())
    }

    fn detach(node: *mut LruEntry) {
        unsafe {
            (*(*node).prev).next = (*node).next;
            (*(*node).next).prev = (*node).prev;
        }
    }

    fn attach(head: *mut LruEntry, node: *mut LruEntry) {
        unsafe {
            (*node).next = (*head).next;
            (*node).prev = head;
            (*(*head).next).prev = node;
            (*head).next = node;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lru_k_replacer() {
        let mut lru_replacer = LruKReplacer::new(7, 2);

        // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
        lru_replacer.record_access(1);
        lru_replacer.record_access(2);
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(6);
        lru_replacer.set_evictable(1, true);
        lru_replacer.set_evictable(2, true);
        lru_replacer.set_evictable(3, true);
        lru_replacer.set_evictable(4, true);
        lru_replacer.set_evictable(5, true);
        lru_replacer.set_evictable(6, false);
        assert_eq!(5, lru_replacer.size());

        // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
        // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
        lru_replacer.record_access(1);

        // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
        // first based on LRU.

        assert_eq!(2, lru_replacer.evict().unwrap());
        assert_eq!(3, lru_replacer.evict().unwrap());
        assert_eq!(4, lru_replacer.evict().unwrap());
        assert_eq!(2, lru_replacer.size());

        // Scenario: Now replacer has frames [5,1].
        // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(4);
        lru_replacer.set_evictable(3, true);
        lru_replacer.set_evictable(4, true);
        assert_eq!(4, lru_replacer.size());

        // Scenario: continue looking for victims. We expect 3 to be evicted next.
        assert_eq!(3, lru_replacer.evict().unwrap());
        assert_eq!(3, lru_replacer.size());

        // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
        lru_replacer.set_evictable(6, true);
        assert_eq!(4, lru_replacer.size());
        assert_eq!(6, lru_replacer.evict().unwrap());
        assert_eq!(3, lru_replacer.size());

        // Now we have [1,5,4]. Continue looking for victims.
        lru_replacer.set_evictable(1, false);
        assert_eq!(2, lru_replacer.size());
        assert_eq!(5, lru_replacer.evict().unwrap());
        assert_eq!(1, lru_replacer.size());

        // Update access history for 1. Now we have [4,1]. Next victim is 4.
        lru_replacer.record_access(1);
        lru_replacer.record_access(1);
        lru_replacer.set_evictable(1, true);
        assert_eq!(2, lru_replacer.size());
        assert_eq!(4, lru_replacer.evict().unwrap());

        assert_eq!(1, lru_replacer.size());
        assert_eq!(1, lru_replacer.evict().unwrap());
        assert_eq!(0, lru_replacer.size());

        // This operation should not modify size
        assert!(lru_replacer.evict().is_none());
        assert_eq!(0, lru_replacer.size());
    }
}
