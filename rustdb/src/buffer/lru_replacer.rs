use crate::buffer::{FrameId, KeyRef, KeyWrapper};

use std::collections::HashMap;
use std::hash::Hash;
use std::ptr::NonNull;
use std::{mem, ptr};

struct LruEntry<K> {
    key: mem::MaybeUninit<K>,
    prev: *mut LruEntry<K>,
    next: *mut LruEntry<K>,
}

impl<K> LruEntry<K> {
    fn new(key: K) -> LruEntry<K> {
        LruEntry {
            key: mem::MaybeUninit::new(key),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }

    fn new_sigil() -> LruEntry<K> {
        LruEntry {
            key: mem::MaybeUninit::uninit(),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

pub struct LruReplacer<K> {
    map: HashMap<KeyRef<K>, NonNull<LruEntry<K>>>,
    cap: usize,
    head: *mut LruEntry<K>,
    tail: *mut LruEntry<K>,
}

impl<K> LruReplacer<K>
where
    K: Hash,
{
    fn detach(&mut self, node: *mut LruEntry<K>) {
        unsafe {
            (*(*node).prev).next = (*node).next;
            (*(*node).next).prev = (*node).prev;
        }
    }

    fn attach(&mut self, node: *mut LruEntry<K>) {
        unsafe {
            (*node).next = (*self.head).next;
            (*node).prev = self.head;
            (*(*self.head).next).prev = node;
            (*self.head).next = node;
        }
    }
}

impl LruReplacer<FrameId> {
    pub fn new(cap: usize) -> Self {
        let replacer = LruReplacer {
            map: HashMap::new(),
            cap,
            head: Box::into_raw(Box::new(LruEntry::new_sigil())),
            tail: Box::into_raw(Box::new(LruEntry::new_sigil())),
        };
        unsafe {
            (*replacer.head).next = replacer.tail;
            (*replacer.tail).prev = replacer.head;
        }
        replacer
    }
    fn remove_last(&mut self) -> Option<LruEntry<FrameId>> {
        let prev = unsafe { (*self.tail).prev };
        if prev != self.head {
            let old_key = unsafe {
                KeyRef {
                    k: (*(*self.tail).prev).key.as_ptr(),
                }
            };
            let old_node = self.map.remove(&old_key).unwrap();
            let node_ptr = old_node.as_ptr();
            self.detach(node_ptr);
            return Some(unsafe { *Box::from_raw(node_ptr) });
        }

        None
    }
}

impl LruReplacer<FrameId> {
    fn victim(&mut self) -> Option<FrameId> {
        self.remove_last()
            .map(|node| unsafe { node.key.assume_init() })
    }

    fn pin(&mut self, frame_id: FrameId) {
        if let Some(node) = self.map.remove(KeyWrapper::from_ref(&frame_id)) {
            self.detach(node.as_ptr());
        }
    }

    fn unpin(&mut self, frame_id: FrameId) {
        if self.map.len() >= self.cap {
            return;
        }
        if !self.map.contains_key(KeyWrapper::from_ref(&frame_id)) {
            let node =
                unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(LruEntry::new(frame_id)))) };
            let keyref = unsafe { (*node.as_ptr()).key.as_ptr() };
            self.map.insert(KeyRef { k: keyref }, node);
            self.attach(node.as_ptr());
        }
    }

    fn size(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_replacer() {
        let mut lru_replacer = LruReplacer::new(7);

        // Scenario: unpin six elements, i.e. add them to the replacer.
        lru_replacer.unpin(1);
        lru_replacer.unpin(2);
        lru_replacer.unpin(3);
        lru_replacer.unpin(4);
        lru_replacer.unpin(5);
        lru_replacer.unpin(6);
        lru_replacer.unpin(1);
        assert_eq!(6, lru_replacer.size());

        // Scenario: get three victims from the lru.
        assert_eq!(1, lru_replacer.victim().unwrap());
        assert_eq!(2, lru_replacer.victim().unwrap());
        assert_eq!(3, lru_replacer.victim().unwrap());

        // Scenario: pin elements in the replacer.
        // Note that 3 has already been victimized, so pinning 3 should have no effect.
        lru_replacer.pin(3);
        lru_replacer.pin(4);
        assert_eq!(2, lru_replacer.size());

        // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
        lru_replacer.unpin(4);

        // Scenario: continue looking for victims. We expect these victims.
        assert_eq!(5, lru_replacer.victim().unwrap());
        assert_eq!(6, lru_replacer.victim().unwrap());
        assert_eq!(4, lru_replacer.victim().unwrap());
    }
}
