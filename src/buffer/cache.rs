use std::{
    collections::HashMap,
    fmt::Debug,
    ptr,
    sync::{Arc, RwLock},
};

use crate::storage::page::{Frame, PageID, FRAME_SIZE};

#[derive(Debug, Clone)]
pub struct CacheEntry {
    page_id: PageID,
    frame: Arc<RwLock<Frame>>,
    prev: *mut CacheEntry,
    next: *mut CacheEntry,
}

impl CacheEntry {
    fn new(frame: Frame) -> CacheEntry {
        CacheEntry {
            page_id: frame.page_id,
            frame: Arc::new(RwLock::new(frame)),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

#[derive(Debug)]
pub struct Cache {
    // Using a Heap to implement an
    // LRU-K cache
    pub max_frames: usize,
    // Hashmap and DLL based LRU

    // NOTE: a lookup on this hashmap with the key, the value
    // is a Box pointers, which is later casted to a raw pointer
    // to get its position in the linked list to move it around
    // in the cache
    map: HashMap<PageID, *mut CacheEntry>,
    head: *mut CacheEntry,
    tail: *mut CacheEntry,
}

impl Cache {
    pub fn new(max_frames: usize) -> Cache {
        Cache {
            max_frames,
            map: HashMap::new(),
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
        }
    }

    pub fn lookup_frame(&mut self, page_id: PageID) -> Option<Arc<RwLock<Frame>>> {
        let entry = self.map.get(&page_id);

        if entry.is_none() {
            println!("[DEBUG][CACHE] cache miss");
            return None;
        }

        println!("[DEBUG][CACHE] cache hit");

        let entry_ptr = *(entry.unwrap());

        unsafe {
            let prev = (*entry_ptr).prev;
            let next = (*entry_ptr).next;

            if !prev.is_null() && !next.is_null() {
                (*prev).next = next;
                (*next).prev = prev;

                (*self.head).prev = entry_ptr;
                (*entry_ptr).next = self.head;
                self.head = entry_ptr;
                (*self.head).prev = ptr::null_mut();
            } else if !prev.is_null() && next.is_null() {
                (*prev).next = ptr::null_mut();
                self.tail = prev;

                (*self.head).prev = entry_ptr;
                (*entry_ptr).next = self.head;
                self.head = entry_ptr;
                (*self.head).prev = ptr::null_mut();
            }
        }

        test_iter(self.head);

        unsafe {
            return Some(Arc::clone(&(*entry_ptr).frame));
        }
    }

    pub fn evict_frame(&mut self, page_id: PageID) {
        let entry = self.map.get(&page_id);

        if entry.is_none() {
            println!("[DEBUG][CACHE] cache miss");
            return;
        }

        let entry_ptr = *(entry.unwrap());

        unsafe {
            let prev = (*entry_ptr).prev;
            let next = (*entry_ptr).next;

            if !prev.is_null() && !next.is_null() {
                (*prev).next = next;
                (*next).prev = prev;

                (*self.head).prev = entry_ptr;
                (*entry_ptr).next = self.head;
                self.head = entry_ptr;
                (*self.head).prev = ptr::null_mut();
            } else if !prev.is_null() && next.is_null() {
                (*prev).next = ptr::null_mut();
                self.tail = prev;

                (*self.head).prev = entry_ptr;
                (*entry_ptr).next = self.head;
                self.head = entry_ptr;
                (*self.head).prev = ptr::null_mut();
            }
        }
    }

    /// Adds a frame with specified page_id, memory offset,
    /// content to the cahce
    /// The return value is an Option<Arc<..>> reference to the
    /// frame being evicted
    pub fn put_frame(
        &mut self,
        page_id: PageID,
        offset: usize,
        content: Box<[u8; FRAME_SIZE as usize]>,
    ) -> Option<Arc<RwLock<Frame>>> {
        let mut evict: Option<Arc<RwLock<Frame>>> = None;

        if self.map.len() + 1 > self.max_frames {
            unsafe {
                let entry = self.tail;

                if !entry.is_null() && ((*entry).next == (*entry).prev) {
                    // one element
                    self.head = ptr::null_mut();
                    self.tail = ptr::null_mut();
                } else if !entry.is_null() {
                    self.tail = (*entry).prev;
                    (*self.tail).next = ptr::null_mut();
                }

                let key = (*(entry)).page_id;
                let mut frame = Arc::clone(&(*self.map.remove(&key).unwrap()).frame);
                frame = Arc::clone(&frame);
                evict = Some(frame.clone());
                println!("[DEBUG][CACHE] Evicting frame {}", frame.read().unwrap());
            }
        }

        let entry = Box::new(CacheEntry::new(Frame::new(page_id, offset, content)));

        let entry_ptr = Box::into_raw(entry);

        unsafe {
            if self.tail.is_null() {
                self.tail = entry_ptr;
                self.head = entry_ptr;
            } else {
                (*entry_ptr).next = self.head;
                (*self.head).prev = entry_ptr;
                self.head = entry_ptr;
            }

            self.map.insert(page_id, entry_ptr);
            // dbg!(&self
            //     .map
            //     .values()
            //     .map(|val| (val.frame.page_id, val.prev, val.next))
            //     .collect::<Vec<_>>());
        }

        test_iter(self.head);

        return evict;
    }
}

fn test_iter(head: *mut CacheEntry) {
    unsafe {
        let start = head;
        let mut ptr = start;

        loop {
            if ptr.is_null() {
                break;
            }
            println!(
                "\tnode: {}, prev {:?}, next {:?}",
                (*ptr).page_id,
                (*ptr).prev,
                (*ptr).next
            );
            ptr = (*ptr).next;
        }
    }
}
