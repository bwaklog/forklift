use std::{collections::HashMap, fmt::Display, ptr, sync::Arc};

use crate::storage::page::{Frame, PageID, FRAME_SIZE};

#[derive(Debug, Clone)]
pub struct CacheEntry {
    frame: Arc<Frame>,
    prev: *mut CacheEntry,
    next: *mut CacheEntry,
}

impl CacheEntry {
    fn new(frame: Frame) -> CacheEntry {
        CacheEntry {
            frame: Arc::new(frame),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

#[derive(Debug)]
pub struct Cache {
    // Using a Heap to implement an
    // LRU-K cache
    pub frames: Vec<Arc<Frame>>,
    pub max_frames: usize,
    // Hashmap and DLL based LRU
    map: HashMap<PageID, Box<CacheEntry>>,

    head: *mut CacheEntry,
    tail: *mut CacheEntry,
}

impl Display for Cache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let disp: Vec<(u32, usize, bool)> = self
            .frames
            .iter()
            .map(|frame| (frame.page_id, frame.offset, frame.dirty))
            .collect();
        write!(f, "{:?}", disp)
    }
}

impl Cache {
    pub fn new(max_frames: usize) -> Cache {
        Cache {
            frames: Vec::with_capacity(max_frames),
            max_frames,
            map: HashMap::new(),
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
        }
    }

    pub fn lookup_frame(&mut self, page_id: PageID) -> Option<Arc<Frame>> {
        let entry = self.map.get(&page_id);

        if entry.is_none() {
            return None;
        }

        let entry_ptr = Box::into_raw(Box::clone(&entry.unwrap()));
        let entry_derf = *entry.unwrap().clone();

        unsafe {
            if entry_ptr != self.head || entry_ptr != self.tail {
                (*(*entry_ptr).prev).next = (*entry_ptr).next;
                (*(*entry_ptr).next).prev = (*entry_ptr).prev;

                (*(self.tail)).next = entry_ptr;
                (*entry_ptr).prev = self.tail;
                (*(self.head)).prev = entry_ptr;
                (*entry_ptr).next = self.head;

                self.head = entry_ptr;
            }
        }

        Some(entry_derf.frame)
    }

    pub fn put_frame(
        &mut self,
        page_id: PageID,
        offset: usize,
        content: Arc<[u8; FRAME_SIZE as usize]>,
    ) {
        if self.map.len() + 1 > self.max_frames {
            let key = unsafe { (*(self.tail)).frame.page_id };
            self.map.remove(&key).unwrap();
            let frame: Arc<Frame>;
            unsafe {
                let entry = self.tail;
                frame = Arc::clone(&(*entry).frame);

                let prev = (*(self.tail)).prev;

                if entry == prev {
                    self.head = ptr::null_mut();
                    self.tail = ptr::null_mut();
                } else {
                    (*prev).next = self.head;
                    (*self.head).prev = prev;
                    self.tail = prev;
                }
            }
            println!("evicting frame {}", frame);
        }

        let entry = Box::new(CacheEntry::new(Frame::new(
            page_id,
            offset,
            content.clone(),
        )));
        self.map.insert(page_id, entry.clone());
        unsafe {
            let entry_ptr = Box::into_raw(entry);

            if self.tail.is_null() {
                self.tail = entry_ptr;
                self.head = entry_ptr;

                (*entry_ptr).next = entry_ptr;
                (*entry_ptr).prev = entry_ptr;
            } else {
                (*self.tail).next = entry_ptr;
                (*entry_ptr).prev = self.tail;
                (*self.head).prev = entry_ptr;
                (*entry_ptr).next = self.head;

                self.head = entry_ptr;
            }
        }

        unsafe {
            let start = self.head;
            let mut ptr = start;

            println!("\thead: {}", (*ptr).frame);
            ptr = (*ptr).next;

            loop {
                if ptr == start {
                    break;
                }
                println!("\tnode: {}", (*ptr).frame);
                ptr = (*ptr).next;
            }
        }

        // if self.frames.len() == self.max_frames && !self.frames.is_empty() {
        //     // LRU-K
        //     // we evict a chosen dirty frame to the disc
        //     // to make space for the new frame
        //     let _evict = self.frames.last().unwrap();
        //
        //     let len = self.frames.len();
        //     let frame = Frame::new(page_id, offset, content);
        //     self.frames[len - 1] = Arc::new(frame);
        //
        //     heapify_up(&mut self.frames, len - 1, self.max_frames);
        //     // we evict here
        // } else {
        //     let frame = Frame::new(page_id, offset, content);
        //     self.frames.push(Arc::new(frame));
        //     let len = self.frames.len();
        //
        //     heapify_up(&mut self.frames, len - 1, self.max_frames);
        // }

        // println!("[DEBUG][Cache] {}", self);
    }
}

// fn right_child_index(index: usize, bounds: usize) -> usize {
//     todo!()
// }
//
// fn left_child_index(index: usize, bounds: usize) -> usize {
//     todo!()
// }
//
// fn parent_index(index: usize, bounds: usize) -> usize {
//     if index == 0 {
//         return 0;
//     }
//
//     if index >= bounds {
//         panic!("invalid index in heap")
//     }
//
//     (index - 1) / 2
// }
//
// fn heapify_up(heap: &mut [Arc<Frame>], index: usize, capacity: usize) {
//     let mut cur = index;
//     loop {
//         if cur == 0 {
//             break;
//         }
//
//         let parent = parent_index(cur, capacity);
//         heap.swap(cur, parent);
//         cur = parent;
//     }
// }
//
// fn heapify_down(heap: &mut [Arc<Frame>]) {}
