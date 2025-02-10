use std::{collections::HashMap, vec};

use super::page::{FRAME_SIZE, PageID};

#[derive(Debug)]
pub struct PageDirector {
    // HashMap<PageID, usize>
    // @ PageID     : page id
    // @ usize      : offset
    // an indirection with offset from directory
    // to the page on disc
    map: HashMap<PageID, usize>,
    free_slots: Vec<usize>,
    highest_page_id: PageID,
}

impl PageDirector {
    pub fn new() -> PageDirector {
        PageDirector {
            map: HashMap::with_capacity(10),
            free_slots: vec![],
            highest_page_id: 0,
        }
    }

    pub fn empty(&mut self) -> bool {
        self.free_slots.is_empty()
    }

    pub fn can_accomodate(&self) -> bool {
        !self.free_slots.is_empty()
    }

    pub fn current_mapsize(&self) -> usize {
        self.map.len() + self.free_slots.len()
    }

    pub fn register_new_page(&mut self) -> (PageID, usize) {
        if self.free_slots.is_empty() && self.map.is_empty() {
            // no page exists in the page directory
            self.highest_page_id += 1;
            self.map.insert(self.highest_page_id, 0);
            return (self.highest_page_id, 0);
        }

        if !self.map.is_empty() && !self.free_slots.is_empty() {
            self.highest_page_id += 1;
            let offset = self
                .free_slots
                .pop()
                .expect("failed to pop from free slots vec");
            self.map.insert(self.highest_page_id, offset);

            return (self.highest_page_id, offset);
        }

        if !self.map.is_empty() && self.free_slots.is_empty() {
            let map_clone = self.map.clone();
            let max_element = map_clone.iter().max_by_key(|entry| entry.1).unwrap();
            self.highest_page_id += 1;
            self.map
                .insert(self.highest_page_id, max_element.1 + FRAME_SIZE as usize);
            return (
                self.highest_page_id,
                max_element.1.to_owned() + FRAME_SIZE as usize,
            );
        }

        (u32::MAX, 0) // fail condition for now
    }
}

impl Default for PageDirector {
    fn default() -> Self {
        PageDirector::new()
    }
}
