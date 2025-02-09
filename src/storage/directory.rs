use std::collections::HashMap;

use super::page::{Frame, PageID};

pub struct PageDirector {
    // HashMap<PageID, usize>
    // @ PageID     : page id
    // @ usize      : offset
    // an indirection with offset from directory
    // to the page on disc
    map: HashMap<PageID, usize>,

    free_slots: Vec<usize>,
}

impl PageDirector {
    pub fn new() -> PageDirector {
        PageDirector {
            map: HashMap::with_capacity(10),
            free_slots: vec![],
        }
    }

    pub fn empty(&mut self) -> bool {
        self.free_slots.is_empty()
    }
}

impl Default for PageDirector {
    fn default() -> Self {
        PageDirector::new()
    }
}
