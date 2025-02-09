// this file defines the structure of a db file
// representing the overall page directory for
// recovery and also page structure
//
// each individual page is supposed to be self
// contained

pub type PageID = u32;
pub const FRAME_SIZE: u64 = 4096; // 4KB frame size

// pages goes synonymously with frames, frames being
// 4KB block of memory that will be pointed to in the
// LRU cache
pub struct Frame {
    pub page_id: u8,
    pub dirty: bool,
    pub content: Box<Vec<u8>>,
}

impl Frame {}
