// this file defines the structure of a db file
// representing the overall page directory for
// recovery and also page structure
//
// each individual page is supposed to be self
// contained

use std::{fmt::Display, io::Read, sync::Arc};

pub type PageID = u32;
pub const FRAME_SIZE: u64 = 4096; // 4KB frame size

// pages goes synonymously with frames, frames being
// 4KB block of memory that will be pointed to in the
// LRU cache
#[allow(unused)]
#[derive(Debug)]
pub struct Frame {
    pub page_id: PageID,
    pub dirty: bool,
    pub offset: usize,
    pub cursor: usize,
    pub content: Arc<[u8; FRAME_SIZE as usize]>,
}

impl Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "page_id_t {} dirty {} offset {}",
            self.page_id, self.dirty, self.offset
        )
    }
}

impl Frame {
    pub fn new(page_id: PageID, offset: usize, content: Arc<[u8; FRAME_SIZE as usize]>) -> Frame {
        Frame {
            page_id,
            offset,
            content,
            cursor: 0,
            dirty: false,
        }
    }
}

impl Read for Frame {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        let read_available = FRAME_SIZE as usize - self.cursor;
        buf.copy_from_slice(&self.content.as_slice()[self.cursor..]);

        Ok(read_available)
    }
}
