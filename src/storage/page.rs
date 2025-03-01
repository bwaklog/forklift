// this file defines the structure of a db file
// representing the overall page directory for
// recovery and also page structure
//
// each individual page is supposed to be self
// contained

use std::{fmt::Display, io, sync::Arc};

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
    pub content: Box<[u8; FRAME_SIZE as usize]>,
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
    pub fn new(page_id: PageID, offset: usize, content: Box<[u8; FRAME_SIZE as usize]>) -> Frame {
        Frame {
            page_id,
            offset,
            content,
            cursor: 0,
            dirty: false,
        }
    }
}

impl io::Read for Frame {
    // not completely implemented
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut buf_len = buf.len();
        let read_available = FRAME_SIZE as usize - self.cursor;
        if buf_len > read_available {
            buf_len = read_available;
        }
        buf.copy_from_slice(&self.content.as_slice()[self.cursor..self.cursor + buf_len]);
        Ok(buf_len)
    }
}

impl io::Write for Frame {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.dirty = true;

        Ok(0)
    }

    fn flush(&mut self) -> io::Result<()> {
        // somehow request the diskmanager to flush the page
        Ok(())
    }
}
