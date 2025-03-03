use std::{
    fmt,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ptr::write,
    sync::{Arc, Mutex, RwLock},
};

use crate::storage::{
    directory::PageDirector,
    page::{Frame, PageID, FRAME_SIZE},
};

use super::cache::Cache;

#[allow(unused)]
pub struct DiskManager {
    db_file: File,
    status: bool,

    page_directory: PageDirector,
    pub cache: Cache,
}

#[derive(Debug, Clone)]
pub enum Error {
    DeletePageError,
    WritePageError,
    CacheFetchMiss,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::DeletePageError => write!(f, "Failed to perform operations to delete a page"),
            Self::WritePageError => write!(f, "Failed to perform operations to write a page"),
            Self::CacheFetchMiss => write!(f, "Frame flush requested is not in cache"),
        }
    }
}

impl std::error::Error for Error {}

impl DiskManager {
    pub fn new(max_frames: usize, db_file: &str) -> DiskManager {
        // let filename = db_file.clone().split(".").nth(0).unwrap();
        // let log_file = format!("{filename}.log");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(db_file)
            .expect("disk manager failed to open file");

        let size = file.metadata().unwrap().len();

        if size == 0 {
            println!("[DEBUG][DiskManager] empty file opened");
            // initialise with db file headers
        }

        DiskManager {
            db_file: file,
            status: true,
            page_directory: PageDirector::new(),
            cache: Cache::new(max_frames),
        }
    }

    pub fn size(self) -> usize {
        self.cache.max_frames
    }

    pub fn new_page(&mut self) -> PageID {
        let len = self.db_file.metadata().unwrap().len();

        if !self.page_directory.can_accomodate() {
            self.db_file.set_len(len + FRAME_SIZE).unwrap();
            println!(
                "[DEBUG][DiskManager] extending file size to add new page to {}",
                self.db_file.metadata().unwrap().len()
            );
        }

        let (registerd_page, offset) = self.page_directory.register_new_page();
        println!(
            "[DEBUG][DiskManager] New page {registerd_page} with size {FRAME_SIZE} created with offset {offset}"
        );

        let mut reader = BufReader::new(&self.db_file);
        reader.seek(SeekFrom::Start(offset as u64)).unwrap();

        let mut content: [u8; FRAME_SIZE as usize] = [0; FRAME_SIZE as usize];
        reader
            .read_exact(&mut content)
            .unwrap_or_else(|_| panic!("failed to read {FRAME_SIZE} bytes from offset {offset}"));

        assert_eq!(content.len(), FRAME_SIZE as usize);

        let evict = self
            .cache
            .put_frame(registerd_page, offset, Box::new(content));
        if let Some(frame) = evict {
            let _ = self.flush_frame(frame);
        }

        registerd_page
    }

    /// TODO: proper error types
    /// Remove page from disk and memory. Find all traces of the page
    /// where it is being used and unallocate from memory
    ///
    /// once a page has been deleted, we need to ensure that its reference (offset)
    /// is also removed from the page directory and appended to the list of free
    /// slots that can be taken up by `DiskManager::new_page` the next time around
    /// while allocating a new page
    ///
    /// todo: implement pins
    pub fn delete_page(&mut self, page_id: PageID) -> Result<(), Box<dyn std::error::Error>> {
        let query_page = self.page_directory.query_page(page_id);
        if query_page.is_none() {
            return Err(Box::new(Error::DeletePageError));
        }

        let page_dir_delte = self.page_directory.remove_page(page_id);
        if page_dir_delte.is_err() {
            return page_dir_delte;
        }

        Ok(())
    }

    #[allow(unused)]
    pub fn write_page(
        &mut self,
        page_id: PageID,
        bytes: Box<[u8; FRAME_SIZE as usize]>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // first check if a page is in memory or not
        // the lookup will bring the frame to memory if not present
        let mut frame = self.load_frame(page_id);

        if frame.is_none() {
            return Err(Box::new(Error::WritePageError));
        }

        let frame = frame.unwrap();

        let mut handler = frame.write().unwrap();
        handler.content = bytes;
        handler.dirty = true;
        drop(handler);

        Ok(())
    }

    #[allow(unused)]
    pub fn read_page(&mut self, page_id: PageID) -> Box<[u8; FRAME_SIZE as usize]> {
        let frame = self.load_frame(page_id);

        if frame.is_none() {
            panic!("Failed to load frame");
        }

        let frame = frame.unwrap();

        return Box::clone(&frame.read().unwrap().content);
    }

    /// Method flushes dirty pages (pages that have been modified)
    /// to disk safely, while having a lock on the frame
    pub fn flush_page(&mut self, page_id: PageID) -> Result<(), Box<dyn std::error::Error>> {
        let frame = self.cache.lookup_frame(page_id);
        if frame.is_none() {
            return Err(Box::new(Error::CacheFetchMiss));
        }

        let frame = frame.unwrap();
        let mut writer = BufWriter::new(&self.db_file);
        let handler = frame.write().unwrap();

        writer.seek(SeekFrom::Start(handler.offset as u64)).unwrap();
        writer.write_all(&*handler.content).unwrap();
        writer.flush().unwrap();

        drop(writer);
        drop(handler);

        Ok(())
    }

    pub fn flush_frame(
        &mut self,
        frame: Arc<RwLock<Frame>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = BufWriter::new(&self.db_file);
        let handler = frame.read().unwrap();

        writer.seek(SeekFrom::Start(handler.offset as u64)).unwrap();
        writer.write_all(&*handler.content).unwrap();
        writer.flush().unwrap();

        drop(writer);
        drop(handler);
        drop(frame); // free from memory
        Ok(())
    }

    fn load_frame(&mut self, page_id: PageID) -> Option<Arc<RwLock<Frame>>> {
        if let Some(frame) = self.cache.lookup_frame(page_id) {
            println!(
                "[DEBUG][DiskManager][Cache] from cache page {}",
                frame.read().unwrap()
            );
            return Some(frame);
        }

        println!("[DEBUG][DiskManager] fetching from disk for {}", page_id);
        if let Some(offset) = self.page_directory.query_page(page_id) {
            let mut reader = BufReader::new(&self.db_file);
            reader.seek(SeekFrom::Start(offset as u64)).unwrap();

            let mut content: [u8; FRAME_SIZE as usize] = [0; FRAME_SIZE as usize];
            reader
                .read_exact(&mut content)
                .unwrap_or_else(|_| panic!("failed to read {FRAME_SIZE} from {offset}"));

            println!("[DEBUG][DiskManager] fetched from disk");

            println!("[DEBUG][DiskManager] updating cache");
            let evict = self.cache.put_frame(page_id, offset, Box::new(content));
            if let Some(frame) = evict {
                println!("flushing frame {}", frame.read().unwrap().page_id);
                let _ = self.flush_frame(frame);
            }

            return self.cache.lookup_frame(page_id);
        } else {
            println!("frame not available on disk");
            return None;
        }
    }

    pub fn get_db_size(&self) -> u64 {
        self.db_file
            .metadata()
            .expect("failed to read metadata for filesize")
            .len()
    }
}

pub struct DiskScheduler {}

impl DiskScheduler {}
