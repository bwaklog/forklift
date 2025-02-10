use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom},
    sync::Arc,
};

use crate::storage::{
    directory::PageDirector,
    page::{PageID, FRAME_SIZE},
};

use super::cache::Cache;

#[allow(unused)]
pub struct DiskManager {
    db_file: File,
    status: bool,

    page_directory: PageDirector,
    pub cache: Cache,
}

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

    pub fn get_db_size(&self) -> u64 {
        self.db_file
            .metadata()
            .expect("failed to read metadata for filesize")
            .len()
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

        self.cache
            .put_frame(registerd_page, offset, Arc::new(content));

        registerd_page
    }

    #[allow(unused)]
    pub fn write_page(&mut self, page_id: PageID, bytes: &[u8; FRAME_SIZE as usize]) {
        todo!()
    }

    #[allow(unused)]
    pub fn read_page(&mut self, page_id: PageID) -> [u8; FRAME_SIZE as usize] {
        self.cache.lookup_frame(page_id);
        todo!()
    }
}

pub struct DiskScheduler {}

impl DiskScheduler {}
