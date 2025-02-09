use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter},
};

use crate::storage::{
    directory::PageDirector,
    page::{FRAME_SIZE, PageID},
};

pub struct DiskManager {
    db_file: File,
    status: bool,

    page_directory: PageDirector,
}

impl DiskManager {
    pub fn new(db_file: &str) -> DiskManager {
        let filename = db_file.clone().split(".").nth(0).unwrap();
        let log_file = format!("{filename}.log");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(db_file)
            .expect("disk manager failed to open file");

        let size = file.metadata().unwrap().len();

        if size == 0 {
            println!("[DEBUG] empty file opened");
            // initialise with db file headers
        }

        DiskManager {
            db_file: file,
            status: true,
            page_directory: PageDirector::new(),
        }
    }

    pub fn new_page(&mut self) -> PageID {
        if self.page_directory.empty() {
            let len = self.db_file.metadata().unwrap().len();
            self.db_file.set_len(len + FRAME_SIZE).unwrap();
        }
        0
    }

    pub fn write_page(&mut self, page_id: PageID, bytes: &[u8; FRAME_SIZE as usize]) {
        todo!()
    }

    pub fn read_page(page_id: PageID) -> [u8; FRAME_SIZE as usize] {
        todo!()
    }
}

pub struct DiskScheduler {}

impl DiskScheduler {}
