use std::sync::{Arc, Mutex};

use crate::storage::page::PageID;

use super::scheduler::DiskManager;

#[allow(unused)]
pub struct BufferPoolManager {
    /// params
    /// max_frames     : max number of frames that can be held
    ///                  in the cache
    /// disc_manager   : Reference to the DiskManager
    max_frames: usize,
    pub disk_manager: Arc<Mutex<DiskManager>>,
}

impl BufferPoolManager {
    pub fn new(max_frames: usize, db_file: &str) -> BufferPoolManager {
        let disk_manager = DiskManager::new(max_frames, db_file);

        BufferPoolManager {
            max_frames,
            disk_manager: Arc::new(Mutex::new(disk_manager)),
        }
    }

    pub fn new_page(&mut self) {
        self.disk_manager.lock().unwrap().new_page();
    }

    pub fn read_page(&mut self, page_id: PageID) {
        let mut disk_manager = self.disk_manager.lock().unwrap();
        let page = disk_manager.read_page(page_id);
    }

    pub fn delete_page(&mut self, page_id: PageID) {
        unimplemented!()
    }

    pub fn flush_page_unsafe(page_id: PageID) {
        unimplemented!()
    }

    pub fn flush_page(page_id: PageID) {
        unimplemented!()
    }

    pub fn flush_all_pages_unsafe() {
        unimplemented!()
    }

    pub fn flush_all_page() {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use std::fs::{self, OpenOptions};

    use crate::storage::page::FRAME_SIZE;

    use super::BufferPoolManager;

    // #[ignore = "reason"]
    #[test]
    fn create_manager() {
        const FILE_PATH: &str = "/tmp/create_bpm_manger_test.db";
        let _ = fs::remove_file(FILE_PATH);

        let _bpm = BufferPoolManager::new(3, FILE_PATH);

        let file = OpenOptions::new()
            .read(true)
            .open(FILE_PATH)
            .unwrap_or_else(|_| panic!("failed to open {}", FILE_PATH));

        let file_size = file.metadata().unwrap().len();

        assert_eq!(file_size, 0);

        fs::remove_file(FILE_PATH).unwrap();
    }

    // #[ignore = "reason"]
    #[test]
    fn test_single_page_write() {
        const FILE_PATH: &str = "/tmp/test_single_page_write.db";
        let _ = fs::remove_file(FILE_PATH);

        let bpm = BufferPoolManager::new(3, FILE_PATH);

        let mut writer = bpm.disk_manager.lock().unwrap();
        writer.new_page();
        drop(writer);

        let file = OpenOptions::new()
            .read(true)
            .open(FILE_PATH)
            .unwrap_or_else(|_| panic!("failed to open {}", FILE_PATH));

        let file_size = file.metadata().unwrap().len();
        println!("[TEST][DEBUG][BPM] new page alloc -> filesize {file_size}");

        assert_eq!(file_size, FRAME_SIZE);

        fs::remove_file(FILE_PATH).unwrap();
    }

    // #[ignore = "reason"]
    #[test]
    fn test_multiple_page_creations() {
        const FILE_PATH: &str = "/tmp/test_multiple_page_creations.db";
        let _ = fs::remove_file(FILE_PATH);

        let mut bpm = BufferPoolManager::new(3, FILE_PATH);

        bpm.new_page();
        bpm.new_page();
        bpm.new_page();
        bpm.new_page();

        // let mut writer = bpm.disk_manager.lock().unwrap();
        // writer.new_page();
        // writer.new_page();
        // writer.new_page();
        // writer.new_page();
        // drop(writer);

        let file = OpenOptions::new()
            .read(true)
            .open(FILE_PATH)
            .unwrap_or_else(|_| panic!("failed to open {}", FILE_PATH));

        let file_size = file.metadata().unwrap().len();
        println!("[TEST][DEBUG][BPM] new page alloc -> filesize {file_size}");

        assert_eq!(file_size, FRAME_SIZE * 4);

        let mut writer = bpm.disk_manager.lock().unwrap();

        let frame_content = writer.read_page(2);
        assert_eq!(frame_content.len(), FRAME_SIZE as usize);

        let _ = writer.read_page(1); // this will be fetched out of memory
                                     // and will push page 3 out of memory

        drop(writer);

        fs::remove_file(FILE_PATH).unwrap();
    }
}
