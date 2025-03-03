use std::sync::{Arc, Mutex};

use crate::storage::page::PageID;

use super::scheduler::DiskManager;

#[allow(unused)]
pub struct BufferPoolManager {
    /// params
    /// max_frames     : max number of frames that can be held
    ///                  in the cache
    /// disk_manager   : Reference to the DiskManager
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

    pub fn delete_page(&mut self, page_id: PageID) -> Result<(), Box<dyn std::error::Error>> {
        self.disk_manager.lock().unwrap().delete_page(page_id)
    }

    pub fn flush_page_unsafe(page_id: PageID) -> Result<(), Box<dyn std::error::Error>> {
        unimplemented!()
    }

    pub fn flush_page(page_id: PageID) -> Result<(), Box<dyn std::error::Error>> {
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
    fn test_create_manager() {
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
    fn test_single_page_alloc() {
        const FILE_PATH: &str = "/tmp/test_single_page_alloc.db";
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
    fn test_multiple_page_alloc() {
        const FILE_PATH: &str = "/tmp/test_multiple_page_alloc.db";
        let _ = fs::remove_file(FILE_PATH);

        let mut bpm = BufferPoolManager::new(3, FILE_PATH);

        bpm.new_page();
        bpm.new_page();
        bpm.new_page();
        bpm.new_page();

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

    #[test]
    fn test_delete_pages() {
        const FILE_PATH: &str = "/tmp/test_delete_pages.db";
        let _ = fs::remove_file(FILE_PATH);

        let mut bpm = BufferPoolManager::new(3, FILE_PATH);
        bpm.new_page();
        bpm.new_page();

        let mut delete_result = bpm.delete_page(3);
        assert_eq!(delete_result.is_err(), true);
        match delete_result {
            Ok(_) => println!("sucessfully deleted page"),
            Err(e) => println!("{}", e),
        }

        delete_result = bpm.delete_page(2);
        assert_eq!(delete_result.is_err(), false);
        match delete_result {
            Ok(_) => println!("sucessfully deleted page"),
            Err(e) => println!("{}", e),
        }

        let db_size = bpm.disk_manager.lock().unwrap().get_db_size();
        assert_eq!(db_size, FRAME_SIZE * 2);

        bpm.new_page();
        let db_size = bpm.disk_manager.lock().unwrap().get_db_size();
        assert_eq!(db_size, FRAME_SIZE * 2);
    }

    #[test]
    fn test_single_page_write() {
        const FILE_PATH: &str = "/tmp/single_page_write_test.db";
        let _ = fs::remove_file(FILE_PATH);

        let mut bpm = BufferPoolManager::new(3, FILE_PATH);
        bpm.new_page();
        bpm.new_page();

        let mut write_res = bpm
            .disk_manager
            .lock()
            .unwrap()
            .write_page(3, Box::new([1; FRAME_SIZE as usize]));
        assert_eq!(write_res.is_err(), true);

        let new_frame = Box::new([1; FRAME_SIZE as usize]);
        dbg!(&new_frame[0], &new_frame.len());

        write_res = bpm.disk_manager.lock().unwrap().write_page(1, new_frame);
        assert_eq!(write_res.is_err(), false);

        let frame = bpm.disk_manager.lock().unwrap().read_page(1);
        assert_eq!(
            frame.iter().map(|v| v.to_owned() as u64).sum::<u64>(),
            FRAME_SIZE
        )
    }

    #[test]
    fn test_single_page_flush() {
        const FILE_PATH: &str = "/tmp/test_single_page_flush.db";
        let _ = fs::remove_dir(FILE_PATH);

        let mut bpm = BufferPoolManager::new(1, FILE_PATH);
        bpm.new_page();

        let new_frame = Box::new([1; FRAME_SIZE as usize]);
        let write_res = bpm.disk_manager.lock().unwrap().write_page(1, new_frame);
        assert_eq!(write_res.is_err(), false);

        dbg!(
            bpm.disk_manager
                .lock()
                .unwrap()
                .cache
                .lookup_frame(1)
                .unwrap()
                .read()
                .unwrap()
                .content
                .iter()
                .map(|v| v.to_owned() as u64)
                .sum::<u64>(),
            FRAME_SIZE
        );

        bpm.new_page();

        assert_eq!(
            bpm.disk_manager
                .lock()
                .unwrap()
                .cache
                .lookup_frame(2)
                .is_none(),
            false
        );

        let read_res = bpm.disk_manager.lock().unwrap().read_page(1);
        assert_eq!(
            read_res.iter().map(|v| v.to_owned() as u64).sum::<u64>(),
            FRAME_SIZE
        );
    }
}
