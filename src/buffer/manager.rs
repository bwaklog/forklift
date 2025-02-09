use std::sync::{Arc, RwLock};

use super::scheduler::DiskManager;

pub struct BufferPoolManager {
    disc_manager: Arc<RwLock<DiskManager>>,
}

impl BufferPoolManager {
    pub fn new(num_frames: usize, db_file: &str) -> BufferPoolManager {
        let disk_manager = DiskManager::new(db_file.clone());

        BufferPoolManager {
            disc_manager: Arc::new(RwLock::new(disk_manager)),
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs::{self, OpenOptions};

    use crate::storage::page::FRAME_SIZE;

    use super::BufferPoolManager;

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

    #[test]
    fn test_single_page_write() {
        const FILE_PATH: &str = "/tmp/test_single_page_write.db";
        let _ = fs::remove_file(FILE_PATH);

        let bpm = BufferPoolManager::new(3, FILE_PATH);

        let mut writer = bpm.disc_manager.write().unwrap();
        writer.new_page();
        drop(writer);

        let file = OpenOptions::new()
            .read(true)
            .open(FILE_PATH)
            .unwrap_or_else(|_| panic!("failed to open {}", FILE_PATH));

        let file_size = file.metadata().unwrap().len();
        println!("[DEBUG] new page alloc -> filesize {file_size}");

        assert_eq!(file_size, FRAME_SIZE);

        // fs::remove_file(FILE_PATH).unwrap();
    }
}
