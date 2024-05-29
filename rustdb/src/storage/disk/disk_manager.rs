use crate::storage::{PageId, PAGE_SIZE};
use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;

pub struct DiskManager {
    db_file: RwLock<tokio::fs::File>,
}

impl DiskManager {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let db_file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(path)
            .await?;
        Ok(DiskManager {
            db_file: RwLock::new(db_file),
        })
    }

    pub async fn write_page(
        &self,
        page_id: PageId,
        page_data: &[u8],
    ) -> Result<(), std::io::Error> {
        let offset = PAGE_SIZE as u64 * page_id as u64;
        let mut db_file = self.db_file.write().await;
        db_file.seek(SeekFrom::Start(offset)).await?;
        db_file.write_all(page_data).await?;
        db_file.flush().await?;
        Ok(())
    }
    pub async fn read_page(
        &self,
        page_id: PageId,
        page_data: &mut [u8],
    ) -> Result<(), std::io::Error> {
        let offset = PAGE_SIZE as u64 * page_id as u64;
        let mut db_file = self.db_file.write().await;
        db_file.seek(SeekFrom::Start(offset)).await?;
        db_file.read_exact(page_data).await?;
        Ok(())
    }
}
