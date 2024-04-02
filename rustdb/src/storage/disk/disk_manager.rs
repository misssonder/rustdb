use crate::error::RustDBResult;
use crate::storage::{PageId, PAGE_SIZE};
use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub struct DiskManager {
    db_file: tokio::fs::File,
}

impl DiskManager {
    pub async fn new(path: impl AsRef<Path>) -> RustDBResult<Self> {
        let db_file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .await?;
        Ok(DiskManager { db_file })
    }

    pub async fn write_page(&mut self, page_id: PageId, page_data: &[u8]) -> RustDBResult<()> {
        let offset = PAGE_SIZE as u64 * page_id as u64;
        self.db_file.seek(SeekFrom::Start(offset)).await?;
        self.db_file.write_all(page_data).await?;
        self.db_file.flush().await?;
        Ok(())
    }
    pub async fn read_page(&mut self, page_id: PageId, page_data: &mut [u8]) -> RustDBResult<()> {
        let offset = PAGE_SIZE as u64 * page_id as u64;
        self.db_file.seek(SeekFrom::Start(offset)).await?;
        self.db_file.read_exact(page_data).await?;
        Ok(())
    }
}
