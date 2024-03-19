use crate::buffer::lru_k_replacer::LruKReplacer;
use crate::buffer::FrameId;
use crate::error::{RustDBError, RustDBResult};
use crate::storage::codec::{Decoder, Encoder};
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::page::b_plus_tree::Node;
use crate::storage::page::{Page, PageRef};
use crate::storage::PageId;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct BufferPoolManager {
    pages: Vec<Arc<RwLock<Page>>>,
    replacer: LruKReplacer,
    page_table: HashMap<PageId, FrameId>,
    free_list: VecDeque<FrameId>,
    disk_manager: DiskManager,
    next_page_id: AtomicUsize,
    pool_size: usize,
}

impl BufferPoolManager {
    pub async fn new(pool_size: usize, k: usize, disk_manager: DiskManager) -> RustDBResult<Self> {
        let replacer = LruKReplacer::new(pool_size, k);
        let mut free_list = VecDeque::with_capacity(pool_size);
        for frame_id in 0..pool_size {
            free_list.push_back(frame_id as FrameId);
        }
        let pages = {
            let mut v = Vec::with_capacity(pool_size);
            (0..pool_size).for_each(|_| v.push(Arc::new(RwLock::new(Page::new(0)))));
            v
        };
        Ok(Self {
            pages,
            replacer,
            page_table: HashMap::new(),
            free_list,
            disk_manager,
            next_page_id: AtomicUsize::new(0),
            pool_size,
        })
    }

    pub async fn new_page(&mut self) -> RustDBResult<Option<Arc<RwLock<Page>>>> {
        if let Some(frame_id) = self.available_frame().await? {
            let page_id = self.allocate_page();
            let mut page = Page::new(page_id);
            page.pin_count = 1;
            self.pages.insert(frame_id, Arc::new(RwLock::new(page)));
            self.page_table.insert(page_id, frame_id);
            self.replacer.record_access(frame_id);
            self.replacer.set_evictable(frame_id, false);
            return Ok(self.pages.get(frame_id).cloned());
        }
        Ok(None)
    }

    pub async fn new_page_ref(&mut self) -> RustDBResult<Option<PageRef>> {
        if let Some(frame_id) = self.available_frame().await? {
            let page_id = self.allocate_page();
            let mut page = Page::new(page_id);
            page.pin_count = 1;
            self.pages.insert(frame_id, Arc::new(RwLock::new(page)));
            self.page_table.insert(page_id, frame_id);
            self.replacer.record_access(frame_id);
            self.replacer.set_evictable(frame_id, false);
            return Ok(self
                .pages
                .get(frame_id)
                .map(|page| PageRef::new(page.clone())));
        }
        Ok(None)
    }
    pub async fn fetch_page(&mut self, page_id: PageId) -> RustDBResult<Option<Arc<RwLock<Page>>>> {
        // fetch page from cache
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let page = self.pages.get_mut(*frame_id).unwrap();
            {
                let mut page = page.write().await;
                page.pin_count += 1;
            }
            self.replacer.record_access(*frame_id);
            self.replacer.set_evictable(*frame_id, false);
            return Ok(Some(page.clone()));
        }
        // fetch page from disk
        if let Some(frame_id) = self.available_frame().await? {
            let page = self.pages.get_mut(frame_id).unwrap();
            {
                let mut page = page.write().await;
                self.disk_manager
                    .read_page(page_id, page.mut_data())
                    .await?;
                page.set_page_id(page_id);
                page.pin_count = 1;
            }
            self.page_table.insert(page_id, frame_id);
            self.replacer.record_access(frame_id);
            self.replacer.set_evictable(frame_id, false);
            return Ok(Some(page.clone()));
        }
        Ok(None)
    }

    pub async fn fetch_page_ref(&mut self, page_id: PageId) -> RustDBResult<Option<PageRef>> {
        Ok(self.fetch_page(page_id).await?.map(PageRef::new))
    }

    pub async fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> Option<PageId> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let mut page = self.pages.get_mut(*frame_id).unwrap().write().await;
            if page.is_dirty() {
                return None;
            }
            page.pin_count -= 1;
            if page.pin_count == 0 {
                self.replacer.set_evictable(*frame_id, true);
            }
            if is_dirty {
                page.set_dirty(is_dirty);
            }
            return Some(page_id);
        }
        None
    }

    pub async fn flush_page(&mut self, page_id: PageId) -> RustDBResult<()> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let page = self.pages.get_mut(*frame_id).unwrap();
            let mut page = page.write().await;
            if page.is_dirty() {
                self.disk_manager
                    .write_page(page.page_id(), page.data())
                    .await?;
                page.set_dirty(false);
            }
        }
        Ok(())
    }

    pub async fn flush_page_all(&mut self) -> RustDBResult<()> {
        for page in self.pages.iter_mut() {
            let mut page = page.write().await;
            if page.is_dirty() {
                self.disk_manager
                    .write_page(page.page_id(), page.data())
                    .await?;
                page.set_dirty(false);
            }
        }
        Ok(())
    }

    pub async fn delete_page(&mut self, page_id: PageId) -> RustDBResult<Option<PageId>> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let mut page = self.pages.get_mut(*frame_id).unwrap().write().await;
            if page.pin_count > 0 {
                return Ok(None);
            }
            if page.is_dirty() {
                self.disk_manager
                    .write_page(page.page_id(), page.data())
                    .await?;
                page.set_dirty(false);
            }
            page.reset();
            self.replacer.remove(*frame_id)?;
            self.free_list.push_back(*frame_id);
            self.page_table.remove(&page_id);
            return Ok(Some(page_id));
        }
        Ok(None)
    }
    async fn available_frame(&mut self) -> RustDBResult<Option<FrameId>> {
        if let Some(frame_id) = self.free_list.pop_front() {
            return Ok(Some(frame_id));
        }
        if let Some(frame_id) = self.replacer.evict() {
            if let Some(page) = self.pages.get_mut(frame_id) {
                let mut page = page.write().await;
                if page.is_dirty() {
                    self.disk_manager
                        .write_page(page.page_id(), page.data())
                        .await?;
                    page.set_dirty(false);
                }
                self.page_table.remove(&page.page_id());
                return Ok(Some(frame_id));
            }
        }
        Ok(None)
    }
    fn allocate_page(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::AcqRel)
    }
}

impl BufferPoolManager {
    pub async fn fetch_page_node<K>(&mut self, page_id: PageId) -> RustDBResult<Node<K>>
    where
        K: Decoder<Error = RustDBError>,
    {
        self.fetch_page_ref(page_id)
            .await?
            .ok_or(RustDBError::BufferPool("Can't fetch page".into()))?
            .read()
            .await
            .node()
    }

    pub async fn encode_page_node<K>(&mut self, node: &Node<K>) -> RustDBResult<()>
    where
        K: Encoder<Error = RustDBError>,
    {
        self.fetch_page_ref(node.page_id())
            .await?
            .ok_or(RustDBError::BufferPool("Can't fetch page".into()))?
            .write()
            .await
            .write_back(node)
    }

    pub async fn new_page_encode<K>(&mut self, node: &mut Node<K>) -> RustDBResult<()>
    where
        K: Encoder<Error = RustDBError>,
    {
        let page = self
            .new_page_ref()
            .await?
            .ok_or(RustDBError::BufferPool("Can't new page".into()))?;
        let page_id = page.read().await.page_id();
        node.set_page_id(page_id);
        node.encode(&mut page.write().await.mut_data())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::PAGE_SIZE;
    use std::io::Write;

    #[tokio::test]
    async fn test_buffer_pool_manager() -> RustDBResult<()> {
        let random_data = [2u8; PAGE_SIZE];
        let db_name = "test1.db";
        let buffer_pool_size = 10;
        let k = 5;
        // No matter if `char` is signed or unsigned by default, this constraint must be met

        let disk_manager = DiskManager::new(db_name).await?;
        let mut bpm = BufferPoolManager::new(buffer_pool_size, k, disk_manager).await?;

        let page0 = bpm.new_page().await?;

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        assert!(page0.is_some());
        assert_eq!(0, page0.clone().unwrap().read().await.page_id());

        // Scenario: Once we have a page, we should be able to read and write content.
        page0
            .unwrap()
            .write()
            .await
            .mut_data()
            .clone_from_slice(&random_data);

        // Scenario: We should be able to create new pages until we fill up the buffer pool.
        for i in 1..buffer_pool_size {
            assert!(bpm.new_page().await?.is_some())
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
        for i in buffer_pool_size..2 * buffer_pool_size {
            assert!(bpm.new_page().await?.is_none())
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4}, we should be able to create 5 new pages
        for i in 0..5 {
            assert!(bpm.unpin_page(i, true).await.is_some());
            bpm.flush_page(i).await?;
        }

        for i in 0..5 {
            let page = bpm.new_page().await?;
            assert!(page.is_some());
            let page_id = page.unwrap().read().await.page_id();
            bpm.unpin_page(page_id, false).await;
        }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        let page0 = bpm.fetch_page(0).await?;
        assert!(page0.is_some());
        assert_eq!(page0.unwrap().read().await.data(), &random_data);
        assert!(bpm.unpin_page(0, true).await.is_some());

        // Shutdown the disk manager and remove the temporary file we created.
        tokio::fs::remove_file(db_name).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_sample() -> RustDBResult<()> {
        let db_name = "test2.db";
        let buffer_pool_size = 10;
        let k = 5;

        let disk_manager = DiskManager::new(db_name).await?;
        let mut bpm = BufferPoolManager::new(buffer_pool_size, k, disk_manager).await?;

        let page0 = bpm.new_page().await?;

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        assert!(page0.is_some());
        let page0 = page0.unwrap();
        assert_eq!(0, page0.read().await.page_id());

        // Scenario: Once we have a page, we should be able to read and write content.
        let data = "Hello".as_bytes();
        page0.write().await.mut_data().write_all(data)?;

        // Scenario: We should be able to create new pages until we fill up the buffer pool.
        for i in 1..buffer_pool_size {
            assert!(bpm.new_page().await?.is_some());
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
        for i in buffer_pool_size..buffer_pool_size * 2 {
            assert!(bpm.new_page().await?.is_none());
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
        // there would still be one buffer page left for reading page 0.
        for i in 0..5 {
            assert!(bpm.unpin_page(i, true).await.is_some())
        }

        for i in 0..4 {
            assert!(bpm.new_page().await?.is_some())
        }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        let page0 = bpm.fetch_page(0).await?;
        assert!(page0.is_some());
        let page0 = page0.unwrap();
        let mut data = [0u8; PAGE_SIZE];
        let mut data_slice = &mut data[..];
        data_slice.write_all("Hello".as_bytes())?;
        assert_eq!(page0.read().await.data(), data);

        // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
        // now be pinned. Fetching page 0 again should fail.
        assert!(bpm.unpin_page(0, true).await.is_some());
        assert!(bpm.new_page().await?.is_some());
        assert!(bpm.fetch_page(0).await?.is_none());

        // Shutdown the disk manager and remove the temporary file we created.
        tokio::fs::remove_file(db_name).await?;

        Ok(())
    }
}
