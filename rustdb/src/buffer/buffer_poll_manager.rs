use crate::buffer::lru_k_replacer::LruKReplacer;
use crate::buffer::{Error, FrameId};
use crate::encoding::{Decoder, Encoder};
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::page::index::Node;
use crate::storage::page::table::{Table, TableNode};
use crate::storage::page::{Page, PageTrait};
use crate::storage::{PageId, PAGE_SIZE};
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{
    OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
};

pub struct BufferPoolManager {
    inner: RwLock<Inner>,
    disk_manager: DiskManager,
    next_page_id: AtomicUsize,
    pool_size: usize,
}

struct Inner {
    pages: Vec<Arc<Page>>,
    replacer: Arc<RwLock<LruKReplacer>>,
    page_table: HashMap<PageId, FrameId>,
    free_list: VecDeque<FrameId>,
}

impl BufferPoolManager {
    pub async fn new(pool_size: usize, k: usize, disk_manager: DiskManager) -> Result<Self, Error> {
        let replacer = Arc::new(RwLock::new(LruKReplacer::new(pool_size, k)));
        let mut free_list = VecDeque::with_capacity(pool_size);
        for frame_id in 0..pool_size {
            free_list.push_back(frame_id as FrameId);
        }
        let pages = {
            let mut v = Vec::with_capacity(pool_size);
            (0..pool_size).for_each(|_| v.push(Arc::new(Page::new(0))));
            v
        };
        let inner = Inner {
            pages,
            replacer,
            page_table: HashMap::with_capacity(pool_size),
            free_list,
        };
        Ok(Self {
            inner: RwLock::new(inner),
            disk_manager,
            next_page_id: AtomicUsize::new(0),
            pool_size,
        })
    }

    pub async fn new_page_ref(&self) -> Result<Option<PageRef>, Error> {
        let mut inner = self.inner.write().await;
        if let Some(frame_id) = self.available_frame(&mut inner).await? {
            let page_id = self.allocate_page();
            let page = Arc::new(Page::new(page_id));
            page.pin_count.store(1, Ordering::Relaxed);
            inner.pages[frame_id] = page.clone();
            inner.page_table.insert(page_id, frame_id);
            let mut replacer = inner.replacer.write().await;
            replacer.record_access(frame_id);
            replacer.set_evictable(frame_id, false);
            return Ok(Some(PageRef::new(
                page.clone(),
                frame_id,
                inner.replacer.clone(),
            )));
        }
        Ok(None)
    }

    pub async fn fetch_page_ref(&self, page_id: PageId) -> Result<Option<PageRef>, Error> {
        let mut inner = self.inner.write().await;
        // fetch page from cache
        if let Some(frame_id) = inner.page_table.get(&page_id).cloned() {
            // we can't take lock guard when we fetch from page; or it will be deadlock
            let page = inner.pages[frame_id].clone();
            page.pin_count.fetch_add(1, Ordering::Relaxed);
            let mut replacer = inner.replacer.write().await;
            replacer.record_access(frame_id);
            replacer.set_evictable(frame_id, false);
            return Ok(Some(PageRef::new(
                page.clone(),
                frame_id,
                inner.replacer.clone(),
            )));
        }
        // fetch page from disk
        let frame_id = self.available_frame(&mut inner).await?;
        if let Some(frame_id) = frame_id {
            let page = inner.pages[frame_id].clone();
            let page_data = page.data();
            let mut page_data = page_data.write().await;
            self.disk_manager
                .read_page(page_id, page_data.as_mut())
                .await?;
            drop(page_data);
            page.set_page_id(page_id);
            page.pin_count.store(1, Ordering::Relaxed);
            inner.page_table.insert(page_id, frame_id);
            let mut replacer = inner.replacer.write().await;
            replacer.record_access(frame_id);
            replacer.set_evictable(frame_id, false);
            return Ok(Some(PageRef::new(
                page.clone(),
                frame_id,
                inner.replacer.clone(),
            )));
        }
        Ok(None)
    }

    pub async fn flush_page(&self, page_id: PageId) -> Result<(), Error> {
        let inner = self.inner.write().await;
        if let Some(frame_id) = inner.page_table.get(&page_id).cloned() {
            let page = inner.pages[frame_id].clone();
            let page_data = page.data();
            let mut page_data = page_data.write().await;
            if page.is_dirty() {
                self.disk_manager
                    .write_page(page.page_id(), page_data.as_mut())
                    .await?;
                page.set_dirty(false);
            }
        }
        Ok(())
    }

    pub async fn flush_page_all(&self) -> Result<(), Error> {
        let inner = self.inner.write().await;
        for page in inner.pages.iter() {
            let page_data = page.data();
            let mut page_data = page_data.write().await;
            if page.is_dirty() {
                self.disk_manager
                    .write_page(page.page_id(), page_data.as_mut())
                    .await?;
                page.set_dirty(false);
            }
        }
        Ok(())
    }

    pub async fn delete_page(&self, page_id: PageId) -> Result<Option<PageId>, Error> {
        let mut inner = self.inner.write().await;
        if let Some(frame_id) = inner.page_table.get(&page_id).cloned() {
            let page = inner.pages[frame_id].clone();
            if page.pin_count.load(Ordering::Relaxed) > 0 {
                return Ok(None);
            }
            let page_data = page.data();
            let mut page_data = page_data.write().await;
            if page.is_dirty() {
                self.disk_manager
                    .write_page(page.page_id(), page_data.as_mut())
                    .await?;
                page.set_dirty(false);
            }
            drop(page_data);
            page.reset().await;
            inner.replacer.write().await.remove(frame_id)?;
            inner.free_list.push_back(frame_id);
            inner.page_table.remove(&page_id);
            return Ok(Some(page_id));
        }
        Ok(None)
    }
    async fn available_frame(
        &self,
        inner: &mut RwLockWriteGuard<'_, Inner>,
    ) -> Result<Option<FrameId>, Error> {
        if let Some(frame_id) = inner.free_list.pop_front() {
            return Ok(Some(frame_id));
        }
        let frame_id = inner.replacer.write().await.evict();
        if let Some(frame_id) = frame_id {
            let page = inner.pages[frame_id].clone();
            let page_data = page.data();
            let mut page_data = page_data.write().await;
            if page.is_dirty() {
                self.disk_manager
                    .write_page(page.page_id(), page_data.as_mut())
                    .await?;
                page.set_dirty(false);
            }
            drop(page_data);
            inner.page_table.remove(&page.page_id());
            return Ok(Some(frame_id));
        }
        Ok(None)
    }
    fn allocate_page(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::AcqRel)
    }
}

impl BufferPoolManager {
    pub async fn fetch_page_read_owned(
        &self,
        page_id: PageId,
    ) -> Result<OwnedPageDataReadGuard, Error> {
        let page = self
            .fetch_page_ref(page_id)
            .await?
            .ok_or(Error::BufferInsufficient)?
            .data_read_owned()
            .await;
        Ok(page)
    }

    pub async fn fetch_page_write_owned(
        &self,
        page_id: PageId,
    ) -> Result<OwnedPageDataWriteGuard, Error> {
        let page = self
            .fetch_page_ref(page_id)
            .await?
            .ok_or(Error::BufferInsufficient)?
            .data_write_owned()
            .await;
        Ok(page)
    }

    pub async fn try_fetch_page_read_owned(
        &self,
        page_id: PageId,
    ) -> Result<OwnedPageDataReadGuard, Error> {
        let page = self
            .fetch_page_ref(page_id)
            .await?
            .ok_or(Error::BufferInsufficient)?
            .try_data_read_owned()?;
        Ok(page)
    }

    pub async fn new_page_write_owned<T>(
        &self,
        node: &mut T,
    ) -> Result<OwnedPageDataWriteGuard, Error>
    where
        T: Encoder + PageTrait,
    {
        let guard = self
            .new_page_ref()
            .await?
            .ok_or(Error::BufferInsufficient)?
            .data_write_owned()
            .await;
        let page_id = guard.page_ref.page_id();
        node.set_page_id(page_id);
        Ok(guard)
    }
    pub async fn fetch_page_node<K>(&self, page_id: PageId) -> Result<(PageRef, Node<K>), Error>
    where
        K: Decoder,
    {
        let page = self
            .fetch_page_ref(page_id)
            .await?
            .ok_or(Error::BufferInsufficient)?;
        let node = page.page.node().await?;
        Ok((page, node))
    }

    pub async fn new_page_node<K>(&self, node: &mut Node<K>) -> Result<PageRef, Error>
    where
        K: Encoder,
    {
        let page = self
            .new_page_ref()
            .await?
            .ok_or(Error::BufferInsufficient)?;
        let page_id = page.page.page_id();
        node.set_page_id(page_id);
        page.page.write_node_back(node).await?;
        Ok(page)
    }

    pub async fn new_page_table(&self, table: &mut Table) -> Result<PageRef, Error> {
        let page = self
            .new_page_ref()
            .await?
            .ok_or(Error::BufferInsufficient)?;
        let page_id = page.page.page_id();
        table.set_page_id(page_id);
        page.page.write_table_back(table).await?;
        Ok(page)
    }
    pub async fn fetch_page_table(&self, page_id: PageId) -> Result<(PageRef, Table), Error> {
        let page = self
            .fetch_page_ref(page_id)
            .await?
            .ok_or(Error::BufferInsufficient)?;
        let table = page.page.table().await?;
        Ok((page, table))
    }

    pub async fn new_page_table_node(&self, table_node: &mut TableNode) -> Result<PageRef, Error> {
        let page = self
            .new_page_ref()
            .await?
            .ok_or(Error::BufferInsufficient)?;
        let page_id = page.page.page_id();
        table_node.set_page_id(page_id);
        page.page.write_table_node_back(table_node).await?;
        Ok(page)
    }
    pub async fn fetch_page_table_node(
        &self,
        page_id: PageId,
    ) -> Result<(PageRef, TableNode), Error> {
        let page = self
            .fetch_page_ref(page_id)
            .await?
            .ok_or(Error::BufferInsufficient)?;
        let table_node = page.page.table_node().await?;
        Ok((page, table_node))
    }
}
pub struct PageRef {
    page: Arc<Page>,
    frame_id: FrameId,
    replacer: Arc<RwLock<LruKReplacer>>,
}

pub struct PageDataWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, [u8; PAGE_SIZE]>,
    page_id: PageId,
    is_dirty: &'a AtomicBool,
}

pub struct PageDataReadGuard<'a> {
    guard: RwLockReadGuard<'a, [u8; PAGE_SIZE]>,
    page_id: PageId,
}

pub struct OwnedPageDataWriteGuard {
    guard: OwnedRwLockWriteGuard<[u8; PAGE_SIZE]>,
    page_ref: PageRef,
}

pub struct OwnedPageDataReadGuard {
    guard: OwnedRwLockReadGuard<[u8; PAGE_SIZE]>,
    page_ref: PageRef,
}

impl PageDataWriteGuard<'_> {
    pub fn page_id(&self) -> PageId {
        self.page_id
    }
}

impl PageDataReadGuard<'_> {
    pub fn page_id(&self) -> PageId {
        self.page_id
    }
}

impl OwnedPageDataWriteGuard {
    pub fn page_id(&self) -> PageId {
        self.page_ref.page_id()
    }
}

impl OwnedPageDataReadGuard {
    pub fn page_id(&self) -> PageId {
        self.page_ref.page_id()
    }
}

impl Drop for PageRef {
    fn drop(&mut self) {
        let page = self.page.clone();
        let frame_id = self.frame_id;
        let replacer = self.replacer.clone();
        tokio::spawn(async move {
            let prev = page.pin_count.fetch_sub(1, Ordering::Relaxed);
            if prev == 1 {
                replacer.write().await.set_evictable(frame_id, true);
            }
        });
    }
}

impl Deref for PageDataWriteGuard<'_> {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl DerefMut for PageDataWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

impl Drop for PageDataWriteGuard<'_> {
    fn drop(&mut self) {
        self.is_dirty.store(true, Ordering::Relaxed);
    }
}

impl Deref for PageDataReadGuard<'_> {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl Deref for OwnedPageDataWriteGuard {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl DerefMut for OwnedPageDataWriteGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

impl Drop for OwnedPageDataWriteGuard {
    fn drop(&mut self) {
        self.page_ref.page.set_dirty(true);
    }
}

impl Deref for OwnedPageDataReadGuard {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl PageRef {
    pub fn new(page: Arc<Page>, frame_id: FrameId, replacer: Arc<RwLock<LruKReplacer>>) -> Self {
        Self {
            page,
            frame_id,
            replacer,
        }
    }

    pub fn page(&self) -> &Arc<Page> {
        &self.page
    }
    pub fn page_id(&self) -> PageId {
        self.page.page_id()
    }
    pub async fn data_write(&self) -> PageDataWriteGuard<'_> {
        let guard = self.page.data_ref().write().await;
        PageDataWriteGuard {
            guard,
            page_id: self.page.page_id(),
            is_dirty: &self.page.is_dirty,
        }
    }

    pub async fn data_read(&self) -> PageDataReadGuard<'_> {
        let guard = self.page.data_ref().read().await;
        PageDataReadGuard {
            guard,
            page_id: self.page_id(),
        }
    }

    pub async fn data_write_owned(self) -> OwnedPageDataWriteGuard {
        let guard = self.page.data().write_owned().await;
        OwnedPageDataWriteGuard {
            guard,
            page_ref: self,
        }
    }

    pub async fn data_read_owned(self) -> OwnedPageDataReadGuard {
        let guard = self.page.data().read_owned().await;
        OwnedPageDataReadGuard {
            guard,
            page_ref: self,
        }
    }

    pub fn try_data_read_owned(self) -> Result<OwnedPageDataReadGuard, Error> {
        let guard = self.page.data().try_read_owned()?;
        Ok(OwnedPageDataReadGuard {
            guard,
            page_ref: self,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::Duration;

    #[tokio::test]
    async fn buffer_pool_manager() -> Result<(), Error> {
        let random_data = [2u8; PAGE_SIZE];
        let file = tempfile::NamedTempFile::new()?;
        let db_name = file.path();
        let buffer_pool_size = 10;
        let k = 5;
        // No matter if `char` is signed or unsigned by default, this constraint must be met

        let disk_manager = DiskManager::new(db_name).await?;
        let bpm = BufferPoolManager::new(buffer_pool_size, k, disk_manager).await?;

        let page0 = bpm.new_page_ref().await?;

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        assert!(page0.is_some());
        let page0 = page0.unwrap();
        assert_eq!(0, page0.page.page_id());

        // Scenario: Once we have a page, we should be able to read and write content.
        page0.data_write().await.clone_from_slice(&random_data);

        let mut pages = Vec::new();
        // Scenario: We should be able to create new pages until we fill up the buffer pool.
        for _ in 1..buffer_pool_size {
            let page = bpm.new_page_ref().await?;
            assert!(page.is_some());
            let page = page.unwrap();
            pages.push(page);
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
        for _i in buffer_pool_size..2 * buffer_pool_size {
            assert!(bpm.new_page_ref().await?.is_none())
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4}, we should be able to create 5 new pages
        {
            let _page0 = page0.data_write().await;
        }
        drop(page0);
        for i in 0..4 {
            if let Some(page) = pages.get(i) {
                let _page = page.data_write().await;
            }
            let _page = pages.remove(0);
            bpm.flush_page(i).await?;
        }
        // wait until page unpin
        tokio::time::sleep(Duration::from_millis(100)).await;

        for _ in 0..5 {
            let page = bpm.new_page_ref().await?;
            assert!(page.is_some());
            let _page_id = page.unwrap().page_id();
        }
        // wait until page unpin
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Scenario: We should be able to fetch the data we wrote a while ago.
        let page0 = bpm.fetch_page_ref(0).await?;
        assert!(page0.is_some());
        let page0 = page0.unwrap();
        assert_eq!(page0.data_read().await.as_ref(), &random_data);

        // Shutdown the disk manager and remove the temporary file we created.

        Ok(())
    }

    #[tokio::test]
    async fn simple() -> Result<(), Error> {
        let file = tempfile::NamedTempFile::new()?;
        let db_name = file.path();
        let buffer_pool_size = 10;
        let k = 5;

        let disk_manager = DiskManager::new(db_name).await?;
        let bpm = BufferPoolManager::new(buffer_pool_size, k, disk_manager).await?;

        let page0 = bpm.new_page_ref().await?;

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        assert!(page0.is_some());
        let page0 = page0.unwrap();
        assert_eq!(0, page0.page_id());

        // Scenario: Once we have a page, we should be able to read and write content.
        let data = "Hello".as_bytes();
        page0.data_write().await.as_mut().write_all(data)?;

        // Scenario: We should be able to create new pages until we fill up the buffer pool.
        let mut pages = Vec::new();
        for _ in 1..buffer_pool_size {
            let page = bpm.new_page_ref().await?;
            assert!(page.is_some());
            let page = page.unwrap();
            {
                let _page = page.data_write().await;
            }
            pages.push(page);
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
        for _ in buffer_pool_size..buffer_pool_size * 2 {
            assert!(bpm.new_page_ref().await?.is_none());
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
        // there would still be one buffer page left for reading page 0.
        {
            let _page0 = page0.data_write().await;
        }
        drop(page0);
        for _ in 0..4 {
            pages.remove(0);
        }
        // wait until page unpin
        tokio::time::sleep(Duration::from_millis(100)).await;

        for _ in 0..4 {
            let page = bpm.new_page_ref().await?;
            assert!(page.is_some());
            pages.push(page.unwrap());
        }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        let page0 = bpm.fetch_page_ref(0).await?;
        assert!(page0.is_some());
        let page0 = page0.unwrap();
        let mut data = [0u8; PAGE_SIZE];
        let mut data_slice = &mut data[..];
        data_slice.write_all("Hello".as_bytes())?;
        assert_eq!(page0.data_read().await.as_ref(), data);

        // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
        // now be pinned. Fetching page 0 again should fail.
        {
            let _page0 = page0.data_write().await;
        }
        drop(page0);
        // wait until page unpin
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(bpm.new_page_ref().await?.is_some());
        assert!(bpm.fetch_page_ref(0).await?.is_none());

        // Shutdown the disk manager and remove the temporary file we created.
        tokio::fs::remove_file(db_name).await?;

        Ok(())
    }
}
