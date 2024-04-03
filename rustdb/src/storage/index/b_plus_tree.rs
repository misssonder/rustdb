use crate::buffer::buffer_poll_manager::BufferPoolManager;
use crate::error::{RustDBError, RustDBResult};
use crate::storage::codec::{Decoder, Encoder};
use crate::storage::page::b_plus_tree::{Header, Internal, Leaf, Node};
use crate::storage::{PageId, RecordId};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::Range;
use tokio::io::AsyncWriteExt;

pub struct Index {
    buffer_pool: BufferPoolManager,
    root: PageId,
    init: bool,
    max_size: usize,
}

impl Index {
    pub async fn search<K>(&mut self, key: &K) -> RustDBResult<Option<RecordId>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord,
    {
        if self.init {
            if let Node::Leaf(ref leaf) = self.find_leaf(key).await? {
                return Ok(leaf.search(key));
            }
        }
        Ok(None)
    }

    // todo change range to RangeBounds
    pub async fn search_range<K>(&mut self, range: Range<K>) -> RustDBResult<Vec<RecordId>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord,
    {
        let mut result = Vec::new();
        if self.init {
            let mut leaf = self.find_leaf(&range.start).await?.assume_leaf();
            loop {
                let start = leaf.kv.binary_search_by(|(k, _)| k.cmp(&range.start));
                let end = leaf.kv.binary_search_by(|(k, _)| k.cmp(&range.end));
                match (start, end) {
                    (Ok(start_index), Ok(end_index)) => {
                        for (_, v) in leaf.kv[start_index..=end_index].iter() {
                            result.push(*v);
                        }
                    }
                    (Ok(start_index), Err(end_index)) => {
                        if end_index < leaf.kv.len() {
                            for (_, v) in leaf.kv[start_index..=end_index].iter() {
                                result.push(*v);
                            }
                        } else {
                            for (_, v) in leaf.kv[start_index..].iter() {
                                result.push(*v);
                            }
                        }
                    }
                    (Err(start_index), Ok(end_index)) => {
                        for (_, v) in leaf.kv[start_index..=end_index].iter() {
                            result.push(*v);
                        }
                    }
                    (Err(start_index), Err(end_index)) => {
                        if end_index == 0 {
                            break;
                        } else if end_index < leaf.kv.len() {
                            if start_index < leaf.kv.len() {
                                for (_, v) in leaf.kv[start_index..=end_index].iter() {
                                    result.push(*v);
                                }
                            }
                        } else if start_index < leaf.kv.len() {
                            for (_, v) in leaf.kv[start_index..].iter() {
                                result.push(*v);
                            }
                        } else {
                            break;
                        }
                    }
                }
                match leaf.next() {
                    None => break,
                    Some(next_id) => {
                        leaf = self
                            .buffer_pool
                            .fetch_page_node(next_id)
                            .await?
                            .1
                            .assume_leaf();
                    }
                }
            }
        }
        Ok(result)
    }

    pub async fn insert<K>(&mut self, key: K, value: RecordId) -> RustDBResult<()>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Default + Clone,
    {
        if !self.init {
            self.init_tree(key, value).await?;
            return Ok(());
        }
        let node = self.find_leaf(&key).await?;
        self.insert_inner(node, key, value).await
    }

    pub async fn delete<K>(&mut self, key: &K) -> RustDBResult<Option<(K, RecordId)>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Clone + Default,
    {
        if !self.init {
            return Ok(None);
        }
        let node = self.find_leaf(key).await?;
        self.delete_inner(node, key).await
    }

    pub async fn insert_inner<K>(
        &mut self,
        mut node: Node<K>,
        key: K,
        value: RecordId,
    ) -> RustDBResult<()>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Default + Clone,
    {
        loop {
            match node {
                Node::Internal(ref mut _internal) => {}
                Node::Leaf(ref mut leaf) => {
                    match leaf.kv.binary_search_by(|(k, _)| k.cmp(&key)) {
                        Ok(index) => leaf.kv[index] = (key.clone(), value),
                        Err(index) => leaf.insert(index, key.clone(), value),
                    };
                    self.buffer_pool.encode_page_node(&node).await?;
                }
            }
            if !node.is_overflow() {
                return Ok(());
            }
            let (median_key, mut sibling) = node.split();
            let sibling_page = self.buffer_pool.new_page_node(&mut sibling).await?;
            let sibling_page_id = sibling.page_id();
            if let Node::Internal(ref mut internal) = sibling {
                for (_, child) in internal.kv.iter() {
                    let (child_page, mut child_node) =
                        self.buffer_pool.fetch_page_node::<K>(*child).await?;
                    child_node.set_parent(sibling_page_id);
                    child_page.write().await.write_back(&child_node)?;
                }
            }
            node.set_next(sibling.page_id());
            sibling.set_prev(node.page_id());
            let parent_node = if let Some(parent_id) = node.parent() {
                let (parent_page, mut parent_node) =
                    self.buffer_pool.fetch_page_node::<K>(parent_id).await?;
                let internal = parent_node.assume_internal_mut();
                let index = internal
                    .kv
                    .binary_search_by(|(k, _)| k.cmp(&median_key))
                    .unwrap_or_else(|index| index);
                internal.insert(index, median_key.clone(), sibling_page_id);
                parent_page.write().await.write_back(&parent_node)?;
                parent_node
            } else {
                let mut parent_node = Node::Internal(Internal {
                    header: Header {
                        size: 1,
                        max_size: node.max_size(),
                        parent: None,
                        page_id: 0,
                        next: None,
                        prev: None,
                    },
                    kv: vec![
                        (K::default(), node.page_id()),
                        (median_key.clone(), sibling_page_id),
                    ],
                });
                let parent_page = self.buffer_pool.new_page_node(&mut parent_node).await?;
                self.root = parent_node.page_id();
                node.set_parent(parent_node.page_id());
                sibling.set_parent(parent_node.page_id());
                parent_page.write().await.write_back(&parent_node)?;
                parent_node
            };
            self.buffer_pool.encode_page_node(&node).await?;
            sibling_page.write().await.write_back(&sibling)?;
            node = parent_node;
        }
    }

    pub async fn delete_inner<K>(
        &mut self,
        mut node: Node<K>,
        key: &K,
    ) -> RustDBResult<Option<(K, RecordId)>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Default + Clone,
    {
        let mut res = None;
        loop {
            match node {
                Node::Internal(ref mut _internal) => {}
                Node::Leaf(ref mut leaf) => {
                    res = match leaf.remove(key) {
                        None => return Ok(None),
                        other => other,
                    };
                }
            }
            if !node.is_underflow() {
                self.buffer_pool.encode_page_node(&node).await?;
                break;
            }
            match node.parent() {
                None => break,
                Some(parent_id) => {
                    let (_parent_page, parent) =
                        self.buffer_pool.fetch_page_node::<K>(parent_id).await?;
                    let mut parent = parent.assume_internal();
                    let (index, _) = parent.search(key);
                    if self.steal(&mut parent, &mut node, index).await?.is_some() {
                        break;
                    }
                    if self.merge(&mut parent, node, index).await? {
                        break;
                    };
                    node = Node::Internal(parent)
                }
            }
        }
        Ok(res)
    }

    pub async fn steal<K>(
        &mut self,
        parent: &mut Internal<K>,
        node: &mut Node<K>,
        index: usize,
    ) -> RustDBResult<Option<()>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Default + Clone,
    {
        //steal from left
        let prev = match index > 0 {
            true => Some(index - 1),
            false => None,
        };
        let next = match index >= parent.kv.len() - 1 {
            true => None,
            false => Some(index + 1),
        };
        match node {
            Node::Internal(internal) => {
                if let Some(prev_index) = prev {
                    let prev_id = parent.kv[prev_index].1;
                    let (prev_page, mut prev_node) =
                        self.buffer_pool.fetch_page_node::<K>(prev_id).await?;
                    let mut prev_node = prev_node.assume_internal();
                    if let Some(steal) = prev_node.steal_last() {
                        // steal from prev node and change parent
                        let right_index = index;
                        internal.push_front(parent.kv[right_index].0.clone(), steal.1);
                        parent.kv[right_index].0 = steal.0;
                        // change child parent pointer
                        let (child_page, mut child) =
                            self.buffer_pool.fetch_page_node::<K>(steal.1).await?;
                        child.set_parent(internal.page_id());
                        child_page.write().await.write_back(&child)?;
                        prev_page.write().await.write_back(&Node::Internal(prev_node))?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(internal.clone()))
                            .await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(parent.clone()))
                            .await?;
                        return Ok(Some(()));
                    }
                }
                if let Some(next_index) = next {
                    let next_id = parent.kv[next_index].1;
                    let (next_page, mut next_node) =
                        self.buffer_pool.fetch_page_node::<K>(next_id).await?;
                    let mut next_node = next_node.assume_internal();
                    if let Some(steal) = next_node.steal_first() {
                        // steal from next node and change parent
                        let right_index = next_index;
                        internal.push_back(parent.kv[right_index].0.clone(), steal.1);
                        parent.kv[right_index].0 = steal.0;
                        // change child parent pointer
                        let (child_page, mut child) =
                            self.buffer_pool.fetch_page_node::<K>(steal.1).await?;
                        child.set_parent(internal.page_id());
                        child_page.write().await.write_back(&child)?;
                        next_page.write().await.write_back(&Node::Internal(next_node))?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(internal.clone()))
                            .await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(parent.clone()))
                            .await?;
                        return Ok(Some(()));
                    }
                }
            }
            Node::Leaf(ref mut leaf) => {
                if let Some(prev_index) = prev {
                    let prev_id = parent.kv[prev_index].1;
                    let (prev_page, mut prev_node) =
                        self.buffer_pool.fetch_page_node::<K>(prev_id).await?;
                    let mut prev_node = prev_node.assume_leaf();
                    if let Some(steal) = prev_node.steal_last() {
                        let right_index = index;
                        parent.kv[right_index].0 = steal.0.clone();
                        let (key, value) = steal;
                        leaf.push_front(key, value);
                        prev_page.write().await.write_back(&Node::Leaf(prev_node))?;
                        self.buffer_pool
                            .encode_page_node(&Node::Leaf(leaf.clone()))
                            .await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(parent.clone()))
                            .await?;
                        return Ok(Some(()));
                    }
                }
                if let Some(next_index) = next {
                    let next_id = parent.kv[next_index].1;
                    let (next_page, mut next_node) =
                        self.buffer_pool.fetch_page_node::<K>(next_id).await?;
                    let mut next_node = next_node.assume_leaf();
                    if let Some(steal) = next_node.steal_first() {
                        // steal from next node and change parent
                        let right_index = next_index;
                        parent.kv[right_index].0 = next_node.kv[0].0.clone();
                        let (key, value) = steal;
                        leaf.push_back(key, value);
                        next_page.write().await.write_back(&Node::Leaf(next_node))?;
                        self.buffer_pool
                            .encode_page_node(&Node::Leaf(leaf.clone()))
                            .await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(parent.clone()))
                            .await?;
                        return Ok(Some(()));
                    }
                }
            }
        }

        Ok(None)
    }

    async fn find_leaf<K>(&mut self, key: &K) -> RustDBResult<Node<K>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord,
    {
        let mut page_id = self.root;
        loop {
            let (page, node) = self.buffer_pool.fetch_page_node(page_id).await?;
            match node {
                Node::Internal(ref internal) => {
                    page_id = internal.search(key).1;
                }
                Node::Leaf(leaf) => {
                    return Ok(Node::Leaf(leaf));
                }
            }
        }
    }

    /// merge this node and it's prev node or next node
    /// return it's parent node
    pub async fn merge<K>(
        &mut self,
        parent: &mut Internal<K>,
        node: Node<K>,
        index: usize,
    ) -> RustDBResult<bool>
    where
        K: Encoder<Error = RustDBError> + Decoder<Error = RustDBError> + Clone + Ord,
    {
        let prev = match index > 0 {
            true => Some(index - 1),
            false => None,
        };
        let next = match index >= parent.kv.len() - 1 {
            true => None,
            false => Some(index + 1),
        };
        match node {
            Node::Internal(internal) => {
                let (mut left_node, mut right_node, right_index) = {
                    if let Some(prev_index) = prev {
                        let prev_id = parent.kv[prev_index].1;
                        let (_prev_page, prev_node) =
                            self.buffer_pool.fetch_page_node(prev_id).await?;
                        let prev_node = prev_node.assume_internal();
                        (prev_node, internal, index)
                    } else if let Some(next_index) = next {
                        let next_id = parent.kv[next_index].1;
                        let (_next_page, next_node) =
                            self.buffer_pool.fetch_page_node(next_id).await?;
                        let next_node = next_node.assume_internal();
                        (internal, next_node, next_index)
                    } else {
                        return Ok(true);
                    }
                };
                let changed_children = right_node.kv.iter().map(|(_, p)| *p).collect::<Vec<_>>();
                let (key, _) = parent.kv.remove(right_index);
                parent.header.size -= 1;
                left_node.merge(key, &mut right_node);
                // change the children's parent id
                for child_id in changed_children {
                    let (child_page, mut child) =
                        self.buffer_pool.fetch_page_node::<K>(child_id).await?;
                    child.set_parent(left_node.page_id());
                    child_page.write().await.write_back(&child)?;
                }
                if parent.header.size == 0 && self.root.eq(&parent.page_id()) {
                    //change root node
                    self.root = left_node.page_id();
                    left_node.header.parent = None;
                    self.buffer_pool
                        .encode_page_node(&Node::Internal(left_node))
                        .await?;
                    self.buffer_pool
                        .encode_page_node(&Node::Internal(right_node))
                        .await?;
                    return Ok(true);
                }
                self.buffer_pool
                    .encode_page_node(&Node::Internal(left_node))
                    .await?;
                self.buffer_pool
                    .encode_page_node(&Node::Internal(right_node))
                    .await?;
                self.buffer_pool
                    .encode_page_node(&Node::Internal(parent.clone()))
                    .await?;
                Ok(false)
            }
            Node::Leaf(leaf) => {
                let (mut left_node, mut right_node, right_index) = {
                    if let Some(prev_index) = prev {
                        let prev_id = parent.kv[prev_index].1;
                        let (_prev_page, prev_node) =
                            self.buffer_pool.fetch_page_node(prev_id).await?;
                        let prev_node = prev_node.assume_leaf();
                        (prev_node, leaf, index)
                    } else if let Some(next_index) = next {
                        let next_id = parent.kv[next_index].1;
                        let (_next_page, next_node) =
                            self.buffer_pool.fetch_page_node(next_id).await?;
                        let next_node = next_node.assume_leaf();
                        (leaf, next_node, next_index)
                    } else {
                        return Ok(true);
                    }
                };
                left_node.merge(&mut right_node);
                parent.kv.remove(right_index);
                parent.header.size -= 1;

                if parent.header.size == 0 && self.root.eq(&parent.page_id()) {
                    //change root node
                    self.root = left_node.page_id();
                    left_node.header.parent = None;
                    self.buffer_pool
                        .encode_page_node(&Node::Leaf(left_node))
                        .await?;
                    self.buffer_pool
                        .encode_page_node(&Node::Leaf(right_node))
                        .await?;
                    return Ok(true);
                }
                self.buffer_pool
                    .encode_page_node(&Node::Leaf(left_node))
                    .await?;
                self.buffer_pool
                    .encode_page_node(&Node::Leaf(right_node))
                    .await?;
                self.buffer_pool
                    .encode_page_node(&Node::Internal(parent.clone()))
                    .await?;

                Ok(false)
            }
        }
    }

    fn is_root<K>(&self, node: &Node<K>) -> bool {
        self.root.eq(&node.page_id())
    }

    async fn init_tree<K>(&mut self, key: K, value: RecordId) -> RustDBResult<()>
    where
        K: Encoder<Error = RustDBError>,
    {
        let page = self
            .buffer_pool
            .new_page_ref()
            .await?
            .ok_or(RustDBError::BufferPool("Can't new page".into()))?;
        let node = Node::Leaf(Leaf {
            header: Header {
                size: 1,
                max_size: self.max_size,
                parent: None,
                page_id: page.read().await.page_id(),
                next: None,
                prev: None,
            },
            kv: vec![(key, value)],
        });
        self.root = page.read().await.page_id();
        page.write().await.write_back(&node)?;
        self.init = true;
        Ok(())
    }

    async fn print<K>(&mut self) -> RustDBResult<()>
    where
        K: Decoder<Error = RustDBError> + Debug,
    {
        let mut pages = VecDeque::new();
        pages.push_back(self.root);
        loop {
            let len = pages.len();
            if len == 0 {
                break;
            }
            for _ in 0..len {
                let page_id = pages.pop_front().unwrap();
                let page = self
                    .buffer_pool
                    .fetch_page_ref(page_id)
                    .await?
                    .ok_or(RustDBError::BufferPool("Can't not fetch page".into()))?;
                let node: Node<K> = page.read().await.node()?;
                match node {
                    Node::Internal(internal) => {
                        print!(
                            "internal:{}[{:?}] ",
                            internal.page_id(),
                            internal.kv[1..].iter().map(|(k, _)| k).collect::<Vec<_>>()
                        );
                        for (_, page_id) in internal.kv.iter() {
                            pages.push_back(*page_id);
                        }
                    }
                    Node::Leaf(leaf) => {
                        print!(
                            "leaf:{}[{:?}] ",
                            leaf.page_id(),
                            leaf.kv.iter().map(|(k, _)| k).collect::<Vec<_>>()
                        );
                    }
                }
            }
            println!();
        }
        println!();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::disk_manager::DiskManager;

    #[tokio::test]
    async fn test_search_range() -> RustDBResult<()> {
        let db_name = "test_search_range.db";
        let disk_manager = DiskManager::new(db_name).await?;
        let buffer_pool_manager = BufferPoolManager::new(50, 2, disk_manager).await?;
        let mut index = Index {
            buffer_pool: buffer_pool_manager,
            root: 0,
            init: false,
            max_size: 100,
        };
        for i in (1..1000).rev() {
            index
                .insert(
                    i as u32,
                    RecordId {
                        page_id: i,
                        slot_num: 0,
                    },
                )
                .await?;
            // tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let range = index
            .search_range(Range {
                start: 1,
                end: 1000,
            })
            .await?;
        assert_eq!(range.len(), 999);
        for (index, record) in range.into_iter().enumerate() {
            assert_eq!(index + 1, record.page_id);
        }

        let range = index
            .search_range(Range {
                start: 801,
                end: 900,
            })
            .await?;
        assert_eq!(range.len(), 100);
        for (index, record) in range.into_iter().enumerate() {
            assert_eq!(index + 801, record.page_id);
        }

        let range = index
            .search_range(Range {
                start: 800,
                end: 1200,
            })
            .await?;
        assert_eq!(range.len(), 200);
        for (index, record) in range.into_iter().enumerate() {
            assert_eq!(index + 800, record.page_id);
        }

        let range = index
            .search_range(Range {
                start: 0,
                end: 1200,
            })
            .await?;
        assert_eq!(range.len(), 999);
        for (index, record) in range.into_iter().enumerate() {
            assert_eq!(index + 1, record.page_id);
        }
        tokio::fs::remove_file(db_name).await?;
        Ok(())
    }
    #[tokio::test]
    async fn test_insert() -> RustDBResult<()> {
        let db_name = "test_insert.db";
        let disk_manager = DiskManager::new(db_name).await?;
        let buffer_pool_manager = BufferPoolManager::new(50, 2, disk_manager).await?;
        let mut index = Index {
            buffer_pool: buffer_pool_manager,
            root: 0,
            init: false,
            max_size: 4,
        };
        for i in (1..100).rev() {
            index
                .insert(
                    i as u32,
                    RecordId {
                        page_id: i,
                        slot_num: 0,
                    },
                )
                .await?;
            // tokio::time::sleep(Duration::from_millis(100)).await;
            println!("insert: {}", i);
            index.print::<u32>().await?;
        }
        for i in 1..100 {
            let val = index.search(&i).await?;
            assert!(val.is_some());
            assert_eq!(i, val.unwrap().page_id as u32);
        }
        assert!(index.search(&101).await?.is_none());
        tokio::fs::remove_file(db_name).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> RustDBResult<()> {
        let db_name = "test_delete.db";
        let disk_manager = DiskManager::new(db_name).await?;
        let buffer_pool_manager = BufferPoolManager::new(100, 2, disk_manager).await?;
        let mut index = Index {
            buffer_pool: buffer_pool_manager,
            root: 0,
            init: false,
            max_size: 4,
        };
        let len = 100;
        for i in 1..len {
            index
                .insert(
                    i as u32,
                    RecordId {
                        page_id: i,
                        slot_num: 0,
                    },
                )
                .await?;
        }
        for i in (1..len).rev() {
            let val = index.delete(&(i as u32)).await?;
            assert!(val.is_some());
        }

        for i in 1..len {
            index
                .insert(
                    i as u32,
                    RecordId {
                        page_id: i,
                        slot_num: 0,
                    },
                )
                .await?;
        }
        index.print::<u32>().await?;
        for i in 1..len {
            let val = index.delete(&(i as u32)).await?;
            println!("delete: {}", i);
            assert!(val.is_some());
            index.print::<u32>().await?;
        }

        let val = index.search(&1).await?;
        println!("{:?}", val);
        tokio::fs::remove_file(db_name).await?;
        Ok(())
    }
}
