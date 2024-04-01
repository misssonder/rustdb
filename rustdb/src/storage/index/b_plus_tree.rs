use crate::buffer::buffer_poll_manager::BufferPoolManager;
use crate::error::{RustDBError, RustDBResult};
use crate::storage::codec::{Decoder, Encoder};
use crate::storage::page::b_plus_tree::{Header, Internal, Leaf, Node};
use crate::storage::{PageId, RecordId};
use std::collections::VecDeque;
use std::fmt::Debug;

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
                        Ok(index) => leaf.kv[index] = (key.clone(), value.clone()),
                        Err(index) => leaf.insert(index, key.clone(), value.clone()),
                    };
                    self.buffer_pool.encode_page_node(&node).await?;
                }
            }
            if !node.is_overflow() {
                return Ok(());
            }
            let (median_key, mut sibling) = node.split();
            self.buffer_pool.new_page_encode(&mut sibling).await?;
            let sibling_page_id = sibling.page_id();
            if let Node::Internal(ref mut internal) = sibling {
                for (_, child) in internal.kv.iter() {
                    let mut child_node: Node<K> = self.buffer_pool.fetch_page_node(*child).await?;
                    child_node.set_parent(sibling_page_id);
                    self.buffer_pool.encode_page_node(&child_node).await?;
                }
            }
            node.set_next(sibling.page_id());
            sibling.set_prev(node.page_id());
            let parent_node = if let Some(parent_id) = node.parent() {
                let mut parent_node: Node<K> = self.buffer_pool.fetch_page_node(parent_id).await?;
                let internal = parent_node.assume_internal_mut();
                let index = internal
                    .kv
                    .binary_search_by(|(k, _)| k.cmp(&median_key))
                    .unwrap_or_else(|index| index);
                internal.insert(index, median_key.clone(), sibling_page_id);
                self.buffer_pool.encode_page_node(&parent_node).await?;
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
                self.buffer_pool.new_page_encode(&mut parent_node).await?;
                self.root = parent_node.page_id();
                node.set_parent(parent_node.page_id());
                sibling.set_parent(parent_node.page_id());
                parent_node
            };
            self.buffer_pool.encode_page_node(&node).await?;
            self.buffer_pool.encode_page_node(&sibling).await?;
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
                    let mut parent: Internal<K> = self
                        .buffer_pool
                        .fetch_page_node(parent_id)
                        .await?
                        .assume_internal();

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
                    let mut prev_page: Internal<K> = self
                        .buffer_pool
                        .fetch_page_node(prev_id)
                        .await?
                        .assume_internal();
                    if let Some(steal) = prev_page.steal_last() {
                        // steal from prev node and change parent
                        let right_index = index;
                        internal.push_front(parent.kv[right_index].0.clone(), steal.1);
                        parent.kv[right_index].0 = steal.0;
                        // change child parent pointer
                        let mut child: Node<K> = self.buffer_pool.fetch_page_node(steal.1).await?;
                        child.set_parent(internal.page_id());
                        self.buffer_pool.encode_page_node(&child).await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(prev_page))
                            .await?;
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
                    let mut next_page: Internal<K> = self
                        .buffer_pool
                        .fetch_page_node(next_id)
                        .await?
                        .assume_internal();
                    if let Some(steal) = next_page.steal_first() {
                        // steal from next node and change parent
                        let right_index = next_index;
                        internal.push_back(parent.kv[right_index].0.clone(), steal.1);
                        parent.kv[right_index].0 = steal.0;
                        // change child parent pointer
                        let mut child: Node<K> = self.buffer_pool.fetch_page_node(steal.1).await?;
                        child.set_parent(internal.page_id());
                        self.buffer_pool.encode_page_node(&child).await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(next_page))
                            .await?;
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
                    let mut prev_page: Leaf<K> = self
                        .buffer_pool
                        .fetch_page_node(prev_id)
                        .await?
                        .assume_leaf();
                    if let Some(steal) = prev_page.steal_last() {
                        let right_index = index;
                        parent.kv[right_index].0 = steal.0.clone();
                        let (key, value) = steal;
                        leaf.push_front(key, value);
                        self.buffer_pool
                            .encode_page_node(&Node::Leaf(prev_page))
                            .await?;
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
                    let mut next_page: Leaf<K> = self
                        .buffer_pool
                        .fetch_page_node(next_id)
                        .await?
                        .assume_leaf();
                    if let Some(steal) = next_page.steal_first() {
                        // steal from next node and change parent
                        let right_index = next_index;
                        parent.kv[right_index].0 = next_page.kv[0].0.clone();
                        let (key, value) = steal;
                        leaf.push_back(key, value);
                        self.buffer_pool
                            .encode_page_node(&Node::Leaf(next_page))
                            .await?;
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
        let mut route = Vec::new();
        loop {
            let node: Node<K> = self.buffer_pool.fetch_page_node(page_id).await?;
            route.push(node.page_id());
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
                        let prev_node: Node<K> = self.buffer_pool.fetch_page_node(prev_id).await?;
                        let prev_node = prev_node.assume_internal();
                        (prev_node, internal, index)
                    } else if let Some(next_index) = next {
                        let next_id = parent.kv[next_index].1;
                        let next_node: Node<K> = self.buffer_pool.fetch_page_node(next_id).await?;
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
                    let mut child: Node<K> = self.buffer_pool.fetch_page_node(child_id).await?;
                    child.set_parent(left_node.page_id());
                    self.buffer_pool.encode_page_node(&child).await?;
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
                        let prev_node: Node<K> = self.buffer_pool.fetch_page_node(prev_id).await?;
                        let prev_node = prev_node.assume_leaf();
                        (prev_node, leaf, index)
                    } else if let Some(next_index) = next {
                        let next_id = parent.kv[next_index].1;
                        let next_node: Node<K> = self.buffer_pool.fetch_page_node(next_id).await?;
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
    use std::time::Duration;
    use super::*;
    use crate::storage::disk::disk_manager::DiskManager;

    #[tokio::test]
    async fn test_insert() -> RustDBResult<()> {
        let db_name = "test_insert.db";
        let disk_manager = DiskManager::new(db_name).await?;
        let buffer_pool_manager = BufferPoolManager::new(30, 2, disk_manager).await?;
        let mut index = Index {
            buffer_pool: buffer_pool_manager,
            root: 0,
            init: false,
            max_size: 4,
        };
        for i in 1..100 {
            index
                .insert(
                    i as u32,
                    RecordId {
                        page_id: i,
                        slot_num: 0,
                    },
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
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
        index.print::<u32>().await?;
        for i in (1..len).rev() {
            let val = index.delete(&(i as u32)).await?;
            assert!(val.is_some());
            println!("delete: {}", i);
            index.print::<u32>().await?;
        }

        let val = index.search(&1).await?;
        println!("{:?}", val);
        tokio::fs::remove_file(db_name).await?;
        Ok(())
    }
}
