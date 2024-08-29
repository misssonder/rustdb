use crate::buffer;
use crate::buffer::buffer_pool_manager::{
    BufferPoolManager, OwnedPageDataReadGuard, OwnedPageDataWriteGuard,
};
use crate::encoding::{Decoder, Encoder};
use crate::storage::page::index::{Header, Internal, Leaf, Node};
use crate::storage::page::{PageEncoding, PageTrait};
use crate::storage::{PageId, RecordId, StorageResult};
use indexmap::IndexMap;
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::{Deref, RangeBounds};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// A concurrency BPlus Tree, use [`Latch`] to lock every node.
pub struct Index<K> {
    buffer_pool: Arc<BufferPoolManager>,
    root: RwLock<PageId>,
    max_size: usize,
    _data: PhantomData<K>,
}

impl<'a, K> Index<K> {
    pub async fn new(buffer_pool: Arc<BufferPoolManager>, max_size: usize) -> StorageResult<Self>
    where
        K: Encoder,
    {
        let mut node = Node::Leaf(Leaf::<K> {
            header: Header {
                size: 0,
                max_size,
                parent: None,
                page_id: 0,
                next: None,
                prev: None,
            },
            kv: Vec::new(),
        });
        buffer_pool.new_page_node(&mut node).await?;
        Ok(Self {
            buffer_pool,
            root: RwLock::new(node.page_id()),
            max_size,
            _data: Default::default(),
        })
    }
    pub async fn search(&self, key: &K) -> StorageResult<Option<RecordId>>
    where
        K: Decoder + Encoder + Ord,
    {
        let mut route = Route::new(RouteOption::default());
        let page_id = self
            .find_route(KeyCondition::Equal(key), &mut route)
            .await?;
        match route.nodes.get(&page_id).unwrap().latch {
            Latch::Read(ref guard) => {
                let leaf = guard.node::<K>()?.assume_leaf();
                Ok(leaf.search(key))
            }
            Latch::Write(ref _guard) => {
                unreachable!()
            }
        }
    }

    pub async fn search_range<'r, R>(&self, range: R) -> StorageResult<Vec<RecordId>>
    where
        K: Decoder + Encoder + Ord + 'r,
        R: RangeBounds<&'r K>,
    {
        let output = 'output: loop {
            let mut result = Vec::new();
            let mut route = Route::new(RouteOption::default());
            let mut excluded = Vec::with_capacity(2);
            if let Bound::Excluded(key) = range.start_bound() {
                excluded.push(key);
            }
            if let Bound::Excluded(key) = range.end_bound() {
                excluded.push(key);
            }
            let page_id = match range.start_bound() {
                Bound::Included(key) | Bound::Excluded(key) => {
                    self.find_route(KeyCondition::Equal(key), &mut route)
                        .await?
                }
                Bound::Unbounded => self.find_route(KeyCondition::Min, &mut route).await?,
            };
            let mut latch = route
                .nodes
                .shift_remove(&page_id)
                .unwrap()
                .latch
                .assume_read();
            'search: loop {
                let leaf = latch.node::<K>()?.assume_leaf();
                let start = match range.start_bound() {
                    Bound::Included(key) | Bound::Excluded(key) => {
                        leaf.kv.binary_search_by(|(k, _)| k.cmp(key))
                    }
                    Bound::Unbounded => Ok(0),
                };
                let end = match range.end_bound() {
                    Bound::Included(key) | Bound::Excluded(key) => {
                        leaf.kv.binary_search_by(|(k, _)| k.cmp(key))
                    }
                    Bound::Unbounded => Ok(leaf.kv.len() - 1),
                };
                match (start, end) {
                    (Ok(start_index), Ok(end_index)) => {
                        for (k, v) in leaf.kv[start_index..=end_index].iter() {
                            if !excluded.contains(&&k) {
                                result.push(*v);
                            }
                        }
                        if end_index < leaf.kv.len() - 1 {
                            break 'output Ok(result);
                        }
                    }
                    (Ok(start_index), Err(end_index)) => {
                        if end_index < leaf.kv.len() {
                            for (k, v) in leaf.kv[start_index..=end_index].iter() {
                                if !excluded.contains(&&k) {
                                    result.push(*v);
                                }
                            }
                            break 'output Ok(result);
                        } else {
                            for (k, v) in leaf.kv[start_index..].iter() {
                                if !excluded.contains(&&k) {
                                    result.push(*v);
                                }
                            }
                        }
                    }
                    (Err(start_index), Ok(end_index)) => {
                        for (k, v) in leaf.kv[start_index..=end_index].iter() {
                            if !excluded.contains(&&k) {
                                result.push(*v);
                            }
                        }
                        if end_index < leaf.kv.len() - 1 {
                            break 'output Ok(result);
                        }
                    }
                    (Err(start_index), Err(end_index)) => {
                        if end_index < leaf.kv.len() {
                            for (k, v) in leaf.kv[start_index..=end_index].iter() {
                                if !excluded.contains(&&k) {
                                    result.push(*v);
                                }
                            }
                            break 'output Ok(result);
                        } else if start_index < leaf.kv.len() {
                            for (k, v) in leaf.kv[start_index..].iter() {
                                if !excluded.contains(&&k) {
                                    result.push(*v);
                                }
                            }
                        } else {
                            break 'output Ok(result);
                        }
                    }
                }
                match leaf.next() {
                    None => break 'output Ok(result),
                    Some(next_id) => {
                        latch = match self.buffer_pool.try_fetch_page_read_owned(next_id).await {
                            Ok(latch) => latch,
                            Err(buffer::Error::TryLock(_)) => {
                                break 'search;
                            }
                            Err(err) => break 'output Err(err),
                        };
                    }
                }
            }
        }?;
        Ok(output)
    }

    pub async fn insert(&self, key: K, value: RecordId) -> StorageResult<()>
    where
        K: Decoder + Encoder + Ord + Default + Clone,
    {
        let option = RouteOption::default().with_action(RouteAction::Insert);
        let mut route = Route::new(option);
        let page_id = self
            .find_route(KeyCondition::Equal(&key), &mut route)
            .await?;
        self.insert_inner(page_id, route, key, value).await
    }

    pub async fn delete(&self, key: &K) -> StorageResult<Option<(K, RecordId)>>
    where
        K: Decoder + Encoder + Ord + Clone,
    {
        let option = RouteOption::default().with_action(RouteAction::Delete);
        let mut route = Route::new(option);
        let page_id = self
            .find_route(KeyCondition::Equal(key), &mut route)
            .await?;
        self.delete_inner(page_id, route, key).await
    }

    async fn insert_inner(
        &self,
        mut page_id: PageId,
        mut route: Route<'_>,
        key: K,
        value: RecordId,
    ) -> StorageResult<()>
    where
        K: Decoder + Encoder + Ord + Default + Clone,
    {
        loop {
            let mut latch = route
                .nodes
                .shift_remove(&page_id)
                .unwrap()
                .latch
                .assume_write();
            let mut node: Node<K> = latch.node()?;
            match node {
                Node::Internal(ref mut _internal) => {}
                Node::Leaf(ref mut leaf) => {
                    match leaf.kv.binary_search_by(|(k, _)| k.cmp(&key)) {
                        Ok(index) => leaf.kv[index] = (key.clone(), value),
                        Err(index) => leaf.insert(index, key.clone(), value),
                    };
                    latch.write_node_back(&node)?;
                }
            }
            if !node.is_overflow() {
                return Ok(());
            }
            let (median_key, mut sibling) = node.split();
            let mut sibling_latch = self.buffer_pool.new_page_write_owned(&mut sibling).await?;
            let sibling_page_id = sibling.page_id();
            if let Node::Internal(ref mut internal) = sibling {
                for (_, child) in internal.kv.iter() {
                    let mut child_latch = self.buffer_pool.fetch_page_write_owned(*child).await?;
                    let mut child_node = child_latch.node::<K>()?;
                    child_node.set_parent(sibling_page_id);
                    child_latch.write_node_back(&child_node)?;
                }
            }
            node.set_next(sibling.page_id());
            sibling.set_prev(node.page_id());
            if let Some(parent_id) = node.parent() {
                let parent_latch = route
                    .nodes
                    .get_mut(&parent_id)
                    .unwrap()
                    .latch
                    .assume_write_mut();
                let mut parent_node = parent_latch.node::<K>()?;
                let internal = parent_node.assume_internal_mut();
                let index = internal
                    .kv
                    .binary_search_by(|(k, _)| k.cmp(&median_key))
                    .unwrap_or_else(|index| index);
                internal.insert(index, median_key.clone(), sibling_page_id);

                parent_latch.write_node_back(&parent_node)?;
                sibling_latch.write_node_back(&sibling)?;
                latch.write_node_back(&node)?;
                page_id = parent_latch.page_id();
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
                let mut parent_latch = self
                    .buffer_pool
                    .new_page_write_owned(&mut parent_node)
                    .await?;
                let parent_id = parent_node.page_id();
                if let Some(root_latch) = route.root_latch.take() {
                    let mut root_latch = root_latch.assume_write();
                    *root_latch = parent_node.page_id();
                }
                node.set_parent(parent_node.page_id());
                sibling.set_parent(parent_node.page_id());

                parent_latch.write_node_back(&parent_node)?;
                sibling_latch.write_node_back(&sibling)?;
                latch.write_node_back(&node)?;
                route
                    .nodes
                    .insert(parent_id, RouteNode::new(Latch::Write(parent_latch), 1));
                page_id = parent_id;
            };
        }
    }

    async fn delete_inner(
        &self,
        mut page_id: PageId,
        mut route: Route<'_>,
        key: &K,
    ) -> StorageResult<Option<(K, RecordId)>>
    where
        K: Decoder + Encoder + Ord + Clone,
    {
        let mut res = None;
        loop {
            let route_node = route.nodes.shift_remove(&page_id).unwrap();
            let mut latch = route_node.latch.assume_write();
            let mut node: Node<K> = latch.node()?;
            match node {
                Node::Internal(ref mut _internal) => {}
                Node::Leaf(ref mut leaf) => {
                    res = match leaf.remove(key) {
                        None => return Ok(None),
                        other => other,
                    };
                }
            }
            latch.write_node_back(&node)?;
            if !node.is_underflow() {
                break;
            }
            match node.parent() {
                None => break,
                Some(parent_id) => {
                    let parent_latch = route
                        .nodes
                        .get_mut(&parent_id)
                        .unwrap()
                        .latch
                        .assume_write_mut();
                    if self
                        .steal(parent_latch, &mut latch, route_node.parent_index)
                        .await?
                        .is_some()
                    {
                        break;
                    }
                    if self
                        .merge(
                            parent_latch,
                            latch,
                            &mut route.root_latch,
                            route_node.parent_index,
                        )
                        .await?
                    {
                        break;
                    };
                    page_id = parent_id;
                }
            }
        }
        Ok(res)
    }

    /// Try to steal key-value from it's sibling node.
    /// If steal successfully, return [`Some`]
    /// else, return [`None`]
    async fn steal(
        &self,
        parent_latch: &mut OwnedPageDataWriteGuard,
        latch: &mut OwnedPageDataWriteGuard,
        index: usize,
    ) -> StorageResult<Option<()>>
    where
        K: Decoder + Encoder + Ord + Clone,
    {
        let mut parent: Internal<K> = parent_latch.node()?.assume_internal();
        let node: Node<K> = latch.node()?;
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
            Node::Internal(mut internal) => {
                if let Some(prev_index) = prev {
                    let prev_id = parent.kv[prev_index].1;
                    let (prev_page, prev_node) =
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
                        child_page.data_write().await.write_node_back(&child)?;
                        prev_page
                            .data_write()
                            .await
                            .write_node_back(&Node::Internal(prev_node))?;
                        latch.write_node_back(&Node::Internal(internal))?;
                        parent_latch.write_node_back(&Node::Internal(parent))?;
                        return Ok(Some(()));
                    }
                }
                if let Some(next_index) = next {
                    let next_id = parent.kv[next_index].1;
                    let (next_page, next_node) =
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
                        child_page.data_write().await.write_node_back(&child)?;
                        next_page
                            .data_write()
                            .await
                            .write_node_back(&Node::Internal(next_node))?;
                        latch.write_node_back(&Node::Internal(internal))?;
                        parent_latch.write_node_back(&Node::Internal(parent))?;
                        return Ok(Some(()));
                    }
                }
            }
            Node::Leaf(mut leaf) => {
                if let Some(prev_index) = prev {
                    let prev_id = parent.kv[prev_index].1;
                    let (prev_page, prev_node) =
                        self.buffer_pool.fetch_page_node::<K>(prev_id).await?;
                    let mut prev_node = prev_node.assume_leaf();
                    if let Some(steal) = prev_node.steal_last() {
                        let right_index = index;
                        parent.kv[right_index].0 = steal.0.clone();
                        let (key, value) = steal;
                        leaf.push_front(key, value);
                        prev_page
                            .data_write()
                            .await
                            .write_node_back(&Node::Leaf(prev_node))?;
                        latch.write_node_back(&Node::Leaf(leaf))?;
                        parent_latch.write_node_back(&Node::Internal(parent))?;
                        return Ok(Some(()));
                    }
                }
                if let Some(next_index) = next {
                    let next_id = parent.kv[next_index].1;
                    let (next_page, next_node) =
                        self.buffer_pool.fetch_page_node::<K>(next_id).await?;
                    let mut next_node = next_node.assume_leaf();
                    if let Some(steal) = next_node.steal_first() {
                        // steal from next node and change parent
                        let right_index = next_index;
                        parent.kv[right_index].0 = next_node.kv[0].0.clone();
                        let (key, value) = steal;
                        leaf.push_back(key, value);
                        next_page
                            .data_write()
                            .await
                            .write_node_back(&Node::Leaf(next_node))?;
                        latch.write_node_back(&Node::Leaf(leaf))?;
                        parent_latch.write_node_back(&Node::Internal(parent))?;
                        return Ok(Some(()));
                    }
                }
            }
        }

        Ok(None)
    }

    /// merge this node and it's prev node or next node
    /// return true if the node which been merged become the root
    async fn merge(
        &self,
        parent_latch: &mut OwnedPageDataWriteGuard,
        latch: OwnedPageDataWriteGuard,
        root_latch: &mut Option<RootLatch<'a>>,
        index: usize,
    ) -> StorageResult<bool>
    where
        K: Encoder + Decoder + Clone + Ord,
    {
        let mut parent: Internal<K> = parent_latch.node()?.assume_internal();
        let node: Node<K> = latch.node()?;
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
                let (mut left_latch, mut left_node, mut right_latch, mut right_node, right_index) = {
                    if let Some(prev_index) = prev {
                        let prev_id = parent.kv[prev_index].1;
                        let prev_latch = self.buffer_pool.fetch_page_write_owned(prev_id).await?;
                        let prev_node = prev_latch.node()?.assume_internal();
                        (prev_latch, prev_node, latch, internal, index)
                    } else if let Some(next_index) = next {
                        let next_id = parent.kv[next_index].1;
                        let next_latch = self.buffer_pool.fetch_page_write_owned(next_id).await?;
                        let next_node = next_latch.node()?.assume_internal();
                        (latch, internal, next_latch, next_node, next_index)
                    } else {
                        unreachable!()
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
                    child_page.data_write().await.write_node_back(&child)?;
                }
                if parent.header.size == 0 && parent.parent().is_none() {
                    //change root node
                    if let Some(root_latch) = root_latch.take() {
                        let mut root_latch = root_latch.assume_write();
                        *root_latch = left_node.page_id();
                    };
                    left_node.header.parent = None;
                    left_latch.write_node_back(&Node::Internal(left_node))?;
                    right_latch.write_node_back(&Node::Internal(right_node))?;
                    return Ok(true);
                }
                left_latch.write_node_back(&Node::Internal(left_node))?;
                right_latch.write_node_back(&Node::Internal(right_node))?;
                parent_latch.write_node_back(&Node::Internal(parent))?;
                Ok(false)
            }
            Node::Leaf(leaf) => {
                let (mut left_latch, mut left_node, mut right_latch, mut right_node, right_index) = {
                    if let Some(prev_index) = prev {
                        let prev_id = parent.kv[prev_index].1;
                        let prev_latch = self.buffer_pool.fetch_page_write_owned(prev_id).await?;
                        let prev_node = prev_latch.node()?.assume_leaf();
                        (prev_latch, prev_node, latch, leaf, index)
                    } else if let Some(next_index) = next {
                        let next_id = parent.kv[next_index].1;
                        let next_latch = self.buffer_pool.fetch_page_write_owned(next_id).await?;
                        let next_node = next_latch.node()?.assume_leaf();
                        (latch, leaf, next_latch, next_node, next_index)
                    } else {
                        unreachable!()
                    }
                };
                left_node.merge(&mut right_node);
                parent.kv.remove(right_index);
                parent.header.size -= 1;

                if parent.header.size == 0 && parent.parent().is_none() {
                    //change root node
                    if let Some(root_latch) = root_latch.take() {
                        let mut root_latch = root_latch.assume_write();
                        *root_latch = left_node.page_id();
                    };
                    left_node.header.parent = None;
                    left_latch.write_node_back(&Node::Leaf(left_node))?;
                    right_latch.write_node_back(&Node::Leaf(right_node))?;
                    return Ok(true);
                }
                left_latch.write_node_back(&Node::Leaf(left_node))?;
                right_latch.write_node_back(&Node::Leaf(right_node))?;
                parent_latch.write_node_back(&Node::Internal(parent))?;
                Ok(false)
            }
        }
    }

    /// Take latches according to latch crabbin
    /// If current node is safe, then release parent latch
    /// If current node is unsafe, then take parent latch
    async fn find_route(
        &'a self,
        key: KeyCondition<&K>,
        route: &mut Route<'a>,
    ) -> StorageResult<PageId>
    where
        K: Decoder + Encoder + Ord,
    {
        let root_latch = match route.option.action {
            RouteAction::Search => {
                let root_guard = self.root.read().await;
                RootLatch::Read(root_guard)
            }
            RouteAction::Insert | RouteAction::Delete => {
                let root_guard = self.root.write().await;
                RootLatch::Write(root_guard)
            }
        };
        let mut page_id = *root_latch;
        let mut parent_index = 0;
        let _ = route.root_latch.insert(root_latch);
        loop {
            let page = self
                .buffer_pool
                .fetch_page_ref(page_id)
                .await?
                .ok_or(buffer::Error::BufferInsufficient)?;
            let (latch, node) = match route.option.action {
                RouteAction::Search => {
                    let read_guard = page.data_read_owned().await;
                    let node = read_guard.node::<K>()?;
                    (Latch::Read(read_guard), node)
                }
                RouteAction::Insert | RouteAction::Delete => {
                    let write_guard = page.data_write_owned().await;
                    let node = write_guard.node::<K>()?;
                    (Latch::Write(write_guard), node)
                }
            };
            if node.parent().is_some() {
                match route.option.action {
                    RouteAction::Search => {
                        route.pop_front_until(page_id);
                    }
                    RouteAction::Insert => {
                        if node.allow_insert() {
                            route.pop_front_until(page_id);
                        }
                    }
                    RouteAction::Delete => {
                        if node.allow_delete() {
                            route.pop_front_until(page_id);
                        }
                    }
                }
            }
            match node {
                Node::Internal(ref internal) => {
                    let (index, child_id) = match key {
                        KeyCondition::Min => (0, internal.kv[0].1),
                        KeyCondition::Max => {
                            (internal.kv.len() - 1, internal.kv[internal.kv.len() - 1].1)
                        }
                        KeyCondition::Equal(key) => internal.search(key),
                    };
                    let node = RouteNode::new(latch, parent_index);
                    route.insert(page_id, node);
                    parent_index = index;
                    page_id = child_id;
                }
                Node::Leaf(_) => {
                    let node = RouteNode::new(latch, parent_index);
                    route.insert(page_id, node);
                    return Ok(page_id);
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn print(&self) -> StorageResult<()>
    where
        K: Decoder + std::fmt::Debug,
    {
        let mut pages = std::collections::VecDeque::new();
        let page_id = self.root.read().await;
        let page_id = *page_id;
        pages.push_back(page_id);
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
                    .ok_or(buffer::Error::BufferInsufficient)?;
                let node: Node<K> = page.data_read().await.node()?;
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

enum KeyCondition<K> {
    Min,
    Max,
    Equal(K),
}

struct Route<'a> {
    nodes: IndexMap<PageId, RouteNode>,
    root_latch: Option<RootLatch<'a>>,
    option: RouteOption,
}

impl Route<'_> {
    fn new(option: RouteOption) -> Self {
        Self {
            nodes: Default::default(),
            root_latch: None,
            option,
        }
    }

    fn pop_front_until(&mut self, page_id: PageId) {
        while let Some((first_page_id, _)) = self.nodes.first() {
            if first_page_id.eq(&page_id) {
                break;
            }
            self.root_latch.take();
            self.nodes.shift_remove_index(0);
        }
    }

    fn insert(&mut self, page_id: PageId, node: RouteNode) -> Option<RouteNode> {
        self.nodes.insert(page_id, node)
    }
}

enum RouteAction {
    Search,
    Insert,
    Delete,
}

struct RouteOption {
    action: RouteAction,
}

impl Default for RouteOption {
    fn default() -> Self {
        Self {
            action: RouteAction::Search,
        }
    }
}

impl RouteOption {
    fn with_action(mut self, action: RouteAction) -> Self {
        self.action = action;
        self
    }
}

struct RouteNode {
    latch: Latch,
    parent_index: usize,
}

impl RouteNode {
    fn new(latch: Latch, parent_index: usize) -> Self {
        RouteNode {
            latch,
            parent_index,
        }
    }
}

enum Latch {
    Read(OwnedPageDataReadGuard),
    Write(OwnedPageDataWriteGuard),
}

impl Latch {
    fn node<K>(&self) -> StorageResult<Node<K>>
    where
        K: Decoder,
    {
        match self {
            Latch::Read(guard) => Ok(guard.node()?),
            Latch::Write(guard) => Ok(guard.node()?),
        }
    }

    fn assume_write_mut(&mut self) -> &mut OwnedPageDataWriteGuard {
        match self {
            Latch::Read(_) => unreachable!(),
            Latch::Write(guard) => guard,
        }
    }

    fn assume_write(self) -> OwnedPageDataWriteGuard {
        match self {
            Latch::Read(_) => unreachable!(),
            Latch::Write(guard) => guard,
        }
    }

    fn assume_write_ref(&self) -> &OwnedPageDataWriteGuard {
        match self {
            Latch::Read(_) => unreachable!(),
            Latch::Write(guard) => guard,
        }
    }

    fn assume_read_ref(&self) -> &OwnedPageDataReadGuard {
        match self {
            Latch::Read(guard) => guard,
            Latch::Write(_) => unreachable!(),
        }
    }

    fn assume_read(self) -> OwnedPageDataReadGuard {
        match self {
            Latch::Read(guard) => guard,
            Latch::Write(_) => unreachable!(),
        }
    }
}

enum RootLatch<'a> {
    Read(RwLockReadGuard<'a, PageId>),
    Write(RwLockWriteGuard<'a, PageId>),
}

impl Deref for RootLatch<'_> {
    type Target = PageId;

    fn deref(&self) -> &Self::Target {
        match self {
            RootLatch::Read(guard) => guard.deref(),
            RootLatch::Write(guard) => guard.deref(),
        }
    }
}

impl<'a> RootLatch<'a> {
    fn assume_write(self) -> RwLockWriteGuard<'a, PageId> {
        match self {
            RootLatch::Read(_) => unreachable!(),
            RootLatch::Write(guard) => guard,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::disk_manager::DiskManager;
    use crate::storage::Error;
    use std::ops::RangeFull;

    async fn test_index() -> StorageResult<Index<u32>> {
        let f = tempfile::NamedTempFile::new()?;
        let disk_manager = DiskManager::new(f.path()).await?;
        let buffer_pool_manager = BufferPoolManager::new(100, 2, disk_manager).await?;
        let index = Index::new(Arc::new(buffer_pool_manager), 4).await?;
        Ok(index)
    }

    async fn insert_inner(index: &Index<u32>, keys: &[u32]) -> StorageResult<()> {
        for i in keys {
            index
                .insert(
                    *i,
                    RecordId {
                        page_id: *i as PageId,
                        slot_num: 0,
                    },
                )
                .await?;
            println!("insert: {}", i);
        }
        Ok(())
    }

    async fn insert_concurrency_inner(
        index: Arc<Index<u32>>,
        len: usize,
        concurrency: usize,
    ) -> StorageResult<()> {
        let mut tasks = Vec::with_capacity(concurrency);
        let limit = len / concurrency;
        for i in 0..concurrency {
            let start = i * limit;
            let end = start + limit;
            let index_clone = index.clone();
            let task = tokio::spawn(async move {
                for i in start..end {
                    index_clone
                        .insert(
                            i as u32,
                            RecordId {
                                page_id: i,
                                slot_num: 0,
                            },
                        )
                        .await?;
                }
                Ok::<_, Error>(())
            });
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap()?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn find_route() -> StorageResult<()> {
        let index = test_index().await?;
        let len = 10000;
        let keys = (0..len).collect::<Vec<_>>();
        insert_inner(&index, &keys).await?;
        let page_id = index
            .find_route(
                KeyCondition::<&u32>::Min,
                &mut Route::new(RouteOption::default()),
            )
            .await?;
        let (_, node) = index.buffer_pool.fetch_page_node::<u32>(page_id).await?;
        let node = node.assume_leaf();
        assert_eq!(node.kv[0].0, 0);
        let page_id = index
            .find_route(
                KeyCondition::<&u32>::Max,
                &mut Route::new(RouteOption::default()),
            )
            .await?;
        let (_, node) = index.buffer_pool.fetch_page_node::<u32>(page_id).await?;
        let node = node.assume_leaf();
        assert_eq!(node.kv[node.kv.len() - 1].0, len - 1);
        Ok(())
    }
    #[tokio::test]
    async fn search_range() -> StorageResult<()> {
        let index = test_index().await?;
        let keys = (1..1000).collect::<Vec<_>>();
        insert_inner(&index, &keys.iter().rev().copied().collect::<Vec<_>>()).await?;
        let range = index
            .search_range((Bound::Unbounded, Bound::Included(&1000)))
            .await?;
        assert_eq!(range.len(), 999);
        let range = index.search_range(&100..).await?;
        assert_eq!(range.len(), 900);
        let range = index.search_range(..=&800).await?;
        assert_eq!(range.len(), 800);
        let range = index.search_range::<_>(RangeFull).await?;
        assert_eq!(range.len(), 999);
        let range = index.search_range(&0..&900).await?;
        assert_eq!(range.len(), 899);
        let range = index
            .search_range((Bound::Excluded(&100), Bound::Included(&1000)))
            .await?;
        assert_eq!(range.len(), 899);
        let range = index.search_range(&1..=&1000).await?;
        assert_eq!(range.len(), 999);
        for (index, record) in range.into_iter().enumerate() {
            assert_eq!(index + 1, record.page_id);
        }

        let range = index.search_range(&801..=&900).await?;
        for (index, record) in range.into_iter().enumerate() {
            assert_eq!(index + 801, record.page_id);
        }

        let range = index.search_range(&800..=&1200).await?;
        assert_eq!(range.len(), 200);
        for (index, record) in range.into_iter().enumerate() {
            assert_eq!(index + 800, record.page_id);
        }

        let range = index.search_range(&0..=&1200).await?;
        assert_eq!(range.len(), 999);
        for (index, record) in range.into_iter().enumerate() {
            assert_eq!(index + 1, record.page_id);
        }
        Ok(())
    }
    #[tokio::test]
    async fn insert() -> StorageResult<()> {
        let keys: Vec<u32> = (1..100).collect::<Vec<_>>();
        let index = test_index().await?;
        insert_inner(&index, &keys.iter().copied().rev().collect::<Vec<_>>()).await?;
        for i in keys {
            let val = index.search(&i).await?;
            assert!(val.is_some());
            assert_eq!(i, val.unwrap().page_id as u32);
        }
        assert!(index.search(&101).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn delete() -> StorageResult<()> {
        let keys: Vec<u32> = (1..100).collect::<Vec<_>>();
        let index = test_index().await?;
        insert_inner(&index, &keys.iter().copied().rev().collect::<Vec<_>>()).await?;
        for key in keys.iter().rev() {
            println!("delete: {}", key);
            index.print().await?;
            let val = index.delete(key).await?;
            assert!(val.is_some());
        }
        insert_inner(&index, &keys).await?;
        index.print().await?;
        for i in keys {
            let val = index.delete(&(i)).await?;
            println!("delete: {}", i);
            assert!(val.is_some());
            index.print().await?;
        }

        let val = index.search(&1).await?;
        println!("{:?}", val);
        Ok(())
    }

    #[tokio::test]
    async fn search_concurrency() -> StorageResult<()> {
        let index = Arc::new(test_index().await?);
        let len = 10000;
        let concurrency = 1;
        insert_concurrency_inner(index.clone(), len, concurrency).await?;
        let mut tasks = Vec::with_capacity(concurrency);
        let limit = len / concurrency;
        for i in 0..concurrency {
            let start = i * limit;
            let end = start + limit;
            let index_clone = index.clone();
            let task = tokio::spawn(async move {
                for i in start..end {
                    let val = index_clone.search(&(i as u32)).await?;
                    assert!(val.is_some());
                    assert_eq!(i as u32, val.unwrap().page_id as u32);
                }
                Ok::<_, Error>(())
            });
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap()?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn search_range_concurrency() -> StorageResult<()> {
        let len = 1000;
        let concurrency = 10;
        let index = Arc::new(test_index().await?);
        insert_concurrency_inner(index.clone(), len, concurrency).await?;
        let mut search_tasks = Vec::with_capacity(concurrency);
        let limit = len / concurrency;
        for i in 0..concurrency {
            let start = i * limit;
            let end = start + limit;
            let index_clone = index.clone();
            let task = tokio::spawn(async move {
                for _ in start..end {
                    let val = index_clone.search_range(&0..=&(len as u32)).await?;
                    assert!(!val.is_empty());
                }
                Ok::<_, Error>(())
            });
            search_tasks.push(task);
        }
        for task in search_tasks {
            task.await.unwrap()?;
        }

        for i in 0..len {
            let val = index.search(&(i as u32)).await?;
            assert!(val.is_some());
            assert_eq!(i as u32, val.unwrap().page_id as u32);
        }
        assert!(index.search(&(len as u32 + 1)).await?.is_none());
        Ok(())
    }
    #[tokio::test]
    async fn insert_concurrency() -> StorageResult<()> {
        let len = 10000;
        let concurrency = 10;
        let index = Arc::new(test_index().await?);
        insert_concurrency_inner(index.clone(), len, concurrency).await?;

        for i in 0..len {
            let val = index.search(&(i as u32)).await?;
            assert!(val.is_some());
            assert_eq!(i as u32, val.unwrap().page_id as u32);
        }
        assert!(index.search(&(len as u32 + 1)).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn delete_concurrency() -> StorageResult<()> {
        let len = 10000;
        let concurrency = 10;
        let index = Arc::new(test_index().await?);
        insert_concurrency_inner(index.clone(), len, concurrency).await?;
        let mut tasks = Vec::with_capacity(concurrency);
        let limit = len / concurrency;

        for i in 0..concurrency {
            let start = i * limit;
            let end = start + limit;
            let index_clone = index.clone();
            let task = tokio::spawn(async move {
                for i in start..end {
                    let val = index_clone.delete(&(i as u32)).await?;
                    assert!(val.is_some());
                    assert_eq!(val.unwrap().1.page_id, i);
                }
                Ok::<_, Error>(())
            });
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap()?;
        }
        index.print().await?;
        for i in 0..len {
            let val = index.search(&(i as u32)).await?;
            assert!(val.is_none());
        }
        insert_concurrency_inner(index.clone(), len, concurrency).await?;
        for i in 0..len {
            let val = index.search(&(i as u32)).await?;
            println!("{}", index.root.read().await);
            assert!(val.is_some());
        }
        Ok(())
    }

    #[tokio::test]
    async fn multiple_index() -> StorageResult<()> {
        let f = tempfile::NamedTempFile::new()?;
        let disk_manager = DiskManager::new(f.path()).await?;
        let buffer_pool_manager = Arc::new(BufferPoolManager::new(100, 2, disk_manager).await?);
        let index1 = Index::new(buffer_pool_manager.clone(), 128).await?;
        let index2 = Index::new(buffer_pool_manager.clone(), 128).await?;
        let keys: Vec<u32> = (1..100).collect::<Vec<_>>();
        insert_inner(&index1, &keys.iter().copied().rev().collect::<Vec<_>>()).await?;
        for i in &keys {
            let val = index1.search(i).await?;
            assert!(val.is_some());
            assert_eq!(*i, val.unwrap().page_id as u32);
        }
        assert!(index1.search(&101).await?.is_none());

        insert_inner(&index2, &keys.iter().copied().rev().collect::<Vec<_>>()).await?;
        for i in &keys {
            let val = index2.search(i).await?;
            assert!(val.is_some());
            assert_eq!(*i, val.unwrap().page_id as u32);
        }
        assert!(index2.search(&101).await?.is_none());
        Ok(())
    }
}
