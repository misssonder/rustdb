use crate::buffer::buffer_poll_manager1::{
    BufferPoolManager, NodeTrait, OwnedPageDataReadGuard, OwnedPageDataWriteGuard, PageRef,
};
use crate::error::{RustDBError, RustDBResult};
use crate::storage::codec::{Decoder, Encoder};
use crate::storage::page::b_plus_tree::{Header, Internal, Leaf, Node};
use crate::storage::{AtomicPageId, PageId, RecordId};
use indexmap::IndexMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::atomic::Ordering;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

pub struct Index {
    buffer_pool: BufferPoolManager,
    root: RwLock<Option<AtomicPageId>>,
    max_size: usize,
}

impl Index {
    pub async fn search<K>(&self, key: &K) -> RustDBResult<Option<RecordId>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Debug,
    {
        let mut route = Route::new(RouteOption::default());
        if let Some(page_id) = self.find_route(key, &mut route).await? {
            match route.nodes.get(&page_id).unwrap().latch {
                Latch::Read(ref guard) => {
                    let leaf = guard.node::<K>()?.assume_leaf();
                    return Ok(leaf.search(key));
                }
                Latch::Write(ref guard) => {
                    unreachable!()
                }
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
        if let Some(Node::Leaf(mut leaf)) = self.find_leaf(&range.start).await? {
            loop {
                let start = leaf.kv.binary_search_by(|(k, _)| k.cmp(&range.start));
                let end = leaf.kv.binary_search_by(|(k, _)| k.cmp(&range.end));
                match (start, end) {
                    (Ok(start_index), Ok(end_index)) => {
                        for (_, v) in leaf.kv[start_index..=end_index].iter() {
                            result.push(*v);
                        }
                        if end_index < leaf.kv.len() {
                            break;
                        }
                    }
                    (Ok(start_index), Err(end_index)) => {
                        if end_index < leaf.kv.len() {
                            for (_, v) in leaf.kv[start_index..=end_index].iter() {
                                result.push(*v);
                            }
                            break;
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
                        if end_index < leaf.kv.len() {
                            break;
                        }
                    }
                    (Err(start_index), Err(end_index)) => {
                        if end_index < leaf.kv.len() {
                            for (_, v) in leaf.kv[start_index..=end_index].iter() {
                                result.push(*v);
                            }
                            break;
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

    pub async fn insert<K>(&self, key: K, value: RecordId) -> RustDBResult<()>
    where
        K: Decoder<Error = RustDBError>
            + Encoder<Error = RustDBError>
            + Ord
            + Default
            + Clone
            + Debug,
    {
        let option = RouteOption::default()
            .with_internal(true)
            .with_leaf(true)
            .with_action(RouteAction::Insert);
        let mut route = Route::new(option);
        if let Some(page_id) = self.find_route(&key, &mut route).await? {
            return self.insert_inner(page_id, route, key, value).await;
        } else {
            println!("init tree");
            self.init_tree(key, value).await?;
        };
        Ok(())
    }

    pub async fn delete<K>(&mut self, key: &K) -> RustDBResult<Option<(K, RecordId)>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Clone + Default,
    {
        if self.root.read().await.is_none() {
            return Ok(None);
        }
        if let Some(page) = self.find_page_leaf(key).await? {
            return self.delete_inner(page, key).await;
        };
        Ok(None)
    }

    pub async fn insert_inner<K>(
        &self,
        mut page_id: PageId,
        mut route: Route,
        key: K,
        value: RecordId,
    ) -> RustDBResult<()>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Default + Clone,
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
                    latch.write_back(&node)?;
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
                    child_latch.write_back(&child_node)?;
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

                parent_latch.write_back(&parent_node)?;
                sibling_latch.write_back(&sibling)?;
                latch.write_back(&node)?;
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
                let root = self.root.read().await;
                root.as_ref()
                    .unwrap()
                    .store(parent_node.page_id(), Ordering::Relaxed);
                node.set_parent(parent_node.page_id());
                sibling.set_parent(parent_node.page_id());

                parent_latch.write_back(&parent_node)?;
                sibling_latch.write_back(&sibling)?;
                latch.write_back(&node)?;
                route
                    .nodes
                    .insert(parent_id, RouteNode::new(Latch::Write(parent_latch), 1));
                page_id = parent_id;
            };
        }
    }

    pub async fn delete_inner<K>(
        &mut self,
        mut page: PageRef,
        key: &K,
    ) -> RustDBResult<Option<(K, RecordId)>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Default + Clone,
    {
        let mut res = None;
        loop {
            let mut node = page.data_read().await.node::<K>()?;
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
                page.data_write().await.write_back(&node)?;
                break;
            }
            match node.parent() {
                None => break,
                Some(parent_id) => {
                    let (parent_page, parent) =
                        self.buffer_pool.fetch_page_node::<K>(parent_id).await?;
                    let parent = parent.assume_internal();
                    let (index, _) = parent.search(key);
                    if self.steal::<K>(&parent_page, &page, index).await?.is_some() {
                        break;
                    }
                    if self.merge::<K>(&parent_page, page, index).await? {
                        break;
                    };

                    page = parent_page;
                }
            }
        }
        Ok(res)
    }

    pub async fn steal<K>(
        &mut self,
        parent_page: &PageRef,
        page: &PageRef,
        index: usize,
    ) -> RustDBResult<Option<()>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Default + Clone,
    {
        let mut parent: Internal<K> = parent_page.data_read().await.node()?.assume_internal();
        let node: Node<K> = page.data_read().await.node()?;
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
                        child_page.data_write().await.write_back(&child)?;
                        prev_page
                            .data_write()
                            .await
                            .write_back(&Node::Internal(prev_node))?;
                        page.data_write()
                            .await
                            .write_back(&Node::Internal(internal))?;
                        parent_page
                            .data_write()
                            .await
                            .write_back(&Node::Internal(parent))?;
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
                        child_page.data_write().await.write_back(&child)?;
                        next_page
                            .data_write()
                            .await
                            .write_back(&Node::Internal(next_node))?;
                        page.data_write()
                            .await
                            .write_back(&Node::Internal(internal))?;
                        parent_page
                            .data_write()
                            .await
                            .write_back(&Node::Internal(parent))?;
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
                            .write_back(&Node::Leaf(prev_node))?;
                        page.data_write().await.write_back(&Node::Leaf(leaf))?;
                        parent_page
                            .data_write()
                            .await
                            .write_back(&Node::Internal(parent))?;
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
                            .write_back(&Node::Leaf(next_node))?;
                        page.data_write().await.write_back(&Node::Leaf(leaf))?;
                        parent_page
                            .data_write()
                            .await
                            .write_back(&Node::Internal(parent))?;
                        return Ok(Some(()));
                    }
                }
            }
        }

        Ok(None)
    }

    /// merge this node and it's prev node or next node
    /// return true if the node which been merged become the root
    pub async fn merge<K>(
        &mut self,
        parent_page: &PageRef,
        page: PageRef,
        index: usize,
    ) -> RustDBResult<bool>
    where
        K: Encoder<Error = RustDBError> + Decoder<Error = RustDBError> + Clone + Ord,
    {
        let mut parent: Internal<K> = parent_page.data_read().await.node()?.assume_internal();
        let node: Node<K> = page.data_read().await.node()?;
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
                let (left_page, mut left_node, right_page, mut right_node, right_index) = {
                    if let Some(prev_index) = prev {
                        let prev_id = parent.kv[prev_index].1;
                        let (prev_page, prev_node) =
                            self.buffer_pool.fetch_page_node(prev_id).await?;
                        let prev_node = prev_node.assume_internal();
                        (prev_page, prev_node, page, internal, index)
                    } else if let Some(next_index) = next {
                        let next_id = parent.kv[next_index].1;
                        let (next_page, next_node) =
                            self.buffer_pool.fetch_page_node(next_id).await?;
                        let next_node = next_node.assume_internal();
                        (page, internal, next_page, next_node, next_index)
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
                    child_page.data_write().await.write_back(&child)?;
                }
                if parent.header.size == 0 && parent.parent().is_none() {
                    //change root node
                    let root = self.root.read().await;
                    root.as_ref()
                        .unwrap()
                        .store(left_node.page_id(), Ordering::Relaxed);
                    left_node.header.parent = None;
                    left_page
                        .data_write()
                        .await
                        .write_back(&Node::Internal(left_node))?;
                    right_page
                        .data_write()
                        .await
                        .write_back(&Node::Internal(right_node))?;
                    return Ok(true);
                }
                left_page
                    .data_write()
                    .await
                    .write_back(&Node::Internal(left_node))?;
                right_page
                    .data_write()
                    .await
                    .write_back(&Node::Internal(right_node))?;
                parent_page
                    .data_write()
                    .await
                    .write_back(&Node::Internal(parent))?;
                Ok(false)
            }
            Node::Leaf(leaf) => {
                let (left_page, mut left_node, right_page, mut right_node, right_index) = {
                    if let Some(prev_index) = prev {
                        let prev_id = parent.kv[prev_index].1;
                        let (prev_page, prev_node) =
                            self.buffer_pool.fetch_page_node(prev_id).await?;
                        let prev_node = prev_node.assume_leaf();
                        (prev_page, prev_node, page, leaf, index)
                    } else if let Some(next_index) = next {
                        let next_id = parent.kv[next_index].1;
                        let (next_page, next_node) =
                            self.buffer_pool.fetch_page_node(next_id).await?;
                        let next_node = next_node.assume_leaf();
                        (page, leaf, next_page, next_node, next_index)
                    } else {
                        unreachable!()
                    }
                };
                left_node.merge(&mut right_node);
                parent.kv.remove(right_index);
                parent.header.size -= 1;

                if parent.header.size == 0 && parent.parent().is_none() {
                    //change root node
                    let root = self.root.read().await;
                    root.as_ref()
                        .unwrap()
                        .store(left_node.page_id(), Ordering::Relaxed);
                    left_node.header.parent = None;
                    left_page
                        .data_write()
                        .await
                        .write_back(&Node::Leaf(left_node))?;
                    right_page
                        .data_write()
                        .await
                        .write_back(&Node::Leaf(right_node))?;
                    return Ok(true);
                }
                left_page
                    .data_write()
                    .await
                    .write_back(&Node::Leaf(left_node))?;
                right_page
                    .data_write()
                    .await
                    .write_back(&Node::Leaf(right_node))?;
                parent_page
                    .data_write()
                    .await
                    .write_back(&Node::Internal(parent))?;
                Ok(false)
            }
        }
    }

    async fn find_page_leaf<K>(&mut self, key: &K) -> RustDBResult<Option<PageRef>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord,
    {
        if let Some(page_id) = self.root.read().await.as_ref() {
            let mut page_id = page_id.load(Ordering::Relaxed);
            loop {
                let (page, node) = self.buffer_pool.fetch_page_node(page_id).await?;
                match node {
                    Node::Internal(ref internal) => {
                        page_id = internal.search(key).1;
                    }
                    Node::Leaf(_) => {
                        return Ok(Some(page));
                    }
                }
            }
        };
        Ok(None)
    }

    /// take latches according to latch crabbin
    /// if current node is safe, then release parent latch
    /// if current node is unsafe, then take parent latch
    async fn find_route<K>(&self, key: &K, route: &mut Route) -> RustDBResult<Option<PageId>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Debug,
    {
        if let Some(page_id) = self.root.read().await.as_ref() {
            let mut page_id = page_id.load(Ordering::Relaxed);
            let mut parent_index = 0;
            loop {
                println!(
                    "key {:?} already get route: {:?} try to get: {:?}",
                    key,
                    route.nodes.keys().collect::<Vec<_>>(),
                    page_id
                );
                let page = self
                    .buffer_pool
                    .fetch_page_ref(page_id)
                    .await?
                    .ok_or(RustDBError::BufferPool("Can't fetch page".into()))?;
                println!("key {:?} try to get {}", key, page_id);
                let read_guard = page.data_read().await;
                println!("key {:?} already get {}", key, read_guard.page_id());
                let node = read_guard.node::<K>()?;
                if let Some(parent_id) = node.parent() {
                    match route.option.action {
                        RouteAction::Search => {
                            route.nodes.shift_remove(&parent_id);
                        }
                        RouteAction::Insert => {
                            if node.allow_insert() {
                                route.nodes.shift_remove(&parent_id);
                            }
                        }
                        RouteAction::Delete => {
                            if node.allow_delete() {
                                route.nodes.shift_remove(&parent_id);
                            }
                        }
                    }
                }
                match node {
                    Node::Internal(ref internal) => {
                        let (index, child_id) = internal.search(key);
                        drop(read_guard);
                        let latch = if route.option.internal_latch_write {
                            let write_guard = page.data_write_owned().await;
                            Latch::Write(write_guard)
                        } else {
                            let read_guard = page.data_read_owned().await;
                            Latch::Read(read_guard)
                        };
                        let node = RouteNode::new(latch, parent_index);
                        route.nodes.insert(page_id, node);
                        parent_index = index;
                        page_id = child_id;
                    }
                    Node::Leaf(_) => {
                        drop(read_guard);
                        if route.option.leaf_latch_write {
                            let write_guard = page.data_write_owned().await;
                            let node = RouteNode::new(Latch::Write(write_guard), parent_index);
                            route.nodes.insert(page_id, node);
                        } else {
                            let read_guard = page.data_read_owned().await;
                            let node = RouteNode::new(Latch::Read(read_guard), parent_index);
                            route.nodes.insert(page_id, node);
                        }
                        return Ok(Some(page_id));
                    }
                }
            }
        };
        Ok(None)
    }
    async fn find_leaf<K>(&mut self, key: &K) -> RustDBResult<Option<Node<K>>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord,
    {
        if let Some(page_id) = self.root.read().await.as_ref() {
            let mut page_id = page_id.load(Ordering::Relaxed);
            loop {
                let (_, node) = self.buffer_pool.fetch_page_node(page_id).await?;
                match node {
                    Node::Internal(ref internal) => {
                        page_id = internal.search(key).1;
                    }
                    Node::Leaf(leaf) => {
                        return Ok(Some(Node::Leaf(leaf)));
                    }
                }
            }
        };
        Ok(None)
    }

    async fn init_tree<K>(&self, key: K, value: RecordId) -> RustDBResult<()>
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
                page_id: page.page_id(),
                next: None,
                prev: None,
            },
            kv: vec![(key, value)],
        });
        let _ = self
            .root
            .write()
            .await
            .insert(AtomicPageId::new(page.page_id()));
        page.data_write().await.write_back(&node)?;
        Ok(())
    }

    async fn print<K>(&self) -> RustDBResult<()>
    where
        K: Decoder<Error = RustDBError> + Debug,
    {
        let mut pages = VecDeque::new();
        if let Some(page_id) = self.root.read().await.as_ref() {
            let page_id = page_id.load(Ordering::Relaxed);
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
                        .ok_or(RustDBError::BufferPool("Can't not fetch page".into()))?;
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
        }

        Ok(())
    }
}

struct Route {
    nodes: IndexMap<PageId, RouteNode>,
    option: RouteOption,
}

impl Route {
    fn new(option: RouteOption) -> Self {
        Self {
            nodes: Default::default(),
            option,
        }
    }
}

enum RouteAction {
    Search,
    Insert,
    Delete,
}

struct RouteOption {
    internal_latch_write: bool,
    leaf_latch_write: bool,
    action: RouteAction,
}

impl Default for RouteOption {
    fn default() -> Self {
        Self {
            internal_latch_write: false,
            leaf_latch_write: false,
            action: RouteAction::Search,
        }
    }
}

impl RouteOption {
    fn with_internal(mut self, write: bool) -> Self {
        self.internal_latch_write = write;
        self
    }

    fn with_leaf(mut self, write: bool) -> Self {
        self.leaf_latch_write = write;
        self
    }

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
    fn node<K>(&self) -> RustDBResult<Node<K>>
    where
        K: Decoder<Error = RustDBError>,
    {
        match self {
            Latch::Read(guard) => guard.node(),
            Latch::Write(guard) => guard.node(),
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

    fn assume_read(&self) -> &OwnedPageDataReadGuard {
        match self {
            Latch::Read(guard) => guard,
            Latch::Write(guard) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::disk_manager::DiskManager;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_search_range() -> RustDBResult<()> {
        let db_name = "test_search_range.db";
        let disk_manager = DiskManager::new(db_name).await?;
        let buffer_pool_manager = BufferPoolManager::new(50, 2, disk_manager).await?;
        let mut index = Index {
            buffer_pool: buffer_pool_manager,
            root: RwLock::new(None),
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
        println!("{:?}", range);
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
        let index = Index {
            buffer_pool: buffer_pool_manager,
            root: RwLock::new(None),
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
            root: RwLock::new(None),
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
            println!("delete: {}", i);
            index.print::<u32>().await?;
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

    #[tokio::test]

    async fn test_search_concurrency() -> RustDBResult<()> {
        let db_name = "test_search_concurrency.db";
        let disk_manager = DiskManager::new(db_name).await?;
        let buffer_pool_manager = BufferPoolManager::new(50, 2, disk_manager).await?;
        let len = 100;
        let index = Arc::new(Index {
            buffer_pool: buffer_pool_manager,
            root: RwLock::new(None),
            max_size: 4,
        });
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

        let index_clone = index.clone();
        let task1 = tokio::spawn(async move {
            for i in 1..len / 2 {
                let val = index_clone.search(&(i as u32)).await?;
                println!("task1 get {} val: {:?}", i, val);
                assert!(val.is_some());
                assert_eq!(i as u32, val.unwrap().page_id as u32);
            }
            Ok::<_, RustDBError>(())
        });
        let index_clone = index.clone();
        let task2 = tokio::spawn(async move {
            for i in len / 2..len {
                let val = index_clone.search(&(i as u32)).await?;
                println!("task2 get {} val: {:?}", i, val);
                assert!(val.is_some());
                assert_eq!(i as u32, val.unwrap().page_id as u32);
            }
            Ok::<_, RustDBError>(())
        });
        task1.await.unwrap()?;
        task2.await.unwrap()?;
        tokio::fs::remove_file(db_name).await?;
        Ok(())
    }
    // #[tokio::test]
    // async fn test_insert_concurrency() -> RustDBResult<()> {
    //     let db_name = "test_insert_concurrency.db";
    //     let disk_manager = DiskManager::new(db_name).await?;
    //     let buffer_pool_manager = BufferPoolManager::new(50, 2, disk_manager).await?;
    //     let len = 60;
    //     let index = Arc::new(Index {
    //         buffer_pool: buffer_pool_manager,
    //         root: RwLock::new(None),
    //         max_size: 4,
    //     });
    //     let index_clone = index.clone();
    //     let task1 = tokio::spawn(async move {
    //         for i in 1..len / 2 {
    //             index_clone
    //                 .insert(
    //                     i as u32,
    //                     RecordId {
    //                         page_id: i,
    //                         slot_num: 0,
    //                     },
    //                 )
    //                 .await?;
    //             println!("task1 insert {}", i);
    //         }
    //         Ok::<_, RustDBError>(())
    //     });
    //     let index_clone = index.clone();
    //     let task2 = tokio::spawn(async move {
    //         for i in len / 2..len {
    //             index_clone
    //                 .insert(
    //                     i as u32,
    //                     RecordId {
    //                         page_id: i,
    //                         slot_num: 0,
    //                     },
    //                 )
    //                 .await?;
    //             println!("task2 insert {}", i);
    //         }
    //         Ok::<_, RustDBError>(())
    //     });
    //     task1.await.unwrap()?;
    //     task2.await.unwrap()?;
    //     index.print::<u32>().await?;
    //
    //     for i in 1..len {
    //         let val = index.search(&(i as u32)).await?;
    //         println!("get {} val: {:?}", i, val);
    //         assert!(val.is_some());
    //         assert_eq!(i as u32, val.unwrap().page_id as u32);
    //     }
    //     assert!(index.search(&(len as u32 + 1)).await?.is_none());
    //     tokio::fs::remove_file(db_name).await?;
    //     Ok(())
    // }
}
