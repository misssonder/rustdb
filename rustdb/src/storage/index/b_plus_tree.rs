use crate::buffer::buffer_poll_manager::BufferPoolManager;
use crate::error::{RustDBError, RustDBResult};
use crate::storage::codec::{Decoder, Encoder};
use crate::storage::page::b_plus_tree::{Header, Internal, Node};
use crate::storage::{PageId, RecordId};

pub struct Index {
    buffer_pool: BufferPoolManager,
    root: PageId,
}

impl Index {
    pub async fn search<K>(&mut self, key: &K) -> RustDBResult<Option<RecordId>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord,
    {
        if let Node::Leaf(ref leaf) = self.find_leaf(key).await? {
            return Ok(leaf.search(key));
        }
        Ok(None)
    }

    pub async fn insert<K>(&mut self, key: K, value: RecordId) -> RustDBResult<()>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Default + Clone,
    {
        let node = self.find_leaf(&key).await?;
        self.insert_inner(node, key, value).await?;
        Ok(())
    }

    pub async fn delete<K>(&mut self, key: &K) -> RustDBResult<Option<(K, RecordId)>> {
        todo!()
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
        match node {
            Node::Internal(ref mut internal) => {}
            Node::Leaf(ref mut leaf) => {
                match leaf.kv.binary_search_by(|(k, _)| k.cmp(&key)) {
                    Ok(index) => leaf.kv[index] = (key.clone(), value.clone()),
                    Err(index) => leaf.kv.insert(index, (key.clone(), value.clone())),
                };
                self.buffer_pool.encode_page_node(&node).await?;
            }
        }
        if node.is_overflow() {
            let (median_key, mut sibling) = node.split();
            let sibling_page_id = self.buffer_pool.new_page_encode(&mut sibling).await?;
            node.set_next(sibling.page_id());
            sibling.set_prev(node.page_id());

            let mut parent_node = if let Some(parent_id) = node.parent() {
                self.buffer_pool.fetch_page_node(parent_id).await?
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
                node.set_parent(parent_node.page_id());
                sibling.set_parent(parent_node.page_id());
                self.root = parent_node.page_id();
                parent_node
            };
            match parent_node {
                Node::Internal(ref mut internal) => {
                    internal.header.size += 1;
                    let index = internal
                        .kv
                        .binary_search_by(|(k, _)| k.cmp(&median_key))
                        .unwrap_err();
                    internal
                        .kv
                        .insert(index, (median_key.clone(), sibling_page_id));
                    // write down to buffer pool
                    self.buffer_pool.encode_page_node(&node).await?;
                    self.buffer_pool.encode_page_node(&sibling).await?;
                    self.buffer_pool.encode_page_node(&parent_node).await?;
                    self.insert_inner(parent_node, key.clone(), value.clone())
                        .await?;
                }
                Node::Leaf(_) => unreachable!(),
            }
        }
        Ok(())
    }

    pub async fn delete_inner<K>(
        &mut self,
        mut node: Node<K>,
        key: &K,
    ) -> RustDBResult<Option<(K, RecordId)>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Default + Clone,
    {
        match node {
            Node::Internal(ref mut internal) => {}
            Node::Leaf(mut leaf) => {
                let (key, value) = match leaf.remove(key) {
                    None => return Ok(None),
                    Some(res) => res,
                };
                if !leaf.is_underflow() {
                    return Ok(Some((key, value)));
                }
                if self.steal(&mut Node::Leaf(leaf)).await?.is_some() {
                    return Ok(Some((key, value)));
                }
            }
        }
        todo!()
    }

    //todo write done to buffer
    pub async fn steal<K>(&mut self, node: &mut Node<K>) -> RustDBResult<Option<()>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord + Default + Clone,
    {
        match node {
            Node::Internal(internal) => {
                if let Some(prev_page_id) = internal.prev() {
                    let prev_page: Node<K> = self.buffer_pool.fetch_page_node(prev_page_id).await?;
                    let mut prev_page = match prev_page {
                        Node::Internal(internal) => internal,
                        Node::Leaf(_) => unreachable!(),
                    };
                    if let Some(steal) = prev_page.steal_last() {
                        let mut child: Node<K> = self.buffer_pool.fetch_page_node(steal.1).await?;
                        child.set_parent(internal.page_id());
                        internal.kv.insert(0, steal);
                        internal.header.size += 1;
                        self.buffer_pool.encode_page_node(&child).await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(prev_page))
                            .await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(internal.clone()))
                            .await?;
                        return Ok(Some(()));
                    }
                }
                if let Some(next_page_id) = internal.next() {
                    let next_page: Node<K> = self.buffer_pool.fetch_page_node(next_page_id).await?;
                    let mut next_page = match next_page {
                        Node::Internal(internal) => internal,
                        Node::Leaf(_) => unreachable!(),
                    };
                    if let Some(steal) = next_page.steal_first() {
                        let mut child: Node<K> = self.buffer_pool.fetch_page_node(steal.1).await?;
                        child.set_parent(internal.page_id());
                        internal.kv.push(steal);
                        internal.header.size += 1;
                        self.buffer_pool.encode_page_node(&child).await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(next_page))
                            .await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Internal(internal.clone()))
                            .await?;
                        return Ok(Some(()));
                    }
                }
            }
            Node::Leaf(ref mut leaf) => {
                if let Some(prev_page_id) = leaf.prev() {
                    let prev_page: Node<K> = self.buffer_pool.fetch_page_node(prev_page_id).await?;
                    let mut prev_page = match prev_page {
                        Node::Internal(_) => {
                            unreachable!()
                        }
                        Node::Leaf(leaf) => leaf,
                    };
                    if let Some(steal) = prev_page.steal_last() {
                        leaf.kv.insert(0, steal);
                        leaf.header.size += 1;
                        self.buffer_pool
                            .encode_page_node(&Node::Leaf(prev_page))
                            .await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Leaf(leaf.clone()))
                            .await?;
                        return Ok(Some(()));
                    }
                }
                if let Some(next_page_id) = leaf.next() {
                    let next_page: Node<K> = self.buffer_pool.fetch_page_node(next_page_id).await?;
                    let mut next_page = match next_page {
                        Node::Internal(_) => {
                            unreachable!()
                        }
                        Node::Leaf(leaf) => leaf,
                    };
                    if let Some(steal) = next_page.steal_first() {
                        leaf.kv.push(steal);
                        leaf.header.size += 1;
                        self.buffer_pool
                            .encode_page_node(&Node::Leaf(next_page))
                            .await?;
                        self.buffer_pool
                            .encode_page_node(&Node::Leaf(leaf.clone()))
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
            let node: Node<K> = self.buffer_pool.fetch_page_node(page_id).await?;
            match node {
                Node::Internal(ref internal) => {
                    page_id = internal.search(key);
                }
                Node::Leaf(leaf) => {
                    return Ok(Node::Leaf(leaf));
                }
            }
        }
    }

    fn is_root<K>(&self, node: &Node<K>) -> bool {
        self.root.eq(&node.page_id())
    }
}
