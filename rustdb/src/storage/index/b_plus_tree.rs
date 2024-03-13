use crate::buffer::buffer_poll_manager::BufferPoolManager;
use crate::error::{RustDBError, RustDBResult};
use crate::storage::codec::{Decoder, Encoder};
use crate::storage::page::b_plus_tree::{Internal, InternalHeader, Node};
use crate::storage::{PageId, RecordId};
use std::io::Read;

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
            Node::Leaf(ref mut leaf) => match leaf.kv.binary_search_by(|(k, _)| k.cmp(&key)) {
                Ok(index) => leaf.kv[index] = (key.clone(), value.clone()),
                Err(index) => leaf.kv.insert(index, (key.clone(), value.clone())),
            },
        }
        if node.is_full() {
            let (median_key, mut sibling) = node.split();
            let sibling_page_id = self.buffer_pool.new_page_encode(&mut sibling).await?;
            if let Node::Leaf(ref mut leaf) = node {
                leaf.set_next(sibling_page_id);
            }
            // todo unpin
            let mut parent_node: Node<K> = if node.page_id().eq(&self.root) {
                let mut parent_node = Node::Internal(Internal {
                    header: InternalHeader {
                        size: 1,
                        max_size: node.max_size(),
                        parent: PageId::MAX,
                        page_id: 0,
                    },
                    kv: vec![
                        (K::default(), node.page_id()),
                        (median_key.clone(), sibling_page_id),
                    ],
                });
                self.buffer_pool.new_page_encode(&mut parent_node).await?;
                parent_node
            } else {
                self.buffer_pool.fetch_page_node(node.parent_id()).await?
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
                    self.insert_inner(parent_node, key.clone(), value.clone())
                        .await?;
                }
                Node::Leaf(_) => unreachable!(),
            }
        }
        Ok(())
    }

    async fn find_leaf<K>(&mut self, key: &K) -> RustDBResult<Node<K>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord,
    {
        let mut page_id = self.root;
        loop {
            //todo if decode return error, we should still unpin page;
            let node: Node<K> = self.buffer_pool.fetch_page_node(page_id).await?;
            let node_page_id = node.page_id();
            match node {
                Node::Internal(ref internal) => {
                    page_id = internal.search(key);
                    self.buffer_pool.unpin_page(node_page_id, false).await;
                }
                Node::Leaf(leaf) => {
                    self.buffer_pool.unpin_page(node_page_id, false).await;
                    return Ok(Node::Leaf(leaf));
                }
            }
        }
    }
}
