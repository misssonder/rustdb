use crate::buffer::buffer_poll_manager::BufferPoolManager;
use crate::error::{RustDBError, RustDBResult};
use crate::storage::codec::{Decoder, Encoder};
use crate::storage::page::b_plus_tree::{Node};
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
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord,
    {
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
            Node::Internal(internal) => {}
            Node::Leaf(ref mut leaf) => {
                match leaf.kv.binary_search_by(|(k, _)| k.cmp(&key)) {
                    Ok(index) => leaf.kv[index] = (key.clone(), value.clone()),
                    Err(index) => leaf.kv.insert(index, (key.clone(), value.clone())),
                }
                if leaf.is_full() {
                    let (median_key, mut sibling) = leaf.split();
                    let sibling_page = self
                        .buffer_pool
                        .new_page()
                        .await?
                        .ok_or(RustDBError::BufferPool("Can't not new page".into()))?;
                    let sibling_page_id = sibling_page.read().await.page_id();
                    sibling.header.page_id = sibling_page_id;
                    sibling.encode(&mut sibling_page.write().await.mut_data())?;
                    // todo unpin
                    let mut parent_page: Node<K> = self
                        .buffer_pool
                        .fetch_page_node(leaf.header.page_id)
                        .await?;
                    match parent_page {
                        Node::Internal(ref mut internal) => {
                            internal.header.size += 1;
                            let index = internal
                                .kv
                                .binary_search_by(|(k, _)| k.cmp(&median_key))
                                .unwrap_err();
                            internal.kv.insert(index, (median_key, sibling_page_id));
                            self.insert_inner(parent_page, key.clone(), value.clone())
                                .await?;
                        }
                        Node::Leaf(_) => unreachable!(),
                    }
                }
            }
        }
        Ok(())
    }

    async fn find_leaf<K>(&mut self, key: &K) -> RustDBResult<Node<K>>
    where
        K: Decoder<Error = RustDBError> + Encoder<Error = RustDBError> + Ord,
    {
        let mut node = self.root;
        loop {
            let page = self
                .buffer_pool
                .fetch_page(node)
                .await?
                .ok_or(RustDBError::BufferPool("Can't fetch page".to_string()))?;
            let page = page.read().await;
            //todo if decode return error, we should still unpin page;
            let tree: Node<K> = Node::decode(&mut page.data())?;
            match tree {
                Node::Internal(ref internal) => {
                    node = internal.search(key);
                    self.buffer_pool.unpin_page(page.page_id(), false).await;
                }
                Node::Leaf(leaf) => {
                    self.buffer_pool.unpin_page(page.page_id(), false).await;
                    return Ok(Node::Leaf(leaf));
                }
            }
        }
    }
}
