use crate::error::RustDBError;
use crate::storage::codec::{Decoder, Encoder};
use crate::storage::{PageId, RecordId, NULL_PAGE};
use bytes::{Buf, BufMut};
use std::cmp::Ordering;
use std::mem;

const INTERNAL_TYPE: u8 = 0;
const LEAF_TYPE: u8 = 1;

#[derive(Debug, PartialEq)]
pub enum Node<K> {
    Internal(Internal<K>),
    Leaf(Leaf<K>),
}

impl<K> Decoder for Node<K>
where
    K: Decoder<Error = RustDBError>,
{
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        match buf.get_u8() {
            INTERNAL_TYPE => Ok(Node::Internal(Internal::decode(buf)?)),
            LEAF_TYPE => Ok(Node::Leaf(Leaf::decode(buf)?)),
            other => Err(RustDBError::Decode(format!("Page type {} invalid", other))),
        }
    }
}

impl<K> Encoder for Node<K>
where
    K: Encoder<Error = RustDBError>,
{
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        match self {
            Node::Internal(interval) => {
                buf.put_u8(INTERNAL_TYPE);
                interval.encode(buf)
            }
            Node::Leaf(leaf) => {
                buf.put_u8(LEAF_TYPE);
                leaf.encode(buf)
            }
        }
    }
}

impl<K> Node<K> {
    pub fn is_overflow(&self) -> bool {
        match self {
            Node::Internal(internal) => internal.is_overflow(),
            Node::Leaf(leaf) => leaf.is_overflow(),
        }
    }
    pub fn is_underflow(&self) -> bool {
        match self {
            Node::Internal(internal) => internal.is_underflow(),
            Node::Leaf(leaf) => leaf.is_underflow(),
        }
    }

    pub fn parent(&self) -> Option<PageId> {
        match self {
            Node::Internal(node) => node.parent(),
            Node::Leaf(node) => node.parent(),
        }
    }
    pub fn set_parent(&mut self, page_id: PageId) {
        match self {
            Node::Internal(node) => node.set_parent(page_id),
            Node::Leaf(node) => node.set_parent(page_id),
        }
    }

    pub fn page_id(&self) -> PageId {
        match self {
            Node::Internal(node) => node.page_id(),
            Node::Leaf(node) => node.page_id(),
        }
    }

    pub fn set_next(&mut self, page_id: PageId) {
        match self {
            Node::Internal(node) => node.set_next(page_id),
            Node::Leaf(node) => node.set_next(page_id),
        }
    }

    pub fn set_prev(&mut self, page_id: PageId) {
        match self {
            Node::Internal(node) => node.set_prev(page_id),
            Node::Leaf(node) => node.set_prev(page_id),
        }
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        match self {
            Node::Internal(internal) => internal.set_page_id(page_id),
            Node::Leaf(leaf) => leaf.set_page_id(page_id),
        }
    }
    pub fn max_size(&mut self) -> usize {
        match self {
            Node::Internal(internal) => internal.max_size(),
            Node::Leaf(leaf) => leaf.max_size(),
        }
    }

    pub fn split(&mut self) -> (K, Node<K>)
    where
        K: Default,
    {
        match self {
            Node::Internal(ref mut internal) => {
                let (median_key, sibling) = internal.split();
                (median_key, Node::Internal(sibling))
            }
            Node::Leaf(ref mut leaf) => {
                let (median_key, sibling) = leaf.split();
                (median_key, Node::Leaf(sibling))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub size: usize,
    pub max_size: usize,
    pub parent: Option<PageId>,
    pub page_id: PageId,
    pub next: Option<PageId>,
    pub prev: Option<PageId>,
}

impl Encoder for Header {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        buf.put_u64(self.size as _);
        buf.put_u64(self.max_size as _);
        match self.parent {
            None => buf.put_u64(NULL_PAGE as _),
            Some(parent) => buf.put_u64(parent as _),
        }
        buf.put_u64(self.page_id as _);
        match self.next {
            None => buf.put_u64(NULL_PAGE as _),
            Some(next) => buf.put_u64(next as _),
        }
        match self.prev {
            None => buf.put_u64(NULL_PAGE as _),
            Some(prev) => buf.put_u64(prev as _),
        }
        Ok(())
    }
}

impl Decoder for Header {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        Ok(Header {
            size: buf.get_u64() as _,
            max_size: buf.get_u64() as _,
            parent: match buf.get_u64() as PageId {
                NULL_PAGE => None,
                other => Some(other),
            },
            page_id: buf.get_u64() as _,
            next: match buf.get_u64() as PageId {
                NULL_PAGE => None,
                other => Some(other),
            },
            prev: match buf.get_u64() as PageId {
                NULL_PAGE => None,
                other => Some(other),
            },
        })
    }
}

/**
 * Store `n` indexed keys and `n + 1` child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: Since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search / lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 * ----------------------------------------------------------------------------------
 * | HEADER | KEY(1) + PAGE_ID(1) | KEY(2) + PAGE_ID(2) | ... | KEY(n) + PAGE_ID(n) |
 * ----------------------------------------------------------------------------------
 */

#[derive(Debug, Clone, PartialEq)]
pub struct Internal<K> {
    pub header: Header,
    pub kv: Vec<(K, PageId)>,
}

impl<K> Decoder for Internal<K>
where
    K: Decoder<Error = RustDBError>,
{
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        let header = Header::decode(buf)?;
        let mut kv = Vec::with_capacity(header.size);
        for _ in 0..header.size {
            let k = K::decode(buf)?;
            let v = buf.get_u64() as _;
            kv.push((k, v));
        }
        Ok(Self { header, kv })
    }
}

impl<K> Encoder for Internal<K>
where
    K: Encoder<Error = RustDBError>,
{
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        self.header.encode(buf)?;
        for (k, v) in self.kv.iter() {
            k.encode(buf)?;
            buf.put_u64(*v as _)
        }
        Ok(())
    }
}

impl<K> Internal<K> {
    pub fn search(&self, key: &K) -> PageId
    where
        K: Ord,
    {
        let (mut start, mut end) = (1, self.header.size - 1);
        while start < end {
            let mid = (start + end) / 2;
            match self.kv[mid].0.cmp(key) {
                Ordering::Less => {
                    start = mid + 1;
                }
                Ordering::Equal => return self.kv[mid].1,
                Ordering::Greater => {
                    end = mid - 1;
                }
            }
        }
        match self.kv[start].0.cmp(key) {
            Ordering::Less => self.kv[start - 1].1,
            _ => self.kv[start].1,
        }
    }

    pub fn is_overflow(&self) -> bool {
        // the max length of the key is m - 1
        self.header.size > self.header.max_size - 1
    }

    pub fn is_underflow(&self) -> bool {
        // the max length of the key is m - 1
        self.parent().is_some() && self.header.size < self.header.max_size / 2
    }

    pub fn max_size(&self) -> usize {
        self.header.max_size
    }

    pub fn page_id(&self) -> PageId {
        self.header.page_id
    }
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.header.page_id = page_id
    }

    pub fn set_parent(&mut self, page_id: PageId) {
        self.header.parent = Some(page_id);
    }

    pub fn parent(&self) -> Option<PageId> {
        self.header.parent
    }

    pub fn set_next(&mut self, page_id: PageId) {
        self.header.next = Some(page_id);
    }

    pub fn next(&self) -> Option<PageId> {
        self.header.next
    }

    pub fn set_prev(&mut self, page_id: PageId) {
        self.header.prev = Some(page_id);
    }

    pub fn prev(&self) -> Option<PageId> {
        self.header.prev
    }

    pub fn split(&mut self) -> (K, Internal<K>)
    where
        K: Default,
    {
        // index 0 is ignored, so we split kv from max_size/2 +1
        let spilt_at = self.header.max_size / 2 + 1;

        let mut sibling_kv = self.kv.split_off(spilt_at);
        let median_key = mem::take(&mut sibling_kv[0].0);
        let mut sibling_header = self.header.clone();
        self.header.size = self.kv.len() - 1;
        sibling_header.size = sibling_kv.len() - 1;
        (
            median_key,
            Internal {
                header: sibling_header,
                kv: sibling_kv,
            },
        )
    }

    pub fn steal_first(&mut self) -> Option<(K, PageId)> {
        if self.allow_steal() {
            self.header.size -= 1;
            return Some(self.kv.remove(0));
        }
        None
    }

    pub fn steal_last(&mut self) -> Option<(K, PageId)> {
        if self.allow_steal() {
            self.header.size -= 1;
            return self.kv.pop();
        }
        None
    }

    fn allow_steal(&self) -> bool {
        self.header.size > self.header.max_size / 2
    }
}

/**
 * Store indexed key and record id (record id = page id combined with slot id,
 * see `include/common/rid.h` for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 * -----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)  |
 * -----------------------------------------------------------------------
 *
 * Header format (size in byte, 16 bytes in total):
 * -----------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) | NextPageId (4) | ... |
 * -----------------------------------------------------------------------
 */

#[derive(Debug, Clone, PartialEq)]
pub struct Leaf<K> {
    pub header: Header,
    pub kv: Vec<(K, RecordId)>,
}

impl<K> Decoder for Leaf<K>
where
    K: Decoder<Error = RustDBError>,
{
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        let header = Header::decode(buf)?;
        let mut kv = Vec::with_capacity(header.size);
        for _ in 0..header.size {
            let k = K::decode(buf)?;
            let v = RecordId::decode(buf)?;
            kv.push((k, v));
        }
        Ok(Self { header, kv })
    }
}

impl<K> Encoder for Leaf<K>
where
    K: Encoder<Error = RustDBError>,
{
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        self.header.encode(buf)?;
        for (k, v) in self.kv.iter() {
            k.encode(buf)?;
            v.encode(buf)?;
        }
        Ok(())
    }
}

impl<K> Leaf<K> {
    pub fn search(&self, key: &K) -> Option<RecordId>
    where
        K: Ord,
    {
        match self.kv.binary_search_by(|(k, _)| k.cmp(key)) {
            Ok(index) => Some(self.kv[index].1.clone()),
            Err(_) => None,
        }
    }

    pub fn is_overflow(&self) -> bool {
        self.header.size > self.header.max_size - 1
    }

    pub fn is_underflow(&self) -> bool {
        self.parent().is_some() && self.header.size < self.header.max_size / 2
    }

    pub fn max_size(&self) -> usize {
        self.header.max_size
    }

    pub fn page_id(&self) -> PageId {
        self.header.page_id
    }
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.header.page_id = page_id
    }

    pub fn set_parent(&mut self, page_id: PageId) {
        self.header.parent = Some(page_id);
    }

    pub fn parent(&self) -> Option<PageId> {
        self.header.parent
    }

    pub fn set_next(&mut self, page_id: PageId) {
        self.header.next = Some(page_id);
    }

    pub fn next(&self) -> Option<PageId> {
        self.header.next
    }

    pub fn set_prev(&mut self, page_id: PageId) {
        self.header.prev = Some(page_id);
    }

    pub fn prev(&self) -> Option<PageId> {
        self.header.prev
    }

    pub fn remove(&mut self, key: &K) -> Option<(K, RecordId)>
    where
        K: Ord,
    {
        match self.kv.binary_search_by(|(k, _)| k.cmp(&key)) {
            Ok(index) => {
                self.header.size -= 1;
                Some(self.kv.remove(index))
            }
            Err(_) => None,
        }
    }

    pub fn split(&mut self) -> (K, Leaf<K>)
    where
        K: Default,
    {
        let spilt_at = self.header.max_size / 2;
        let mut sibling_kv = self.kv.split_off(spilt_at);
        let median_key = mem::take(&mut sibling_kv[0].0);
        let mut sibling_header = self.header.clone();
        self.header.size = self.kv.len();
        sibling_header.size = sibling_kv.len();
        (
            median_key,
            Leaf {
                header: sibling_header,
                kv: sibling_kv,
            },
        )
    }

    pub fn steal_first(&mut self) -> Option<(K, RecordId)> {
        if self.allow_steal() {
            self.header.size -= 1;
            return Some(self.kv.remove(0));
        }
        None
    }

    pub fn steal_last(&mut self) -> Option<(K, RecordId)> {
        if self.allow_steal() {
            self.header.size -= 1;
            return self.kv.pop();
        }
        None
    }

    fn allow_steal(&self) -> bool {
        self.header.size > self.header.max_size / 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RustDBResult;
    use crate::storage::PAGE_SIZE;

    #[derive(PartialEq, Debug)]
    struct Key {
        data: u32,
    }

    impl Encoder for Key {
        type Error = RustDBError;

        fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
        where
            B: BufMut,
        {
            buf.put_u32(self.data);
            Ok(())
        }
    }

    impl Decoder for Key {
        type Error = RustDBError;

        fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
        where
            B: Buf,
        {
            Ok(Self {
                data: buf.get_u32(),
            })
        }
    }

    #[test]
    fn test_header_decode_encode() -> RustDBResult<()> {
        let header = Header {
            size: 1,
            max_size: 2,
            parent: None,
            page_id: 4,
            next: Some(5),
            prev: Some(6),
        };
        let mut buffer = [0; PAGE_SIZE];
        header.encode(&mut buffer.as_mut())?;
        let new_header1 = Header::decode(&mut buffer.as_ref())?;
        let new_header2 = Header::decode(&mut buffer.as_ref())?;
        assert_eq!(header, new_header1);
        assert_eq!(header, new_header2);
        Ok(())
    }

    #[test]
    fn test_internal_decode_encode() -> RustDBResult<()> {
        let len = 100;
        let mut kv = Vec::with_capacity(len);
        for i in 0..len {
            kv.push((Key { data: i as u32 }, i))
        }
        let tree = Node::Internal(Internal {
            header: Header {
                size: len,
                max_size: len,
                parent: Some(1),
                page_id: 2,
                next: Some(5),
                prev: Some(6),
            },
            kv,
        });

        let mut buffer = [0; PAGE_SIZE];
        tree.encode(&mut buffer.as_mut())?;
        let new_tree: Node<Key> = Node::decode(&mut buffer.as_ref())?;
        assert_eq!(new_tree, tree);
        tree.encode(&mut buffer.as_mut())?;
        let new_tree2: Node<Key> = Node::decode(&mut buffer.as_ref())?;
        assert_eq!(new_tree, new_tree2);
        Ok(())
    }

    #[test]
    fn test_leaf_decode_encode() -> RustDBResult<()> {
        let len = 100;
        let mut kv = Vec::with_capacity(len);
        for i in 0..len {
            kv.push((
                Key { data: i as u32 },
                RecordId {
                    page_id: i,
                    slot_num: i as _,
                },
            ))
        }
        let tree = Node::Leaf(Leaf {
            header: Header {
                size: len,
                max_size: len,
                parent: Some(2),
                page_id: 3,
                next: Some(9),
                prev: Some(8),
            },
            kv,
        });

        let mut buffer = [0; PAGE_SIZE];
        tree.encode(&mut buffer.as_mut())?;
        let new_tree: Node<Key> = Node::decode(&mut buffer.as_ref())?;
        assert_eq!(new_tree, tree);
        tree.encode(&mut buffer.as_mut())?;
        let new_tree2: Node<Key> = Node::decode(&mut buffer.as_ref())?;
        assert_eq!(new_tree, new_tree2);
        Ok(())
    }
}
