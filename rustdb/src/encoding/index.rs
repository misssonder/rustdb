use crate::encoding::{Decoder, Encoder};
use crate::error::RustDBError;
use crate::storage::page::index::{Header, Internal, Leaf, Node};
use crate::storage::{PageId, RecordId, NULL_PAGE};
use bytes::{Buf, BufMut};

const INTERNAL_TYPE: u8 = 1;
const LEAF_TYPE: u8 = 2;
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
        let mut kv = Vec::with_capacity(header.size + 1);
        for _ in 0..header.size + 1 {
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
