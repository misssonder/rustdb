use crate::encoding::{Decoder, Encoder};
use crate::error::RustDBError;
use crate::storage::page::index::{Header, Internal, Leaf, Node};
use crate::storage::{RecordId, NULL_PAGE};
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
        match u8::decode(buf)? {
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
                INTERNAL_TYPE.encode(buf)?;
                interval.encode(buf)
            }
            Node::Leaf(leaf) => {
                LEAF_TYPE.encode(buf)?;
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
        self.size.encode(buf)?;
        self.max_size.encode(buf)?;
        match self.parent {
            None => NULL_PAGE.encode(buf)?,
            Some(parent) => parent.encode(buf)?,
        }
        self.page_id.encode(buf)?;
        match self.next {
            None => NULL_PAGE.encode(buf)?,
            Some(next) => next.encode(buf)?,
        }
        match self.prev {
            None => NULL_PAGE.encode(buf)?,
            Some(prev) => prev.encode(buf)?,
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
            size: usize::decode(buf)?,
            max_size: usize::decode(buf)?,
            parent: match usize::decode(buf)? {
                NULL_PAGE => None,
                other => Some(other),
            },
            page_id: usize::decode(buf)?,
            next: match usize::decode(buf)? {
                NULL_PAGE => None,
                other => Some(other),
            },
            prev: match usize::decode(buf)? {
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
            let v = usize::decode(buf)?;
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
            v.encode(buf)?
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
