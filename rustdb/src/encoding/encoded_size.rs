use crate::encoding::EncoderVecLen;
use crate::storage::PAGE_SIZE;

pub trait EncodedSize {
    fn encoded_size(&self) -> usize;

    fn total_size(&self) -> usize {
        PAGE_SIZE
    }
}

macro_rules! impl_encoded_size {
    ($($ty:ty);+$(;)?) => {
        $(impl EncodedSize for $ty  {
            #[inline]
             fn encoded_size(&self) -> usize{
                 std::mem::size_of::<$ty>()
             }
        })+
    };
    ($($ty:ty,$encoded_size:expr);+$(;)?) => {
        $(impl EncodedSize for $ty  {
            #[inline]
             fn encoded_size(&self) -> usize{
                 $encoded_size
             }
        })+
    };
}

impl_encoded_size! {
    u8;
    u16;
    u32;
    u64;
    u128;
    i8;
    i16;
    i32;
    i64;
    i128;
    f32;
    f64;
    isize;
    usize;
}
impl_encoded_size! {
    bool,std::mem::size_of::<u8>();
    Option<bool>,std::mem::size_of::<u8>();
    Option<u8>,std::mem::size_of::<u8>();
    Option<u16>,std::mem::size_of::<u16>();
    Option<u32>,std::mem::size_of::<u32>();
    Option<u64>,std::mem::size_of::<u64>();
    Option<u128>,std::mem::size_of::<u128>();
    Option<i8>,std::mem::size_of::<i8>();
    Option<i16>,std::mem::size_of::<i16>();
    Option<i32>,std::mem::size_of::<i32>();
    Option<i64>,std::mem::size_of::<i64>();
    Option<i128>,std::mem::size_of::<i128>();
    Option<f32>,std::mem::size_of::<f32>();
    Option<f64>,std::mem::size_of::<f64>();
    Option<isize>,std::mem::size_of::<isize>();
    Option<usize>,std::mem::size_of::<usize>();
}

impl EncodedSize for String {
    fn encoded_size(&self) -> usize {
        std::mem::size_of::<u32>() + self.as_bytes().len()
    }
}

impl EncodedSize for Option<String> {
    fn encoded_size(&self) -> usize {
        match self {
            None => std::mem::size_of::<u32>(),
            Some(str) => str.encoded_size(),
        }
    }
}

impl<T: EncodedSize> EncodedSize for &[T] {
    fn encoded_size(&self) -> usize {
        (self.len() as EncoderVecLen).encoded_size()
            + self.iter().fold(0, |init, val| init + val.encoded_size())
    }
}

impl<T: EncodedSize> EncodedSize for Vec<T> {
    fn encoded_size(&self) -> usize {
        self.as_slice().encoded_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{Decoder, Encoder};

    #[test]
    fn encode_size() {
        let mut buffer = [0; PAGE_SIZE];
        let str = String::from("Hello world");
        str.encode(&mut buffer.as_mut()).unwrap();
        assert_eq!(
            String::decode(&mut buffer[..str.encoded_size()].as_ref()).unwrap(),
            str
        );
    }
}
