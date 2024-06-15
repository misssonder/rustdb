use crate::sql::types::DataType;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::IResult;

pub fn datatype(i: &[u8]) -> IResult<&[u8], DataType> {
    alt((
        map(tag_no_case(DataType::Boolean.as_str()), |_| {
            DataType::Boolean
        }),
        map(tag_no_case(DataType::Tinyint.as_str()), |_| {
            DataType::Tinyint
        }),
        map(tag_no_case(DataType::Smallint.as_str()), |_| {
            DataType::Smallint
        }),
        map(tag_no_case(DataType::Integer.as_str()), |_| {
            DataType::Integer
        }),
        map(tag_no_case(DataType::Bigint.as_str()), |_| DataType::Bigint),
        map(tag_no_case(DataType::Float.as_str()), |_| DataType::Float),
        map(tag_no_case(DataType::Double.as_str()), |_| DataType::Double),
    ))(i)
}

#[cfg(test)]
mod tests {
    

    #[test]
    fn datatype() {
        println!("{:?}", super::datatype("Smallint".as_bytes()));
    }
}
