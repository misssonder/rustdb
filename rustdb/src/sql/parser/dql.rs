use crate::sql::parser::dml::r#where;
use crate::sql::parser::expression::{expression, Expression};
use crate::sql::parser::keyword::Keyword;
use crate::sql::parser::{identifier, IResult};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::error::context;
use nom::multi::separated_list1;
use nom::sequence::{delimited, preceded, terminated, tuple};

#[derive(Clone, Debug, PartialEq)]
pub struct Select {
    pub select: SelectItem,
    pub from: Vec<FromItem>,
    pub r#where: Option<Expression>,
    pub group_by: Option<Vec<Expression>>,
    pub having: Option<Expression>,
    pub order: Option<Vec<(Expression, Order)>>,
    pub offset: Option<Expression>,
    pub limit: Option<Expression>,
}

/// SelectItem handle `*` which Expression can't stand for
#[derive(Clone, Debug, PartialEq)]
pub enum SelectItem {
    All,
    Part(Vec<(Expression, Option<String>)>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum FromItem {
    Table(FromTable),
    Join(FromJoin),
}

#[derive(Clone, Debug, PartialEq)]
pub struct FromTable {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FromJoin {
    pub left: Box<FromItem>,
    pub right: Box<FromItem>,
    pub r#type: JoinType,
    pub predicate: Option<Expression>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Order {
    Ascending,
    Descending,
}

pub fn select(i: &str) -> IResult<&str, Select> {
    context(
        "select",
        terminated(
            map(
                tuple((
                    preceded(multispace0, select_item),
                    preceded(multispace1, from),
                    opt(preceded(multispace1, r#where)),
                    opt(preceded(multispace1, group_by)),
                    opt(preceded(multispace1, having)),
                    opt(preceded(multispace1, order)),
                    opt(preceded(multispace1, offset)),
                    opt(preceded(multispace1, limit)),
                )),
                |(select_item, from, r#where, group_by, having, order, offset, limit)| Select {
                    select: select_item,
                    from,
                    r#where,
                    group_by,
                    having,
                    order,
                    offset,
                    limit,
                },
            ),
            preceded(multispace0, tag(";")),
        ),
    )(i)
}

/// Parse `Select field1 as f1, field2 as f2`
/// Parse `Select *`
fn select_item(i: &str) -> IResult<&str, SelectItem> {
    context(
        "select item",
        preceded(
            preceded(multispace0, tag_no_case(Keyword::Select.to_str())),
            preceded(
                multispace1,
                alt((
                    map(tag("*"), |_| SelectItem::All),
                    map(
                        separated_list1(
                            delimited(multispace0, tag(","), multispace0),
                            select_clause,
                        ),
                        SelectItem::Part,
                    ),
                )),
            ),
        ),
    )(i)
}

/// Parse `field1 as f1, field2 as f2`
fn select_clauses(i: &str) -> IResult<&str, Vec<(Expression, Option<String>)>> {
    context(
        "select clauses",
        separated_list1(delimited(multispace0, tag(","), multispace0), select_clause),
    )(i)
}

/// Parse `field1 as f1`
fn select_clause(i: &str) -> IResult<&str, (Expression, Option<String>)> {
    context(
        "select clause",
        map(
            preceded(
                multispace0,
                tuple((
                    expression(0),
                    opt(preceded(
                        tuple((multispace1, tag_no_case(Keyword::As.to_str()))),
                        preceded(multispace1, identifier),
                    )),
                )),
            ),
            |(expression, reference)| {
                (expression, reference.map(|reference| reference.to_string()))
            },
        ),
    )(i)
}

fn from(i: &str) -> IResult<&str, Vec<FromItem>> {
    context(
        "from item",
        preceded(
            tuple((multispace0, tag_no_case(Keyword::From.to_str()))),
            separated_list1(delimited(multispace0, tag(","), multispace0), from_item),
        ),
    )(i)
}

fn from_item(i: &str) -> IResult<&str, FromItem> {
    let (i, table) = context("from item", preceded(multispace0, from_table))(i)?;
    let item = context(
        "from item",
        map(
            opt(preceded(
                multispace1,
                from_join(FromItem::Table(table.clone())),
            )),
            |join| match join {
                None => FromItem::Table(table.clone()),
                Some(join) => join,
            },
        ),
    )(i)?;
    Ok(item)
}

fn from_table(i: &str) -> IResult<&str, FromTable> {
    context(
        "from table",
        map(
            tuple((
                preceded(multispace0, identifier),
                opt(preceded(
                    tuple((multispace1, tag_no_case(Keyword::As.to_str()))),
                    preceded(multispace1, identifier),
                )),
            )),
            |(name, alias)| FromTable {
                name: name.to_string(),
                alias: alias.map(|alias| alias.to_string()),
            },
        ),
    )(i)
}

fn from_join(left: FromItem) -> impl FnMut(&str) -> IResult<&str, FromItem> {
    move |i| {
        let (i, join_type) = match preceded(multispace0, join_type)(i) {
            Ok((i, join_type)) => (i, join_type),
            Err(_err) => return Ok((i, left.clone())),
        };
        let (i, join) = context(
            "from join",
            map(
                tuple((
                    preceded(multispace1, from_table),
                    opt(preceded(
                        tuple((multispace1, tag_no_case(Keyword::On.to_str()))),
                        expression(0),
                    )),
                )),
                |(right, predicate)| FromJoin {
                    left: Box::new(left.clone()),
                    right: Box::new(FromItem::Table(right)),
                    r#type: join_type.clone(),
                    predicate,
                },
            ),
        )(i)?;
        from_join(FromItem::Join(join))(i)
    }
}

fn join_type(i: &str) -> IResult<&str, JoinType> {
    context(
        "join type",
        alt((
            map(
                preceded(multispace0, tag_no_case(Keyword::Join.to_str())),
                |_| JoinType::Inner,
            ),
            map(
                tuple((
                    preceded(multispace0, tag_no_case(Keyword::Cross.to_str())),
                    preceded(multispace1, tag_no_case(Keyword::Join.to_str())),
                )),
                |_| JoinType::Cross,
            ),
            map(
                tuple((
                    preceded(multispace0, tag_no_case(Keyword::Inner.to_str())),
                    preceded(multispace1, tag_no_case(Keyword::Join.to_str())),
                )),
                |_| JoinType::Inner,
            ),
            map(
                tuple((
                    preceded(multispace0, tag_no_case(Keyword::Left.to_str())),
                    preceded(multispace1, tag_no_case(Keyword::Outer.to_str())),
                    preceded(multispace1, tag_no_case(Keyword::Join.to_str())),
                )),
                |_| JoinType::Left,
            ),
            map(
                tuple((
                    preceded(multispace0, tag_no_case(Keyword::Right.to_str())),
                    preceded(multispace1, tag_no_case(Keyword::Outer.to_str())),
                    preceded(multispace1, tag_no_case(Keyword::Join.to_str())),
                )),
                |_| JoinType::Left,
            ),
        )),
    )(i)
}

/// Parse `Group By filed1, field2`
fn group_by(i: &str) -> IResult<&str, Vec<Expression>> {
    context(
        "group by",
        preceded(
            tuple((
                preceded(multispace0, tag_no_case(Keyword::Group.to_str())),
                preceded(multispace1, tag_no_case(Keyword::By.to_str())),
            )),
            separated_list1(delimited(multispace0, tag(","), multispace0), expression(0)),
        ),
    )(i)
}

fn having(i: &str) -> IResult<&str, Expression> {
    context(
        "having",
        preceded(
            preceded(multispace0, tag_no_case(Keyword::Having.to_str())),
            preceded(multispace1, expression(0)),
        ),
    )(i)
}

/// Parse `Order By filed1 desc, field2`
fn order(i: &str) -> IResult<&str, Vec<(Expression, Order)>> {
    context(
        "order",
        preceded(
            tuple((
                preceded(multispace0, tag_no_case(Keyword::Order.to_str())),
                preceded(multispace1, tag_no_case(Keyword::By.to_str())),
            )),
            separated_list1(
                delimited(multispace0, tag(","), multispace0),
                tuple((
                    expression(0),
                    map(opt(preceded(multispace1, desc_or_asc)), |order| {
                        order.unwrap_or(Order::Ascending)
                    }),
                )),
            ),
        ),
    )(i)
}

fn offset(i: &str) -> IResult<&str, Expression> {
    context(
        "offset",
        preceded(
            preceded(multispace0, tag_no_case(Keyword::Offset.to_str())),
            preceded(multispace1, expression(0)),
        ),
    )(i)
}

fn limit(i: &str) -> IResult<&str, Expression> {
    context(
        "limit",
        preceded(
            preceded(multispace0, tag_no_case(Keyword::Limit.to_str())),
            preceded(multispace1, expression(0)),
        ),
    )(i)
}

fn desc_or_asc(i: &str) -> IResult<&str, Order> {
    preceded(
        multispace0,
        alt((
            map(tag_no_case(Keyword::Asc.to_str()), |_| Order::Ascending),
            map(tag_no_case(Keyword::Desc.to_str()), |_| Order::Descending),
        )),
    )(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::expression::{Literal, Operation};

    #[test]
    fn select() {
        let sql = "select s.id as i, name, marks, attendance
from user as u, students as s
inner join
marks as m
on s.id=m.id
cross join
attendance as a
where a.attendance>=75;";
        let parsed = super::select(sql).unwrap().1;
        let select_item = SelectItem::Part(vec![
            (
                Expression::Field(Some("id".into()), "s".into()),
                Some("i".to_string()),
            ),
            (Expression::Field(None, "name".into()), None),
            (Expression::Field(None, "marks".into()), None),
            (Expression::Field(None, "attendance".into()), None),
        ]);
        let from = vec![
            FromItem::Table(FromTable {
                name: "user".to_string(),
                alias: Some("u".to_string()),
            }),
            FromItem::Join(FromJoin {
                left: Box::new(FromItem::Join(FromJoin {
                    left: Box::new(FromItem::Table(FromTable {
                        name: "students".to_string(),
                        alias: Some("s".to_string()),
                    })),
                    right: Box::new(FromItem::Table(FromTable {
                        name: "marks".to_string(),
                        alias: Some("m".to_string()),
                    })),
                    r#type: JoinType::Inner,
                    predicate: Some(Expression::Operation(Operation::Equal(
                        Box::new(Expression::Field(Some("id".to_string()), "s".to_string())),
                        Box::new(Expression::Field(Some("id".to_string()), "m".to_string())),
                    ))),
                })),
                right: Box::new(FromItem::Table(FromTable {
                    name: "attendance".to_string(),
                    alias: Some("a".to_string()),
                })),
                r#type: JoinType::Cross,
                predicate: None,
            }),
        ];
        let r#where = Some(Expression::Operation(Operation::GreaterThanOrEqual(
            Box::new(Expression::Field(
                Some("attendance".to_string()),
                "a".to_string(),
            )),
            Box::new(Expression::Literal(Literal::Integer(75))),
        )));
        assert_eq!(parsed.select, select_item);
        assert_eq!(parsed.from, from);
        assert_eq!(parsed.r#where, r#where);
    }
}
