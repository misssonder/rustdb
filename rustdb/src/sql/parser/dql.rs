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
    select: SelectItem,
    from: Vec<FromItem>,
    r#where: Option<Expression>,
    group_by: Option<Vec<Expression>>,
    having: Option<Expression>,
    order: Option<Vec<(Expression, Order)>>,
    offset: Option<Expression>,
    limit: Option<Expression>,
}

/// SelectItem handle `*` which Expression can't stand for
#[derive(Clone, Debug, PartialEq)]
pub enum SelectItem {
    All,
    Part(Vec<(Expression, Option<String>)>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum FromItem {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        r#type: JoinType,
        predicate: Option<Expression>,
    },
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
                    opt(preceded(multispace1, r#where)),
                    opt(preceded(multispace1, group_by)),
                    opt(preceded(multispace1, having)),
                    opt(preceded(multispace1, order)),
                    opt(preceded(multispace1, offset)),
                    opt(preceded(multispace1, limit)),
                )),
                |(select_item, r#where, group_by, having, order, offset, limit)| Select {
                    select: select_item,
                    from: vec![],
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
