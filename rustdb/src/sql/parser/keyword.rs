use crate::sql::parser::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::error::context;

#[derive(Clone, Debug, PartialEq)]
pub enum Keyword {
    And,
    As,
    Asc,
    Begin,
    Bool,
    Boolean,
    By,
    Char,
    Commit,
    Create,
    Cross,
    Default,
    Delete,
    Desc,
    Double,
    Drop,
    Exists,
    Explain,
    False,
    Float,
    From,
    Group,
    Having,
    If,
    Index,
    Infinity,
    Inner,
    Insert,
    Int,
    Integer,
    Into,
    Is,
    Join,
    Key,
    Left,
    Like,
    Limit,
    NaN,
    Not,
    Null,
    Of,
    Offset,
    On,
    Only,
    Or,
    Order,
    Outer,
    Primary,
    Read,
    References,
    Right,
    Rollback,
    Select,
    Set,
    String,
    System,
    Table,
    Text,
    Time,
    Transaction,
    True,
    Unique,
    Update,
    Values,
    Varchar,
    Where,
    Write,
}

impl Keyword {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(ident: &str) -> Option<Self> {
        Some(match ident.to_uppercase().as_ref() {
            "AS" => Self::As,
            "ASC" => Self::Asc,
            "AND" => Self::And,
            "BEGIN" => Self::Begin,
            "BOOL" => Self::Bool,
            "BOOLEAN" => Self::Boolean,
            "BY" => Self::By,
            "CHAR" => Self::Char,
            "COMMIT" => Self::Commit,
            "CREATE" => Self::Create,
            "CROSS" => Self::Cross,
            "DEFAULT" => Self::Default,
            "DELETE" => Self::Delete,
            "DESC" => Self::Desc,
            "DOUBLE" => Self::Double,
            "DROP" => Self::Drop,
            "EXISTS" => Self::Exists,
            "EXPLAIN" => Self::Explain,
            "FALSE" => Self::False,
            "FLOAT" => Self::Float,
            "FROM" => Self::From,
            "GROUP" => Self::Group,
            "HAVING" => Self::Having,
            "IF" => Self::If,
            "INDEX" => Self::Index,
            "INFINITY" => Self::Infinity,
            "INNER" => Self::Inner,
            "INSERT" => Self::Insert,
            "INT" => Self::Int,
            "INTEGER" => Self::Integer,
            "INTO" => Self::Into,
            "IS" => Self::Is,
            "JOIN" => Self::Join,
            "KEY" => Self::Key,
            "LEFT" => Self::Left,
            "LIKE" => Self::Like,
            "LIMIT" => Self::Limit,
            "NAN" => Self::NaN,
            "NOT" => Self::Not,
            "NULL" => Self::Null,
            "OF" => Self::Of,
            "OFFSET" => Self::Offset,
            "ON" => Self::On,
            "ONLY" => Self::Only,
            "OR" => Self::Or,
            "ORDER" => Self::Order,
            "OUTER" => Self::Outer,
            "PRIMARY" => Self::Primary,
            "READ" => Self::Read,
            "REFERENCES" => Self::References,
            "RIGHT" => Self::Right,
            "ROLLBACK" => Self::Rollback,
            "SELECT" => Self::Select,
            "SET" => Self::Set,
            "STRING" => Self::String,
            "SYSTEM" => Self::System,
            "TABLE" => Self::Table,
            "TEXT" => Self::Text,
            "TIME" => Self::Time,
            "TRANSACTION" => Self::Transaction,
            "TRUE" => Self::True,
            "UNIQUE" => Self::Unique,
            "UPDATE" => Self::Update,
            "VALUES" => Self::Values,
            "VARCHAR" => Self::Varchar,
            "WHERE" => Self::Where,
            "WRITE" => Self::Write,
            _ => return None,
        })
    }

    pub fn to_str(&self) -> &str {
        match self {
            Self::As => "AS",
            Self::Asc => "ASC",
            Self::And => "AND",
            Self::Begin => "BEGIN",
            Self::Bool => "BOOL",
            Self::Boolean => "BOOLEAN",
            Self::By => "BY",
            Self::Char => "CHAR",
            Self::Commit => "COMMIT",
            Self::Create => "CREATE",
            Self::Cross => "CROSS",
            Self::Default => "DEFAULT",
            Self::Delete => "DELETE",
            Self::Desc => "DESC",
            Self::Double => "DOUBLE",
            Self::Drop => "DROP",
            Self::Exists => "EXISTS",
            Self::Explain => "EXPLAIN",
            Self::False => "FALSE",
            Self::Float => "FLOAT",
            Self::From => "FROM",
            Self::Group => "GROUP",
            Self::Having => "HAVING",
            Self::If => "IF",
            Self::Index => "INDEX",
            Self::Infinity => "INFINITY",
            Self::Inner => "INNER",
            Self::Insert => "INSERT",
            Self::Int => "INT",
            Self::Integer => "INTEGER",
            Self::Into => "INTO",
            Self::Is => "IS",
            Self::Join => "JOIN",
            Self::Key => "KEY",
            Self::Left => "LEFT",
            Self::Like => "LIKE",
            Self::Limit => "LIMIT",
            Self::NaN => "NAN",
            Self::Not => "NOT",
            Self::Null => "NULL",
            Self::Of => "OF",
            Self::Offset => "OFFSET",
            Self::On => "ON",
            Self::Only => "ONLY",
            Self::Outer => "OUTER",
            Self::Or => "OR",
            Self::Order => "ORDER",
            Self::Primary => "PRIMARY",
            Self::Read => "READ",
            Self::References => "REFERENCES",
            Self::Right => "RIGHT",
            Self::Rollback => "ROLLBACK",
            Self::Select => "SELECT",
            Self::Set => "SET",
            Self::String => "STRING",
            Self::System => "SYSTEM",
            Self::Table => "TABLE",
            Self::Text => "TEXT",
            Self::Time => "TIME",
            Self::Transaction => "TRANSACTION",
            Self::True => "TRUE",
            Self::Unique => "UNIQUE",
            Self::Update => "UPDATE",
            Self::Values => "VALUES",
            Self::Varchar => "VARCHAR",
            Self::Where => "WHERE",
            Self::Write => "WRITE",
        }
    }
}

pub fn keyword(i: &str) -> IResult<&str, Keyword> {
    context(
        "keyword",
        alt((
            keyword_a_to_d,
            keyword_e_to_g,
            keyword_h_to_k,
            keyword_l_to_n,
            keyword_o_to_q,
            keyword_r_to_t,
            keyword_u_to_z,
        )),
    )(i)
}

fn keyword_a_to_d(i: &str) -> IResult<&str, Keyword> {
    alt((
        map(tag_no_case(Keyword::And.to_str()), |_| Keyword::And),
        map(tag_no_case(Keyword::As.to_str()), |_| Keyword::As),
        map(tag_no_case(Keyword::Asc.to_str()), |_| Keyword::Asc),
        map(tag_no_case(Keyword::Boolean.to_str()), |_| Keyword::Boolean),
        map(tag_no_case(Keyword::Begin.to_str()), |_| Keyword::Begin),
        map(tag_no_case(Keyword::By.to_str()), |_| Keyword::By),
        map(tag_no_case(Keyword::Bool.to_str()), |_| Keyword::Bool),
        map(tag_no_case(Keyword::Char.to_str()), |_| Keyword::Char),
        map(tag_no_case(Keyword::Commit.to_str()), |_| Keyword::Commit),
        map(tag_no_case(Keyword::Create.to_str()), |_| Keyword::Create),
        map(tag_no_case(Keyword::Cross.to_str()), |_| Keyword::Cross),
        map(tag_no_case(Keyword::Default.to_str()), |_| Keyword::Default),
        map(tag_no_case(Keyword::Delete.to_str()), |_| Keyword::Delete),
        map(tag_no_case(Keyword::Desc.to_str()), |_| Keyword::Desc),
        map(tag_no_case(Keyword::Drop.to_str()), |_| Keyword::Drop),
        map(tag_no_case(Keyword::Double.to_str()), |_| Keyword::Double),
    ))(i)
}

fn keyword_e_to_g(i: &str) -> IResult<&str, Keyword> {
    alt((
        map(tag_no_case(Keyword::Exists.to_str()), |_| Keyword::Exists),
        map(tag_no_case(Keyword::Explain.to_str()), |_| Keyword::Explain),
        map(tag_no_case(Keyword::False.to_str()), |_| Keyword::False),
        map(tag_no_case(Keyword::Float.to_str()), |_| Keyword::Float),
        map(tag_no_case(Keyword::From.to_str()), |_| Keyword::From),
        map(tag_no_case(Keyword::Group.to_str()), |_| Keyword::Group),
    ))(i)
}

fn keyword_h_to_k(i: &str) -> IResult<&str, Keyword> {
    alt((
        map(tag_no_case(Keyword::Having.to_str()), |_| Keyword::Having),
        map(tag_no_case(Keyword::If.to_str()), |_| Keyword::If),
        map(tag_no_case(Keyword::Index.to_str()), |_| Keyword::Index),
        map(tag_no_case(Keyword::Infinity.to_str()), |_| {
            Keyword::Infinity
        }),
        map(tag_no_case(Keyword::Inner.to_str()), |_| Keyword::Inner),
        map(tag_no_case(Keyword::Insert.to_str()), |_| Keyword::Insert),
        map(tag_no_case(Keyword::Int.to_str()), |_| Keyword::Int),
        map(tag_no_case(Keyword::Integer.to_str()), |_| Keyword::Integer),
        map(tag_no_case(Keyword::Into.to_str()), |_| Keyword::Into),
        map(tag_no_case(Keyword::Is.to_str()), |_| Keyword::Is),
        map(tag_no_case(Keyword::Join.to_str()), |_| Keyword::Join),
    ))(i)
}

fn keyword_l_to_n(i: &str) -> IResult<&str, Keyword> {
    alt((
        map(tag_no_case(Keyword::Left.to_str()), |_| Keyword::Left),
        map(tag_no_case(Keyword::Like.to_str()), |_| Keyword::Like),
        map(tag_no_case(Keyword::Limit.to_str()), |_| Keyword::Limit),
        map(tag_no_case(Keyword::NaN.to_str()), |_| Keyword::NaN),
        map(tag_no_case(Keyword::Not.to_str()), |_| Keyword::Not),
        map(tag_no_case(Keyword::Null.to_str()), |_| Keyword::Null),
    ))(i)
}

fn keyword_o_to_q(i: &str) -> IResult<&str, Keyword> {
    alt((
        map(tag_no_case(Keyword::Of.to_str()), |_| Keyword::Of),
        map(tag_no_case(Keyword::Offset.to_str()), |_| Keyword::Offset),
        map(tag_no_case(Keyword::On.to_str()), |_| Keyword::On),
        map(tag_no_case(Keyword::Only.to_str()), |_| Keyword::Only),
        map(tag_no_case(Keyword::Or.to_str()), |_| Keyword::Or),
        map(tag_no_case(Keyword::Order.to_str()), |_| Keyword::Order),
        map(tag_no_case(Keyword::Outer.to_str()), |_| Keyword::Outer),
        map(tag_no_case(Keyword::Primary.to_str()), |_| Keyword::Primary),
    ))(i)
}

fn keyword_r_to_t(i: &str) -> IResult<&str, Keyword> {
    alt((
        map(tag_no_case(Keyword::Read.to_str()), |_| Keyword::Of),
        map(tag_no_case(Keyword::References.to_str()), |_| {
            Keyword::References
        }),
        map(tag_no_case(Keyword::Right.to_str()), |_| Keyword::Right),
        map(tag_no_case(Keyword::Rollback.to_str()), |_| {
            Keyword::Rollback
        }),
        map(tag_no_case(Keyword::Select.to_str()), |_| Keyword::Select),
        map(tag_no_case(Keyword::Set.to_str()), |_| Keyword::Set),
        map(tag_no_case(Keyword::System.to_str()), |_| Keyword::System),
        map(tag_no_case(Keyword::String.to_str()), |_| Keyword::String),
        map(tag_no_case(Keyword::Table.to_str()), |_| Keyword::Table),
        map(tag_no_case(Keyword::Text.to_str()), |_| Keyword::Text),
        map(tag_no_case(Keyword::Time.to_str()), |_| Keyword::Time),
        map(tag_no_case(Keyword::Transaction.to_str()), |_| {
            Keyword::Transaction
        }),
        map(tag_no_case(Keyword::True.to_str()), |_| Keyword::True),
    ))(i)
}

fn keyword_u_to_z(i: &str) -> IResult<&str, Keyword> {
    alt((
        map(tag_no_case(Keyword::Unique.to_str()), |_| Keyword::Unique),
        map(tag_no_case(Keyword::Update.to_str()), |_| Keyword::Update),
        map(tag_no_case(Keyword::Values.to_str()), |_| Keyword::Values),
        map(tag_no_case(Keyword::Varchar.to_str()), |_| Keyword::Varchar),
        map(tag_no_case(Keyword::Where.to_str()), |_| Keyword::Where),
        map(tag_no_case(Keyword::Write.to_str()), |_| Keyword::Write),
    ))(i)
}
