//! This module implements all the logic related to parsing and representing
//! queries.

// TODO: Handle sub operator.
// TODO: Handle symbols?
// TODO: Better error handling?
// TODO: Allow more than 2 operators to AND,OR and XOR (... and ... and ...)
// given the underlying engine supports it. Also flattening.
// TODO: Fuzzy precedence?

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, multispace0, multispace1},
    combinator::{cut, map, recognize, verify},
    multi::many0,
    sequence::{delimited, pair, terminated},
    IResult,
};
use thiserror::Error;

use std::str::FromStr;

const KEYWORDS: [&str; 4] = ["not", "and", "xor", "or"];
const MAX_LENGTH: usize = 2048;

fn parse_property(s: &str) -> IResult<&str, Expression> {
    map(
        verify(
            recognize(pair(
                alpha1,
                many0(alt((
                    alphanumeric1,
                    tag("_"),
                    tag("-"),
                    tag("."),
                    tag("/"),
                    tag(":"),
                ))),
            )),
            |x: &str| !KEYWORDS.contains(&&*x.to_lowercase()),
        ),
        Expression::property,
    )(s)
}

fn parse_and_op(s: &str) -> IResult<&str, Expression> {
    let (rest, lhs) = parse_term(s)?;
    let (rest, _) =
        delimited(multispace1, tag_no_case("and"), multispace1)(rest)?;
    let (rest, rhs) = parse_term(rest)?;
    Ok((rest, Expression::and(lhs, rhs)))
}

fn parse_or_op(s: &str) -> IResult<&str, Expression> {
    let (rest, lhs) = parse_term(s)?;
    let (rest, _) =
        delimited(multispace1, tag_no_case("or"), multispace1)(rest)?;
    let (rest, rhs) = parse_term(rest)?;
    Ok((rest, Expression::or(lhs, rhs)))
}

fn parse_xor_op(s: &str) -> IResult<&str, Expression> {
    let (rest, lhs) = parse_term(s)?;
    let (rest, _) =
        delimited(multispace1, tag_no_case("xor"), multispace1)(rest)?;
    let (rest, rhs) = parse_term(rest)?;
    Ok((rest, Expression::xor(lhs, rhs)))
}

fn parse_inverted(s: &str) -> IResult<&str, Expression> {
    let (rest, _) =
        alt((terminated(tag_no_case("not"), multispace1), tag("!")))(s)?;
    let (rest, expr) = cut(parse_term)(rest)?;
    Ok((rest, Expression::not(expr)))
}

fn parse_wrapped(s: &str) -> IResult<&str, Expression> {
    delimited(
        tag("("),
        delimited(multispace0, cut(parse_expression), multispace0),
        tag(")"),
    )(s)
}

fn parse_term(s: &str) -> IResult<&str, Expression> {
    alt((parse_inverted, parse_wrapped, parse_property))(s)
}

fn parse_expression(s: &str) -> IResult<&str, Expression> {
    delimited(
        multispace0,
        cut(alt((parse_and_op, parse_or_op, parse_xor_op, parse_term))),
        multispace0,
    )(s)
}

fn parse_root(s: &str) -> IResult<&str, Expression> {
    map(delimited(multispace0, tag("*"), multispace0), |_| Expression::Root)(s)
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ExpressionError {
    #[error("parser error {0:?}")]
    ParserError(String),
    #[error("invalid end of input {0:?}")]
    InvalidEndOfInput(String),
    #[error("input can't be longer than {MAX_LENGTH}")]
    InputStringToolLong,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Expression {
    Root,
    Property(String),
    Or(Box<Expression>, Box<Expression>),
    And(Box<Expression>, Box<Expression>),
    Xor(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
}

impl Expression {
    pub fn parse(query: &str) -> Result<Self, ExpressionError> {
        if query.len() > MAX_LENGTH {
            Err(ExpressionError::InputStringToolLong)
        } else {
            match alt((parse_root, parse_expression))(query) {
                Ok((rest, expression)) => {
                    if rest.is_empty() {
                        Ok(expression)
                    } else {
                        Err(ExpressionError::InvalidEndOfInput(rest.to_owned()))
                    }
                }
                Err(e) => Err(ExpressionError::ParserError(format!("{}", e))),
            }
        }
    }

    #[inline]
    pub fn property(name: &str) -> Self {
        Expression::Property(name.to_owned())
    }

    #[inline]
    pub fn or(lhs: Self, rhs: Self) -> Self {
        Expression::Or(Box::new(lhs), Box::new(rhs))
    }

    #[inline]
    pub fn and(lhs: Self, rhs: Self) -> Self {
        Expression::And(Box::new(lhs), Box::new(rhs))
    }

    #[inline]
    pub fn xor(lhs: Self, rhs: Self) -> Self {
        Expression::Xor(Box::new(lhs), Box::new(rhs))
    }

    #[inline]
    pub fn not(expr: Self) -> Self {
        Expression::Not(Box::new(expr))
    }

    pub fn serialize(&self) -> String {
        match self {
            Self::Root => "*".to_owned(),
            Self::Property(name) => name.clone(),
            Self::Not(inner) => format!("not ({})", inner.as_ref().serialize()),
            Self::And(lhs, rhs) => format!(
                "({}) and ({})",
                lhs.as_ref().serialize(),
                rhs.as_ref().serialize()
            ),
            Self::Or(lhs, rhs) => format!(
                "({}) or ({})",
                lhs.as_ref().serialize(),
                rhs.as_ref().serialize()
            ),
            Self::Xor(lhs, rhs) => format!(
                "({}) xor ({})",
                lhs.as_ref().serialize(),
                rhs.as_ref().serialize()
            ),
        }
    }
}

impl FromStr for Expression {
    type Err = ExpressionError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Expression::parse(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case("foo", ("", "foo"))]
    #[case("foo/bar", ("", "foo/bar"))]
    #[case("foo/bar.baz", ("", "foo/bar.baz"))]
    #[case("foo:bar", ("", "foo:bar"))]
    #[case("foo:1221", ("", "foo:1221"))]
    fn parse_valid_property(#[case] value: &str, #[case] result: (&str, &str)) {
        assert_eq!(
            parse_property(value).unwrap(),
            (result.0, Expression::property(result.1))
        );
    }

    #[rstest]
    #[case("")]
    #[case("4foo")]
    #[case(":foo")]
    #[case(".")]
    #[case("/foo")]
    fn parse_invalid_property(#[case] value: &str) {
        assert!(parse_property(value).is_err());
    }

    #[rstest]
    #[case("foo", Expression::property("foo"))]
    #[case("(foo)", Expression::property("foo"))]
    #[case("not foo", Expression::not(Expression::property("foo")))]
    #[case("(not (foo))", Expression::not(Expression::property("foo")))]
    #[case("!foo", Expression::not(Expression::property("foo")))]
    #[case("!(foo)", Expression::not(Expression::property("foo")))]
    #[case(
        "foo and bar",
        Expression::and(
            Expression::property("foo"),
            Expression::property("bar")
        )
    )]
    #[case(
        "foo or bar",
        Expression::or(
            Expression::property("foo"),
            Expression::property("bar")
        )
    )]
    #[case(
        "foo xor bar",
        Expression::xor(
            Expression::property("foo"),
            Expression::property("bar")
        )
    )]
    #[case(
        "foo and not bar",
        Expression::and(
            Expression::property("foo"),
            Expression::not(Expression::property("bar"))
        )
    )]
    #[case(
        "not not not not foo",
        Expression::not(Expression::not(Expression::not(Expression::not(
            Expression::property("foo"),
        ))))
    )]
    #[case(
        "not foo and bar",
        Expression::and(
            Expression::not(Expression::property("foo")),
            Expression::property("bar"),
        )
    )]
    #[case(
        "(not foo) and bar",
        Expression::and(
            Expression::not(Expression::property("foo")),
            Expression::property("bar"),
        )
    )]
    #[case(
        "not (foo and bar)",
        Expression::not(Expression::and(
            Expression::property("foo"),
            Expression::property("bar")
        ),)
    )]
    #[case(
        "(foo and bar) or baz",
        Expression::or(
            Expression::and(
                Expression::property("foo"),
                Expression::property("bar"),
            ),
            Expression::property("baz"),
        )
    )]
    #[case(
        "foo and (bar or baz)",
        Expression::and(
            Expression::property("foo"),
            Expression::or(
                Expression::property("bar"),
                Expression::property("baz"),
            ),
        )
    )]
    fn parse_valid_expression(
        #[case] value: &str,
        #[case] expected: Expression,
    ) {
        assert_eq!(Expression::parse(value).unwrap(), expected);
    }

    #[rstest]
    #[case("")]
    #[case("foo and")]
    #[case("foo or")]
    #[case("foo and bar and")]
    #[case("(foo and bar and (a or b)")]
    #[case(")")]
    #[case("(")]
    #[case("()")]
    #[case("(and)")]
    #[case("foo and bar or baz")]
    fn parse_invalid_expression(#[case] value: &str) {
        assert!(Expression::parse(value).is_err());
    }

    #[rstest]
    #[case("foo")]
    #[case("(foo)")]
    #[case("not foo")]
    #[case("(not (foo))")]
    #[case("!foo")]
    #[case("!(foo)")]
    #[case("foo and bar")]
    #[case("foo or bar")]
    #[case("foo xor bar")]
    #[case("foo and not bar")]
    #[case("not not not not foo")]
    #[case("not foo and bar")]
    #[case("(not foo) and bar")]
    #[case("not (foo and bar)")]
    #[case("(foo and bar) or baz")]
    #[case("foo and (bar or baz)")]
    fn parse_serialize_round_trip(#[case] input: &str) {
        let parsed = Expression::parse(input).unwrap();
        assert_eq!(parsed, Expression::parse(&parsed.serialize()).unwrap());
    }
}
