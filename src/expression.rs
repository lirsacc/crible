//! This module implements all the logic related to parsing and representing
//! boolean queries over properties.

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

const MAX_LENGTH: usize = 2048;

/** Rough grammar for the nom parser:
 *
 * <property> = [A-Za-z][A-Za-z0-9-_\.\/\:]*
 *
 * <and-operation> = <term> \s+ { "and" | "AND" } \s+ <term>
 * <or-operation> = <term> \s+ { "or" | "OR" } \s+ <term>
 * <xor-operation> = <term> \s+ { "xor" | "XOR" } \s+ <term>
 *
 * <inverted> = "not" \s+ <expression>
 * <wrapped> = "(" \s* <expression> \s* ")"
 * <subexpression> = <and-operation> | <or-operation> | <xor-operation> | <term>
 *
 * <term> = <inverted> | <wrapped> | <property>
 *
 * <root> = "*"
 *
 * <expression> = \s* { <root> | <subexpression> } \s*
**/

const KEYWORDS: [&str; 4] = ["not", "and", "xor", "or"];

fn parse_property(s: &str) -> IResult<&str, Expression> {
    map(
        verify(
            recognize(pair(
                // Properties start with a letter
                alpha1,
                // They can then be any combination of letter, digit and separator ([-_./:])
                many0(alt((
                    alphanumeric1,
                    tag("_"),
                    tag("-"),
                    tag("."),
                    tag("/"),
                    tag(":"),
                ))),
            )),
            // As long as they don't conflict with existing keywords
            // TODO: is there a better way to do this than `verify(...)`?
            |x: &str| !KEYWORDS.contains(&&*x.to_lowercase()),
        ),
        Expression::property,
    )(s)
}

// Operations (and, xor, or) are pairs of terms separated with a fixed operator.
// The main consequence of this is that we do not support mixed operators in the
// same operation, e.g. "A and B or C" would require disambiguating through
// precedence and is considered invalid. Such queries must be spelled out using
// parenthesis so "(A and B) or C" for the natural interpretation of the
// previous example. This is purely to simplify the parsing / grammar and given
// the use case where operations should be built by machines this is an
// acceptable tradeoff.

fn parse_and_operation(s: &str) -> IResult<&str, Expression> {
    let (rest, lhs) = parse_term(s)?;
    let (rest, _) =
        delimited(multispace1, tag_no_case("and"), multispace1)(rest)?;
    let (rest, rhs) = parse_term(rest)?;
    Ok((rest, Expression::and(lhs, rhs)))
}

fn parse_or_operation(s: &str) -> IResult<&str, Expression> {
    let (rest, lhs) = parse_term(s)?;
    let (rest, _) =
        delimited(multispace1, tag_no_case("or"), multispace1)(rest)?;
    let (rest, rhs) = parse_term(rest)?;
    Ok((rest, Expression::or(lhs, rhs)))
}

fn parse_xor_operation(s: &str) -> IResult<&str, Expression> {
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
        delimited(
            multispace0,
            // `cut` here essentially says there must be a non empty
            // subexpression between pairs of ()
            cut(parse_subexpression),
            multispace0,
        ),
        tag(")"),
    )(s)
}

fn parse_term(s: &str) -> IResult<&str, Expression> {
    alt((parse_inverted, parse_wrapped, parse_property))(s)
}

fn parse_subexpression(s: &str) -> IResult<&str, Expression> {
    delimited(
        multispace0,
        cut(alt((
            parse_and_operation,
            parse_or_operation,
            parse_xor_operation,
            parse_term,
        ))),
        multispace0,
    )(s)
}

fn parse_root(s: &str) -> IResult<&str, Expression> {
    map(delimited(multispace0, tag("*"), multispace0), |_| Expression::Root)(s)
}

fn parse_expression(s: &str) -> IResult<&str, Expression> {
    alt((
        // '*' is a valid query when used standalone. It's invalid used anywhere
        // else. There's no further validation that the root term can only occur
        // alone, so this is only true for parsed queries.
        parse_root,
        parse_subexpression,
    ))(s)
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
/// A boolean expression.
pub enum Expression {
    Root,
    Property(String),
    Or(Box<Expression>, Box<Expression>),
    And(Box<Expression>, Box<Expression>),
    Xor(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
}

impl Expression {
    pub fn parse(input: &str) -> Result<Self, ExpressionError> {
        if input.len() > MAX_LENGTH {
            Err(ExpressionError::InputStringToolLong)
        } else {
            match parse_expression(input) {
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

    // Helpers to build Expressions with less characters.
    // TODO: Should we implement corresponding std::ops traits instead?
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

    // This should provide a _canonical_ representation of a query ignoring
    // whitespace and parenthesis. Useful for caching / deduplication / etc.
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
