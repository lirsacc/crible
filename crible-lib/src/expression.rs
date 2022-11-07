//! This module implements all the logic related to parsing and representing
//! boolean queries over properties.

// TODO: Handle symbols?
// TODO: Better error handling?
// TODO: Fuzzy precedence?

use std::ops::{BitAnd, BitOr, BitXor, Not, Sub};
use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{
    alpha1, alphanumeric1, multispace0, multispace1,
};
use nom::combinator::{cut, map, recognize, verify};
use nom::multi::{many0, many1};
use nom::sequence::{delimited, pair, terminated};
use nom::IResult;
use thiserror::Error;

const MAX_LENGTH: usize = 2048;

// Rough grammar for the nom parser
// =======================================================================
//
// <property> = [A-Za-z][A-Za-z0-9-_\.\/\:]*
//
// <and-operation> = <term> \s+ { { "and" | "AND" } \s+ <term> }+
// <or-operation> = <term> \s+ { { "or" | "OR" } \s+ <term> }+
// <xor-operation> = <term> \s+ { { "xor" | "XOR" } \s+ <term> }+
// <sub-operation> = <term> \s+ { { "-" } \s+ <term> }+
//
// <inverted> = "not" \s+ <expression>
// <wrapped> = "(" \s* <expression> \s* ")"
//
// <subexpression> = <and-operation>
//                 | <or-operation>
//                 | <xor-operation>
//                 | <sub-operation>
//                 | <term>
//
// <term> = <inverted> | <wrapped> | <property>
//
// <root> = "*"
//
// <expression> = \s* { <root> | <subexpression> } \s*
//
// =======================================================================

const KEYWORDS: [&str; 4] = ["not", "and", "xor", "or"];

fn parse_property(s: &str) -> IResult<&str, Expression> {
    map(
        verify(
            recognize(pair(
                // Properties start with a letter
                alpha1,
                // They can then be any combination of letter, digit and
                // separator ([-_./:])
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

pub(crate) fn validate_property_name(s: &str) -> bool {
    parse_property(s).map_or(false, |(rest, _)| rest.is_empty())
}

// Operations (and, xor, or) are pairs of terms separated with a fixed operator.
// The main consequence of this is that we do not support mixed operators in the
// same operation, e.g. "A and B or C" would require disambiguating through
// precedence and is considered invalid. Such queries must be spelled out using
// parenthesis so "(A and B) or C" for the natural interpretation of the
// previous example. This is purely to simplify the parsing / grammar and given
// the use case where operations should be built by machines this is an
// acceptable tradeoff.

type ParseResult<'a> = IResult<&'a str, Expression>;

fn repeated_op(keyword: &'static str) -> impl Fn(&str) -> ParseResult {
    move |s: &str| -> ParseResult {
        let (rest, _) =
            delimited(multispace1, tag_no_case(keyword), multispace1)(s)?;
        let (rest, rhs) = parse_term(rest)?;
        Ok((rest, rhs))
    }
}

fn op(
    keyword: &'static str,
) -> impl Fn(&str) -> IResult<&str, Vec<Expression>> {
    move |s: &str| -> IResult<&str, Vec<Expression>> {
        let (rest, lhs) = parse_term(s)?;
        let (rest, mut expressions) = many1(repeated_op(keyword))(rest)?;
        let mut v: Vec<Expression> = vec![lhs];
        v.append(&mut expressions);
        Ok((rest, v))
    }
}

fn parse_and_operation(s: &str) -> ParseResult {
    let (rest, v) = op("and")(s)?;
    Ok((rest, Expression::And(v)))
}

fn parse_or_operation(s: &str) -> ParseResult {
    let (rest, v) = op("or")(s)?;
    Ok((rest, Expression::Or(v)))
}

fn parse_xor_operation(s: &str) -> ParseResult {
    let (rest, v) = op("xor")(s)?;
    Ok((rest, Expression::Xor(v)))
}

fn parse_sub_operation(s: &str) -> ParseResult {
    let (rest, v) = op("-")(s)?;
    Ok((rest, Expression::Sub(v)))
}

fn parse_inverted(s: &str) -> ParseResult {
    let (rest, _) =
        alt((terminated(tag_no_case("not"), multispace1), tag("!")))(s)?;
    let (rest, expr) = cut(parse_term)(rest)?;
    Ok((rest, Expression::not(expr)))
}

fn parse_wrapped(s: &str) -> ParseResult {
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

fn parse_term(s: &str) -> ParseResult {
    alt((parse_inverted, parse_wrapped, parse_property))(s)
}

fn parse_subexpression(s: &str) -> ParseResult {
    delimited(
        multispace0,
        cut(alt((
            parse_and_operation,
            parse_or_operation,
            parse_xor_operation,
            parse_sub_operation,
            parse_term,
        ))),
        multispace0,
    )(s)
}

fn parse_root(s: &str) -> ParseResult {
    map(delimited(multispace0, tag("*"), multispace0), |_| Expression::Root)(s)
}

fn parse_expression(s: &str) -> ParseResult {
    alt((
        // '*' is a valid query when used standalone. It's invalid used
        // anywhere else. There's no further validation that the root
        // term can only occur alone, so this is only true for parsed
        // queries.
        parse_root,
        parse_subexpression,
    ))(s)
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("parser error {0:?}")]
    Invalid(String),
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
    Or(Vec<Expression>),
    And(Vec<Expression>),
    Xor(Vec<Expression>),
    Sub(Vec<Expression>),
    Not(Box<Expression>),
}

#[inline]
fn join(sep: &'static str, expressions: &[Expression]) -> String {
    if expressions.len() > 1 {
        format!(
            "({})",
            expressions[1..].iter().fold(
                expressions[0].serialize(),
                |mut s, e| {
                    s.push_str(sep);
                    s.push_str(&e.serialize());
                    s
                }
            )
        )
    } else {
        expressions[0].serialize()
    }
}

impl Expression {
    pub fn parse(input: &str) -> Result<Self, Error> {
        if input.len() > MAX_LENGTH {
            Err(Error::InputStringToolLong)
        } else {
            match parse_expression(input) {
                Ok((rest, expression)) => {
                    if rest.is_empty() {
                        Ok(expression)
                    } else {
                        Err(Error::InvalidEndOfInput(rest.to_owned()))
                    }
                }
                Err(e) => Err(Error::Invalid(format!("{}", e))),
            }
        }
    }

    // Helpers to build Expressions with less characters.
    // TODO: Should we implement corresponding std::ops traits instead?
    #[inline]
    pub fn property(name: &str) -> Self {
        Expression::Property(name.to_owned())
    }

    // This should provide a _canonical_ representation of a query ignoring
    // whitespace and parenthesis. Useful for caching / deduplication / etc.
    pub fn serialize(&self) -> String {
        match self {
            Self::Root => "*".to_owned(),
            Self::Property(name) => name.clone(),
            Self::Not(inner) => format!("not ({})", inner.as_ref().serialize()),
            Self::And(inner) => join(" and ", inner),
            Self::Or(inner) => join(" or ", inner),
            Self::Xor(inner) => join(" xor ", inner),
            Self::Sub(inner) => join(" - ", inner),
        }
    }
}

impl FromStr for Expression {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Expression::parse(value)
    }
}

impl BitOr for Expression {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Expression::Or(l), Expression::Or(r)) => {
                Expression::Or([r, l].concat())
            }
            (Expression::Or(l), r) => Expression::Or([l, vec![r]].concat()),
            (l, Expression::Or(r)) => Expression::Or([vec![l], r].concat()),
            (l, r) => Expression::Or(vec![l, r]),
        }
    }
}

impl BitAnd for Expression {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Expression::And(l), Expression::And(r)) => {
                Expression::And([r, l].concat())
            }
            (Expression::And(l), r) => Expression::And([l, vec![r]].concat()),
            (l, Expression::And(r)) => Expression::And([vec![l], r].concat()),
            (l, r) => Expression::And(vec![l, r]),
        }
    }
}

impl BitXor for Expression {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Expression::Xor(l), Expression::Xor(r)) => {
                Expression::Xor([r, l].concat())
            }
            (Expression::Xor(l), r) => Expression::Xor([l, vec![r]].concat()),
            (l, Expression::Xor(r)) => Expression::Xor([vec![l], r].concat()),
            (l, r) => Expression::Xor(vec![l, r]),
        }
    }
}

impl Sub for Expression {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Expression::Sub(l), Expression::Sub(r)) => {
                Expression::Sub([r, l].concat())
            }
            (Expression::Sub(l), r) => Expression::Sub([l, vec![r]].concat()),
            (l, Expression::Sub(r)) => Expression::Sub([vec![l], r].concat()),
            (l, r) => Expression::Sub(vec![l, r]),
        }
    }
}

impl Not for Expression {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Expression::Not(e) => *e,
            e => Expression::Not(Box::new(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    // Write less verbose tests
    type E = Expression;

    fn p(s: &'static str) -> Expression {
        Expression::Property(s.to_owned())
    }

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
        assert!(validate_property_name(value));
    }

    #[rstest]
    #[case("")]
    #[case("4foo")]
    #[case(":foo")]
    #[case(".")]
    #[case("/foo")]
    fn parse_invalid_property(#[case] value: &str) {
        assert!(parse_property(value).is_err());
        assert!(!validate_property_name(value));
    }

    #[rstest]
    #[case("foo", p("foo"))]
    #[case("(foo)", p("foo"))]
    #[case("not foo", E::not(p("foo")))]
    #[case("(not (foo))", E::not(p("foo")))]
    #[case("!foo", E::not(p("foo")))]
    #[case("!(foo)", E::not(p("foo")))]
    #[case("foo and bar", p("foo") & p("bar"))]
    #[case("foo and bar and baz", E::And(vec![p("foo"), p("bar"), p("baz")]))]
    #[case("foo or bar", p("foo") | p("bar"))]
    #[case("foo or bar or baz", E::Or(vec![p("foo"), p("bar"), p("baz")]))]
    #[case("foo xor bar", p("foo") ^ p("bar"))]
    #[case("foo xor bar xor baz", E::Xor(vec![p("foo"), p("bar"), p("baz")]))]
    #[case("foo and not bar", p("foo") & E::not(p("bar")))]
    #[case("not not not not foo", E::not(E::not(E::not(E::not(p("foo"))))))]
    #[case("not foo and bar", E::not(p("foo")) & p("bar"))]
    #[case("(not foo) and bar", E::not(p("foo")) & p("bar"))]
    #[case("not (foo and bar)", E::not(p("foo") & p("bar")))]
    #[case(
        "(foo and bar) or baz",
        (p("foo") & p("bar")) | p("baz")
    )]
    #[case(
        "foo and (bar or baz)",
        p("foo") & (p("bar") | p("baz"))
    )]
    #[case("foo - (bar or baz)", p("foo") - (p("bar") | p("baz")))]
    #[case(
        "foo - (bar or baz) - (foo and bar and baz)",
        E::Sub(
            vec![
                p("foo"),
                (p("bar") | p("baz")),
                E::And(vec![p("foo"), p("bar"), p("baz")]),
            ]
        )
    )]
    #[case(
        "foo - (bar or baz) - (foo and (bar and baz and bam))",
        E::Sub(
            vec![
                p("foo"),
                p("bar") | p("baz"),
                E::And(vec![p("foo"), E::And(vec![
                    p("bar"),
                    p("baz"),
                    p("bam"),
                ])]),
            ]
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
    #[case("foo and bar and baz and")]
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
    #[case("foo - (bar or baz) - (foo and bar and baz)")]
    #[case("foo - (bar or baz) - (foo and (bar and baz and bam))")]
    fn parse_serialize_round_trip(#[case] input: &str) {
        let parsed = Expression::parse(input).unwrap();
        assert_eq!(parsed, Expression::parse(&parsed.serialize()).unwrap());
    }
}
