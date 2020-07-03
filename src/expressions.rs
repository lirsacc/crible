use boolean_expression::Expr;
use pest::Parser;

#[derive(Parser)]
#[grammar = "logic.pest"]
struct LogicParser;

type Pair<'i> = pest::iterators::Pair<'i, Rule>;
pub type ParseError = pest::error::Error<Rule>;

pub fn parse_expression(source: &str) -> Result<Expr<String>, ParseError> {
    let mut pairs = LogicParser::parse(Rule::query, source)?;

    // Should have only one query with a single or_term if parsing was
    // successful so we can bypass when parsing.
    Ok(parse_pair(
        pairs.next().unwrap().into_inner().next().unwrap(),
    ))
}

fn parse_pair(pair: Pair) -> Expr<String> {
    match pair.as_rule() {
        Rule::or_term => {
            let mut inner = pair.into_inner();
            let lhs = parse_pair(inner.next().unwrap());
            match inner.next() {
                None => lhs,
                Some(_) => Expr::Or(
                    Box::new(lhs),
                    Box::new(parse_pair(inner.next().unwrap())),
                ),
            }
        }
        Rule::and_term => {
            let mut inner = pair.into_inner();
            let lhs = parse_pair(inner.next().unwrap());
            match inner.next() {
                None => lhs,
                Some(_) => Expr::And(
                    Box::new(lhs),
                    Box::new(parse_pair(inner.next().unwrap())),
                ),
            }
        }
        Rule::factor => {
            let mut inner = pair.into_inner();
            let leader = inner.next().unwrap();
            match leader.as_rule() {
                Rule::not => {
                    Expr::Not(Box::new(parse_pair(inner.next().unwrap())))
                }
                _ => parse_pair(leader),
            }
        }
        Rule::primary => parse_pair(pair.into_inner().next().unwrap()),
        Rule::token => Expr::Terminal(pair.as_str().to_owned()),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {

    use super::parse_expression;
    use boolean_expression::Expr;
    use parameterized::parameterized;

    fn assert_parses(value: &str) {
        parse_expression(value).unwrap();
    }

    #[parameterized(
    input = {
        "foo",
        "(foo)",
        "(foo AND bar)",
        "foo AND bar",
        "(foo OR bar)",
        "((foo OR bar) AND baz)",
        "foo OR bar AND baz",
        "NOT foo",
        "NOT NOT foo",
        "NOT foo AND bar",
        "NOT (foo AND bar)"
    })]
    fn it_parses_valid_queries(input: &str) {
        assert_parses(input);
    }

    fn t(x: &str) -> Expr<String> {
        Expr::Terminal(x.to_owned())
    }

    #[test]
    fn it_parses_single_token_correctly() {
        assert_eq!(t("foo"), parse_expression("foo").unwrap());
    }

    #[test]
    fn it_parses_single_parens_token_correctly() {
        assert_eq!(t("foo"), parse_expression("(foo)").unwrap());
    }

    #[test]
    fn it_parses_complex_expression_correctly() {
        assert_eq!(
            Expr::Not(Box::new(Expr::And(
                Box::new(Expr::Or(
                    Box::new(Expr::And(
                        Box::new(t("a")),
                        Box::new(Expr::Or(Box::new(t("b")), Box::new(t("c")))),
                    )),
                    Box::new(t("d")),
                )),
                Box::new(Expr::Not(Box::new(t("e")))),
            ))),
            parse_expression("NOT ((a AND (b OR c) OR d) AND NOT e)").unwrap()
        );
    }
}
