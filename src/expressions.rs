use boolean_expression::Expr;
use pest::Parser;

#[derive(Parser)]
#[grammar = "logic.pest"]
struct LogicParser;

type Pair<'i> = pest::iterators::Pair<'i, Rule>;
pub type ParseError = pest::error::Error<Rule>;

// TODO: Lots of unwrap, review them.

pub fn parse_expression(source: &str) -> Result<Expr<String>, ParseError> {
    let mut pairs = LogicParser::parse(Rule::expr, source)?;

    // Should have only one expression with a single query if parsing was
    // successful.
    let query = pairs.next().unwrap().into_inner().next().unwrap();
    Ok(parse_pair(query))
}

fn parse_pair(pair: Pair) -> Expr<String> {
    match pair.as_rule() {
        Rule::query => parse_pair(pair.into_inner().next().unwrap()),
        // Temporarily dropped the grammar for negation as a root negation is not
        // supported given that it could require inverting a single index which
        // is specifically not what this is targeted at. It should be re-added
        // with the restriction that it cannot be used pon the root expression.
        // Rule::negated_term => {
        //     let mut inner = pair.into_inner();
        //     inner.next().unwrap();
        //     Expr::Not(Box::new(parse_pair(inner.next().unwrap())))
        // }
        Rule::term => {
            let mut inner = pair.into_inner();
            let first = inner.next().unwrap();
            match first.as_rule() {
                Rule::facet => Expr::Terminal(first.as_str().to_owned()),
                Rule::query => {
                    let rhs = Box::new(parse_pair(first));
                    let operator = inner.next().unwrap();
                    let lhs = Box::new(parse_pair(inner.next().unwrap()));
                    match operator.as_rule() {
                        Rule::and => Expr::And(lhs, rhs),
                        Rule::or => Expr::Or(lhs, rhs),
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}
