use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn parse_sql_queries(sql_query: &str) -> Vec<sqlparser::ast::Statement> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql_query).expect("Failed to parse SQL");
    ast
}
