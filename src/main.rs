use sqlparser::ast::{ColumnDef, DataType, Statement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<String>,
    pub data: Vec<Vec<String>>,
}

#[derive(Debug)]
pub struct Database {
    file_path: String,
    pub tables: HashMap<String, Table>,
}

fn main() {
    let sql_query = "CREATE TABLE my_table (
        id INT,
        name VARCHAR(255),
        age INT
    );
    INSERT INTO my_table VALUES (1, 'Alice', 30);";

    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql_query).expect("Failed to parse SQL");
    //println!("{:#?}", ast);

    let mut database = Database {
        file_path: String::from("/your/database/file/path"),
        tables: HashMap::new(),
    };

    for statement in ast {
        match statement {
            Statement::CreateTable { name, columns, .. } => {
                let table_name = name.to_string();
                let columns_info: Vec<(String, DataType)> = columns
                    .iter()
                    .map(|col| {
                        let ColumnDef {
                            name, data_type, ..
                        } = col;
                        (name.to_string(), data_type.clone())
                    })
                    .collect();
                let column_names: Vec<String> =
                    columns_info.iter().map(|(name, _)| name.clone()).collect();
                let new_table = Table {
                    name: table_name.clone(),
                    columns: column_names,
                    data: vec![],
                };
                database.tables.insert(table_name, new_table);

                // Display the database and table info
            }

            Statement::Insert {
                table_name, source, ..
            } => {
                let table_name = table_name.to_string();
                let table = database
                    .tables
                    .get_mut(&table_name)
                    .expect("Table does not exist");

                if let sqlparser::ast::Query { body, .. } = *source {
                    if let sqlparser::ast::SetExpr::Values(values) = *body {
                        for row in values.rows {
                            let row_values: Vec<String> =
                                row.into_iter()
                                    .map(|val| match val {
                                        sqlparser::ast::Expr::Value(
                                            sqlparser::ast::Value::Number(n, _),
                                        ) => n.to_string(),
                                        sqlparser::ast::Expr::Value(
                                            sqlparser::ast::Value::SingleQuotedString(s),
                                        ) => s,
                                        _ => panic!("Unsupported value type"),
                                    })
                                    .collect();
                            table.data.push(row_values);
                        }
                    }
                }
            }
            _ => panic!("Unsupported SQL statement"),
        }
    }
    println!("{:#?}", database);
}
