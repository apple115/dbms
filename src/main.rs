use sqlparser::ast::{ColumnDef, DataType, Statement};
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
    INSERT INTO my_table VALUES (1, 'Alice', 30);
    SELECT id, name FROM my_table;
";

    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql_query).expect("Failed to parse SQL");
    // println!("{:#?}", ast);

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

            Statement::Query(ref query) => {
                if let sqlparser::ast::Query { body, .. } = query.as_ref() {
                    if let sqlparser::ast::SetExpr::Select(select) = &**body {
                        if let sqlparser::ast::TableFactor::Table { name, .. } =
                            &select.from[0].relation
                        {
                            let table_name = name.to_string();
                            let selected_columns: Vec<String> = select
                                .projection
                                .iter()
                                .map(|p| match p {
                                    sqlparser::ast::SelectItem::UnnamedExpr(expr) => match expr {
                                        sqlparser::ast::Expr::Identifier(ident) => {
                                            ident.value.clone()
                                        }
                                        _ => panic!("Unsupported selection expression"),
                                    },
                                    _ => panic!("Unsupported selection item"),
                                })
                                .collect();

                            println!("Table name: {}", table_name);
                            println!("Selected columns: {:?}", selected_columns);
                            // Output the selected columns' data only
                            let table_data = &database.tables[&table_name].data;
                            let selected_table_data: Vec<Vec<String>> = table_data
                                .iter()
                                .map(|row| {
                                    let mut selected_row_data: Vec<String> = Vec::new();
                                    for (col, val) in row.iter().enumerate() {
                                        if selected_columns
                                            .contains(&database.tables[&table_name].columns[col])
                                        {
                                            selected_row_data.push(val.clone());
                                        }
                                    }
                                    selected_row_data
                                })
                                .collect();
                            println!("Selected table data: {:?}", selected_table_data);
                        } else {
                            panic!("No table name provided");
                        }
                    } else {
                        panic!("Unsupported query type");
                    }
                }
            }
            _ => panic!("Unsupported SQL statement"),
        }
    }
    println!("{:#?}", database);
}
