use super::database::Database;
use super::Table;
use sqlparser::ast::Statement;
use sqlparser::ast::{ColumnDef, DataType};

pub fn execute_queries(database: &mut Database, ast: Vec<Statement>) {
    // Execute the parsed SQL statements on the database
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
                                    sqlparser::ast::SelectItem::Wildcard(_) => {
                                        // Handle selecting all columns
                                        // Here, you might fetch columns' names from the database, if available
                                        database.tables[&table_name]
                                            .columns
                                            .iter()
                                            .map(|col| col.to_string())
                                            .collect()
                                    }
                                    sqlparser::ast::SelectItem::UnnamedExpr(expr) => match expr {
                                        sqlparser::ast::Expr::Identifier(ident) => {
                                            vec![ident.value.clone()]
                                        }
                                        _ => panic!("Unsupported selection expression"),
                                    },
                                    _ => panic!("Unsupported selection item"),
                                })
                                .flatten() // Flatten the Vec<Option<String>> to Vec<String>
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

            Statement::Delete {
                from, selection, ..
            } => {
                if let sqlparser::ast::TableWithJoins {
                    relation, joins, ..
                } = &from[0]
                {
                    if let sqlparser::ast::TableFactor::Table { name, .. } = relation {
                        let table_name = name.to_string();
                        println!("Table name: {}", table_name);

                        match selection {
                            Some(select_condition) => {
                                if let sqlparser::ast::Expr::BinaryOp { left, op, right } =
                                    select_condition
                                {
                                    match (*left, op, *right) {
                                        (
                                            sqlparser::ast::Expr::Identifier(ident),
                                            sqlparser::ast::BinaryOperator::Eq,
                                            sqlparser::ast::Expr::Value(value),
                                        ) => match ident.value.as_str() {
                                            "id" => {
                                                if let sqlparser::ast::Value::Number(id, _) = value
                                                {
                                                    if let Some(table) =
                                                        database.tables.get_mut(&table_name)
                                                    {
                                                        let condition_column = "id";
                                                        if let Some(index) = table
                                                            .columns
                                                            .iter()
                                                            .position(|col| col == condition_column)
                                                        {
                                                            table.data.retain(|row| {
                                                                if let Some(val) = row.get(index) {
                                                                    *val != id
                                                                } else {
                                                                    true
                                                                }
                                                            });
                                                            println!(
                                                                "Deleted data from table: {:?}",
                                                                &table_name
                                                            );
                                                        } else {
                                                            panic!("Condition column does not exist in the table");
                                                        }
                                                    } else {
                                                        panic!("Table not found in the database");
                                                    }
                                                } else {
                                                    panic!(
                                                        "Unsupported condition value for deletion"
                                                    );
                                                }
                                            }
                                            "name" => {
                                                if let sqlparser::ast::Value::SingleQuotedString(
                                                    name,
                                                ) = value
                                                {
                                                    if let Some(table) =
                                                        database.tables.get_mut(&table_name)
                                                    {
                                                        let condition_column = "name";
                                                        if let Some(index) = table
                                                            .columns
                                                            .iter()
                                                            .position(|col| col == condition_column)
                                                        {
                                                            table.data.retain(|row| {
                                                                if let Some(val) = row.get(index) {
                                                                    *val != name
                                                                } else {
                                                                    true
                                                                }
                                                            });
                                                            println!(
                                                                "Deleted data from table: {:?}",
                                                                &table_name
                                                            );
                                                        } else {
                                                            panic!("Condition column does not exist in the table");
                                                        }
                                                    } else {
                                                        panic!("Table not found in the database");
                                                    }
                                                } else {
                                                    panic!(
                                                        "Unsupported condition value for deletion"
                                                    );
                                                }
                                            }
                                            "age" => {
                                                if let sqlparser::ast::Value::Number(age, _) = value
                                                {
                                                    if let Some(table) =
                                                        database.tables.get_mut(&table_name)
                                                    {
                                                        let condition_column = "age";
                                                        if let Some(index) = table
                                                            .columns
                                                            .iter()
                                                            .position(|col| col == condition_column)
                                                        {
                                                            table.data.retain(|row| {
                                                                if let Some(val) = row.get(index) {
                                                                    *val != age
                                                                } else {
                                                                    true
                                                                }
                                                            });
                                                            println!(
                                                                "Deleted data from table: {:?}",
                                                                &table_name
                                                            );
                                                        } else {
                                                            panic!("Condition column does not exist in the table");
                                                        }
                                                    } else {
                                                        panic!("Table not found in the database");
                                                    }
                                                } else {
                                                    panic!(
                                                        "Unsupported condition value for deletion"
                                                    );
                                                }
                                            }
                                            _ => panic!("Unsupported column for deletion"),
                                        },
                                        _ => panic!("Unsupported condition structure for deletion"),
                                    }
                                } else {
                                    panic!("Unsupported condition for deletion");
                                }
                            }
                            None => {
                                if let Some(table) = database.tables.get_mut(&table_name) {
                                    // Remove all rows
                                    table.data.clear();
                                    println!("Deleted all data from table: {:?}", &table_name);
                                } else {
                                    panic!("Table not found in the database");
                                }
                            }
                        }
                    } else {
                        panic!("No table name provided for deletion");
                    }
                } else {
                    panic!("No table specified for deletion");
                }
            }

            _ => panic!("Unsupported SQL statement"),
        }
    }
}
