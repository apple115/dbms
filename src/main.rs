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
    );";

    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql_query).expect("Failed to parse SQL");

    if let Statement::CreateTable { name, columns, .. } = &ast[0] {
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
        let column_names: Vec<String> = columns_info.iter().map(|(name, _)| name.clone()).collect();
        let new_table = Table {
            name: table_name.clone(),
            columns: column_names,
            data: vec![],
        };
        let mut database = Database {
            file_path: String::from("/your/database/file/path"),
            tables: HashMap::new(),
        };
        database.tables.insert(table_name, new_table);

        // Display the database and table info
        println!("Database: {:#?}", database);
    }
}
