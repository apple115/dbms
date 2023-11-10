use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use std::io::{BufRead, BufReader, Write};
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

impl Database {
    pub fn new() -> Self {
        Database {
            file_path: String::from("db.txt"),
            tables: HashMap::new(),
        }
    }

    pub fn save(&self) -> std::io::Result<()> {
        let mut file = File::create(&self.file_path)?;
        for (_, table) in &self.tables {
            file.write_all(format!("Table: {}\n", table.name).as_bytes())?;
            for column in &table.columns {
                file.write_all(column.as_bytes())?;
                file.write_all(b",")?;
            }
            file.write_all(b"\n")?;
            for row in &table.data {
                let row_str = row.join(",");
                file.write_all(row_str.as_bytes())?;
                file.write_all(b"\n")?;
            }
            file.write_all(b"\n")?;
        }
        Ok(())
    }

    pub fn load(&mut self) -> std::io::Result<()> {
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);

        let mut current_table_name = String::new();
        let mut current_columns: Vec<String> = Vec::new();
        let mut current_data: Vec<Vec<String>> = Vec::new();

        for line in reader.lines() {
            let line = line?;

            if line.starts_with("Table: ") {
                // Save the previous table data
                if !current_table_name.is_empty() {
                    let table = Table {
                        name: current_table_name.clone(),
                        columns: current_columns.clone(),
                        data: current_data.clone(),
                    };
                    self.tables.insert(current_table_name.clone(), table);

                    // Reset for the next table
                    current_columns.clear();
                    current_data.clear();
                }

                // Extract the table name from the line
                current_table_name = line.trim_start_matches("Table: ").to_string();
            } else if !line.is_empty() {
                // Split the line into columns and add to current_data
                let row: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
                current_data.push(row.clone());

                // If columns are not set, set them based on the first row
                if current_columns.is_empty() {
                    current_columns = row;
                }
            }
        }

        // Save the last table data
        if !current_table_name.is_empty() {
            let table = Table {
                name: current_table_name.clone(),
                columns: current_columns,
                data: current_data,
            };
            self.tables.insert(current_table_name, table);
        }

        Ok(())
    }
    // Add other database-related methods here
}
