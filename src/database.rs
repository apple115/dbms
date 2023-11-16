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
            file.write_all(b"Columns ")?;
            let columns_str = table.columns.join(",");
            file.write_all(columns_str.as_bytes())?;
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
            if line.starts_with("Table:") {
                // Save the previous table data
                if !current_table_name.is_empty() {
                    self.tables.insert(
                        current_table_name.clone(),
                        Table {
                            name: current_table_name.clone(),
                            columns: current_columns.clone(),
                            data: current_data.clone(),
                        },
                    );
                }

                // Start parsing a new table
                current_table_name = line.trim_start_matches("Table:").trim().to_string();
                current_columns.clear();
                current_data.clear();
            } else if line.starts_with("Columns") {
                // Parse column names
                current_columns = line
                    .trim_start_matches("Columns")
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
            } else if !line.trim().is_empty() {
                // Parse data rows
                let row_data: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
                current_data.push(row_data);
            }
        }

        // Save the last table data
        if !current_table_name.is_empty() {
            self.tables.insert(
                current_table_name.clone(),
                Table {
                    name: current_table_name.clone(),
                    columns: current_columns.clone(),
                    data: current_data.clone(),
                },
            );
        }

        Ok(())
    }
    // Add other database-related methods here
}
