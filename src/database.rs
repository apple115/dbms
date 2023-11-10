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

impl Database {
    pub fn new() -> Self {
        Database {
            file_path: String::from("/your/database/file/path"),
            tables: HashMap::new(),
        }
    }
    // Add other database-related methods here
}
