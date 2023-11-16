use super::database::Database;
use super::Table;
use sqlparser::ast::Statement;
use sqlparser::ast::{ColumnDef, DataType};
use std::collections::HashMap;

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
            //select * from my_table,email_table where my_table.id = email_table.id
            Statement::Query(ref query) => {
                if let sqlparser::ast::Query { body, .. } = query.as_ref() {
                    if let sqlparser::ast::SetExpr::Select(select) = &**body {
                        // Ensure there are tables to select from
                        if !select.from.is_empty() {
                            // Get the first table
                            if let sqlparser::ast::TableWithJoins { relation, .. } = &select.from[0]
                            {
                                if let sqlparser::ast::TableFactor::Table { name, .. } = relation {
                                    let mut table_name = name.to_string();
                                    let mut selected_columns: Vec<String> = Vec::new();

                                    // Get the columns for the first table
                                    if let Some(first_table) = database.tables.get(&table_name) {
                                        // Get all columns as selected_columns for the Cartesian product
                                        selected_columns.extend(
                                            first_table
                                                .columns
                                                .iter()
                                                .map(|col| format!("{}.{}", table_name, col)),
                                        );
                                    } else {
                                        panic!("Table not found in the database");
                                    }

                                    // Get the data of the first table
                                    let mut joined_table_data: Vec<Vec<String>> =
                                        database.tables[&table_name].data.clone();

                                    // Iterate over the remaining tables in the FROM clause
                                    for table_with_joins in &select.from[1..] {
                                        if let sqlparser::ast::TableWithJoins { relation, .. } =
                                            table_with_joins
                                        {
                                            if let sqlparser::ast::TableFactor::Table {
                                                name: second_table_name,
                                                ..
                                            } = relation
                                            {
                                                // Get the data of the second table
                                                let second_table_data = &database.tables
                                                    [second_table_name.to_string().as_str()]
                                                .data;

                                                // Perform a Cartesian product with the second table
                                                joined_table_data = joined_table_data
                                                    .into_iter()
                                                    .flat_map(|row| {
                                                        second_table_data.iter().map(
                                                            move |second_row| {
                                                                row.iter()
                                                                    .cloned()
                                                                    .chain(
                                                                        second_row.iter().cloned(),
                                                                    )
                                                                    .collect()
                                                            },
                                                        )
                                                    })
                                                    .collect();

                                                // Add the columns from the second table with a prefix
                                                let second_table_columns: Vec<String> = database
                                                    .tables[second_table_name.to_string().as_str()]
                                                .columns
                                                .iter()
                                                .map(|col| format!("{}.{}", second_table_name, col))
                                                .collect();
                                                selected_columns.extend(second_table_columns);
                                            } else {
                                                panic!(
                                                    "Table in FROM clause is not a regular table"
                                                );
                                            }
                                        }
                                    }

                                    println!("Table name: {}", table_name);
                                    println!("Selected columns: {:?}", selected_columns);
                                    println!("table:{:#?}", joined_table_data);
                                    // Filter the joined data based on the WHERE clause
                                    let filtered_table_data: Vec<Vec<String>> = joined_table_data
                                        .iter()
                                        .filter(|row| {
                                            if let Some(selection_condition) = &select.selection {
                                                match evaluate_condition(
                                                    selection_condition,
                                                    row,
                                                    &selected_columns,
                                                    database,
                                                ) {
                                                    Ok(result) => result,
                                                    Err(err) => {
                                                        eprintln!(
                                                            "Error evaluating WHERE clause: {}",
                                                            err
                                                        );
                                                        false
                                                    }
                                                }
                                            } else {
                                                // If there is no WHERE clause, include all rows
                                                true
                                            }
                                        })
                                        .map(|row| {
                                            let mut selected_row_data: Vec<String> = Vec::new();
                                            for col_name in selected_columns.iter() {
                                                // Check if the column name is in the format "table_name.column_name"
                                                if col_name.contains('.') {
                                                    // If it is in the format "table_name.column_name", split it
                                                    if let Some((table, column)) =
                                                        split_table_column(col_name)
                                                    {
                                                        table_name = table.to_string();
                                                        // Find the index of the column in the row
                                                        if let Some(col_index) = database.tables
                                                            [&table_name]
                                                            .columns
                                                            .iter()
                                                            .position(|col| col == column)
                                                        {
                                                            // Add the value to the selected row data
                                                            selected_row_data
                                                                .push(row[col_index].clone());
                                                        } else {
                                                            eprintln!(
                                                                "Column not found in the row"
                                                            );
                                                        }
                                                    } else {
                                                        eprintln!(
                                                            "Invalid column name format: {}",
                                                            col_name
                                                        );
                                                    }
                                                } else {
                                                    // If it is not in the format "table_name.column_name", assume it's a column name without a table prefix
                                                    if let Some(col_index) = database.tables
                                                        [&table_name]
                                                        .columns
                                                        .iter()
                                                        .position(|col| col == col_name)
                                                    {
                                                        // Add the value to the selected row data
                                                        selected_row_data
                                                            .push(row[col_index].clone());
                                                    } else {
                                                        eprintln!("Column not found in the row");
                                                    }
                                                }
                                            }
                                            selected_row_data
                                        })
                                        .collect();
                                    println!("Selected table data: {:?}", filtered_table_data);
                                } else {
                                    panic!("No table name provided");
                                }
                            } else {
                                panic!("Invalid table structure in FROM clause");
                            }
                        } else {
                            panic!("No tables provided in FROM clause");
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

            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let table_name = match table {
                    sqlparser::ast::TableWithJoins {
                        relation:
                            sqlparser::ast::TableFactor::Table {
                                name: sqlparser::ast::ObjectName(ident),
                                ..
                            },
                        joins,
                    } => ident
                        .iter()
                        .map(|ident| ident.value.to_string())
                        .collect::<String>(),
                    _ => panic!("Expected a table name"),
                };
                if let Some(table) = database.tables.get_mut(&table_name) {
                    let mut column_updates: HashMap<String, String> = HashMap::new();
                    let mut update_ids: Vec<String> = Vec::new();
                    for assignment in assignments {
                        if let sqlparser::ast::Assignment { id, value } = assignment {
                            if let [sqlparser::ast::Ident { value: column, .. }] = id.as_slice() {
                                match value {
                                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(
                                        val,
                                        _,
                                    )) => {
                                        // Record the assignment: column -> val
                                        column_updates.insert(column.clone(), val.to_string());
                                    }
                                    sqlparser::ast::Expr::Value(
                                        sqlparser::ast::Value::SingleQuotedString(val),
                                    ) => {
                                        // Record the assignment: column -> val
                                        column_updates.insert(column.clone(), val.clone());
                                    }
                                    _ => panic!("Unsupported value type for assignment"),
                                }
                            }
                        }
                    }

                    if let Some(select_condition) = selection {
                        match select_condition {
                            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                                match (left.as_ref(), op, right.as_ref()) {
                                    (
                                        sqlparser::ast::Expr::Identifier(ident),
                                        sqlparser::ast::BinaryOperator::Eq,
                                        sqlparser::ast::Expr::Value(value),
                                    ) => {
                                        match ident.value.as_str() {
                                            "id" => {
                                                if let sqlparser::ast::Value::Number(id, _) = value
                                                {
                                                    for row in &mut table.data {
                                                        if let Some(row_id) =
                                                            row.get(0).map(|id| id.clone())
                                                        {
                                                            if row_id == *id {
                                                                update_ids.push(row_id);
                                                            }
                                                        }
                                                    }
                                                    println!(
                                                        "Updated data in table: {:?}",
                                                        &table_name
                                                    );
                                                } else {
                                                    panic!("Unsupported condition value for 'id'");
                                                }
                                            }
                                            // Handle other condition columns if needed
                                            _ => {
                                                panic!("Unsupported condition column");
                                            }
                                        }
                                    }
                                    // Handle other possible condition structures
                                    _ => panic!("Unsupported condition structure"),
                                }
                            }
                            // Handle other possible expression structures
                            _ => panic!("Unsupported expression structure for selection condition"),
                        }
                    }
                    if update_ids.is_empty() {
                        update_ids = table.data.iter().map(|row| row[0].clone()).collect();
                    }
                    apply_updates(&column_updates, update_ids, table);
                } else {
                    panic!("Table not found in the database)");
                }
            }
            Statement::AlterTable {
                name,
                if_exists,
                only,
                operations,
            } => {
                let table_name = match name {
                    sqlparser::ast::ObjectName(ident) => ident
                        .iter()
                        .map(|ident| ident.value.to_string())
                        .collect::<String>(),
                    _ => panic!("Expected a table name"),
                };

                if let Some(table) = database.tables.get_mut(&table_name) {
                    for operation in operations {
                        match operation {
                            sqlparser::ast::AlterTableOperation::AddColumn {
                                column_keyword,
                                if_not_exists,
                                column_def,
                            } => {
                                if column_keyword {
                                    let column_name = &column_def.name.value;
                                    if !table.columns.contains(column_name) {
                                        let data_type = match &column_def.data_type {
                                            sqlparser::ast::DataType::Varchar(Some(length)) => {
                                                format!("VARCHAR({})", length.length)
                                            }
                                            // Handle other data types if needed
                                            _ => panic!("Unsupported data type"),
                                        };

                                        // Add the new column to the table
                                        table.columns.push(column_name.to_string());
                                        for row in &mut table.data {
                                            row.push("".to_string()); // You may initialize with a default value
                                        }

                                        // Print a message indicating the column addition
                                        println!(
                                            "Added column '{}' to table '{}'",
                                            column_name, table_name
                                        );
                                    } else if !if_not_exists {
                                        // If the column already exists and if_not_exists is not set, panic
                                        panic!(
                                            "Column '{}' already exists in table '{}'",
                                            column_name, table_name
                                        );
                                    }
                                } else {
                                    // Handle other alter table operations if needed
                                    panic!("Unsupported ALTER TABLE operation");
                                }
                            }
                            sqlparser::ast::AlterTableOperation::DropColumn {
                                column_name,
                                if_exists,
                                ..
                            } => {
                                let column_to_drop = column_name.value.clone();

                                // TODO: Implement the logic to drop the column from the table
                                if !if_exists || table.columns.contains(&column_to_drop) {
                                    table.columns.retain(|col| col != &column_to_drop);

                                    // Drop the corresponding data in each row
                                    for row in &mut table.data {
                                        let index = table
                                            .columns
                                            .iter()
                                            .position(|col| col == &column_to_drop);
                                        if let Some(index) = index {
                                            row.remove(index);
                                        }
                                    }

                                    println!(
                                        "Dropped column '{}' from table: {:?}",
                                        column_to_drop, &table_name
                                    );
                                } else {
                                    println!(
                                        "Column '{}' does not exist in table: {:?}",
                                        column_to_drop, &table_name
                                    );
                                }
                            }
                            // Handle other alter table operations if needed
                            _ => panic!("Unsupported ALTER TABLE operation"),
                        }
                    }
                } else {
                    panic!("Table not found in the database");
                }
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => {
                match object_type {
                    sqlparser::ast::ObjectType::Table => {
                        // Assuming names is a Vec of ObjectName
                        if let Some(sqlparser::ast::ObjectName(identifiers)) = names.first() {
                            let table_name = identifiers
                                .iter()
                                .map(|ident| ident.value.to_string())
                                .collect::<String>();

                            if if_exists || database.tables.contains_key(&table_name) {
                                // Drop the table if it exists or if if_exists is set
                                database.tables.remove(&table_name);
                                println!("Dropped table: {}", table_name);
                            } else {
                                println!("Table '{}' does not exist.", table_name);
                            }
                        } else {
                            println!("Invalid DROP TABLE statement: no table name provided");
                        }
                    }
                    // Handle other object types if needed
                    _ => {
                        println!("Unsupported object type in DROP statement");
                    }
                }
            }
            _ => panic!("Unsupported SQL statement"),
        }
    }
}
fn apply_updates(
    column_updates: &HashMap<String, String>,
    update_ids: Vec<String>,
    table: &mut Table,
) {
    for row in &mut table.data {
        if let Some(row_id) = row.get(0).map(|id| id.clone()) {
            if update_ids.contains(&row_id) {
                for (col, val) in row.iter_mut().enumerate() {
                    if let Some(update_val) = column_updates.get(&table.columns[col]) {
                        *val = update_val.clone();
                    }
                }
            }
        }
    }
}
fn evaluate_condition(
    condition: &sqlparser::ast::Expr,
    row: &[String],
    selected_columns: &[String],
    database: &Database,
) -> Result<bool, &'static str> {
    match condition {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            match (left.as_ref(), op, right.as_ref()) {
                (
                    sqlparser::ast::Expr::CompoundIdentifier(left_cols),
                    sqlparser::ast::BinaryOperator::Eq,
                    sqlparser::ast::Expr::CompoundIdentifier(right_cols),
                ) => {
                    // Assuming both sides are of the form "table_name.column_name"
                    let (left_table, left_col) = (
                        &left_cols[0].value.to_string(),
                        &left_cols[1].value.to_string(),
                    );
                    let (right_table, right_col) = (
                        &right_cols[0].value.to_string(),
                        &right_cols[1].value.to_string(),
                    );

                    let left_col_index = selected_columns
                        .iter()
                        .position(|col| col == &format!("{}.{}", left_table, left_col));
                    let right_col_index = selected_columns
                        .iter()
                        .position(|col| col == &format!("{}.{}", right_table, right_col));

                    if let (Some(left_index), Some(right_index)) = (left_col_index, right_col_index)
                    {
                        let left_row_val = row.get(left_index).unwrap_or(&"".to_string()).clone();
                        let right_row_val = row.get(right_index).unwrap_or(&"".to_string()).clone();

                        Ok(left_row_val == right_row_val)
                    } else {
                        Err("Column not found in the selected columns")
                    }
                }
                _ => Err("Unsupported condition structure in WHERE clause"),
            }
        }
        _ => Err("Unsupported expression structure in WHERE clause"),
    }
}
fn split_table_column(col_name: &str) -> Option<(&str, &str)> {
    let parts: Vec<&str> = col_name.split('.').collect();
    if parts.len() == 2 {
        Some((parts[0], parts[1]))
    } else {
        None
    }
}
