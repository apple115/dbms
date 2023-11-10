mod database;
mod query_execution;
mod sql_parsing;
use database::Database;
use database::Table;
use query_execution::execute_queries;
use sql_parsing::parse_sql_queries;
use std::io;
use std::io::Write;

fn main() {
    let sql_query = "CREATE TABLE my_table (
        id INT,
        name VARCHAR(255),
        age INT
    );


    INSERT INTO my_table VALUES (1, 'Alice', 30);
    INSERT INTO my_table VALUES (3, 'Charlie', 25);
    
";

    // INSERT INTO my_table VALUES (2, 'Bob', 20);
    // INSERT INTO my_table VALUES (3, 'Charlie', 25);
    //
    // SELECT id, name FROM my_table;
    // SELECT * FROM my_table;
    // SELECT * FROM my_table;
    //ALTER TABLE my_table DROP COLUMN email;
    //
    //   CREATE TABLE my_other_table (
    //     id INT,
    //     name VARCHAR(255),
    //     age INT
    // );
    //
    //INSERT INTO my_other_table VALUES (1, 'Alice', 30);
    //
    //SELECT * FROM my_other_table;
    let mut database = Database::new(); // Create a new database
                                        // let ast = parse_sql_queries(sql_query); // Parse the SQL queries
                                        //                                         // println!("{:#?}", ast); // Print the parsed queries
                                        // execute_queries(&mut database, ast); // Execute the parsed queries on the database
    database.load();
    println!("{:#?}", database); // Print the database}
                                 //let _ = database.save();
                                 // loop {
                                 //     print!("dbms> ");
                                 //     io::stdout().flush().unwrap(); // Flush the output
                                 //
                                 //     let mut query = String::new();
                                 //     io::stdin().read_line(&mut query).unwrap();
                                 //
                                 // }
}
