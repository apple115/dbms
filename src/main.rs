mod database;
mod query_execution;
mod sql_parsing;
use database::Database;
use database::Table;
use query_execution::execute_queries;
use sql_parsing::parse_sql_queries;

fn main() {
    let sql_query = "CREATE TABLE my_table (
        id INT,
        name VARCHAR(255),
        age INT
    );
    
    INSERT INTO my_table VALUES (1, 'Alice', 30);
    INSERT INTO my_table VALUES (2, 'Bob', 20);
    INSERT INTO my_table VALUES (3, 'Charlie', 25);
    UPDATE my_table SET name ='yyc' WHERE id = 2;
    SELECT id, name FROM my_table;
    SELECT * FROM my_table;
    DELETE FROM my_table WHERE id = 1;
    DELETE FROM my_table ;
    SELECT * FROM my_table;
";
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
    let ast = parse_sql_queries(sql_query); // Parse the SQL queries
                                            // println!("{:#?}", ast); // Print the parsed queries
    execute_queries(&mut database, ast); // Execute the parsed queries on the database
    println!("{:#?}", database); // Print the database}
}
