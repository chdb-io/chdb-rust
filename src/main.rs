mod bindings;

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let query = args.get(1).map_or("SELECT version()".to_string(), |arg| arg.to_string());
    let format = args.get(2).map_or("CSV".to_string(), |arg| arg.to_string());

    match bindings::execute(&query, &format) {
        Some(result) => println!("{}", result),
        None => println!("Query execution failed."),
    }
}
