mod bindings;

fn main() {
    let query = "SELECT version()";
    let format = "CSV";

    match bindings::execute(query, format) {
        Some(result) => println!("{}", result),
        None => println!("Query execution failed."),
    }
}

