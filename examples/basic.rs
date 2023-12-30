use chdb_rust::query;

fn main() {
    let v = query("SELECT 'Hello libchdb.so from chdbSimple'", "CSV").unwrap();
    match String::from_utf8(v.buf().to_vec()) {
        Ok(s) => println!("{}", s),
        Err(e) => println!("Invalid UTF-8 sequence: {}", e),
    }
}
