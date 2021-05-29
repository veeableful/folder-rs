use folder_rs::Index;
use serde_json::to_string;

fn main() -> Result<(), ()> {
    let mut index = Index::load("index")?;
    let mut result = index.search("lunar new year")?;
    result = index.search("lunar new year")?;
    println!("{}", to_string(&result).unwrap());
    Ok(())
}