use folder_rs::Index;

fn main() -> Result<(), ()> {
    let mut index = Index::load("index")?;
    let mut result = index.search("lunar new year")?;
    result = index.search("lunar new year")?;
    println!("{:?}", &result);
    Ok(())
}