use folder_rs::Index;

fn main() -> Result<(), ()> {
    let mut index = Index::load("index")?;
    let mut result = index.search("lunar new year")?;
    eprintln!("main: A");
    result = index.search("lunar new year")?;
    eprintln!("main: B");
    println!("{:?}", &result);
    Ok(())
}