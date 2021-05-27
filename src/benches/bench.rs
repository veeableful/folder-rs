use criterion::{black_box, criterion_group, criterion_main, Criterion};
use folder_rs::Index;

fn criterion_benchmark(c: &mut Criterion) {
    let mut index = Index::load("index").unwrap();
    c.bench_function("search \"lunar new year\"", |b| b.iter(|| {
        index.search(black_box("lunar new year")).unwrap();
    }));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);