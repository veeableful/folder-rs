use criterion::{black_box, criterion_group, criterion_main, Criterion};
use folder_rs::Index;

fn criterion_benchmark(c: &mut Criterion) {
    let mut index = Index::load("index").unwrap();
    c.bench_function("analyze \"lunar new year\"", |b| b.iter(|| {
        index.analyze(black_box("lunar new year"));
    }));
    c.bench_function("find_documents [\"lunar\", \"new\", \'year\"]", |b| b.iter(|| {
        index.find_documents(black_box(&vec!["lunar", "new", "year"])).unwrap();
    }));
    c.bench_function("fetch_term_stat \"lunar\"", |b| b.iter(|| {
        index.fetch_term_stat(black_box("lunar")).unwrap();
    }));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);