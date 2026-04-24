use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use nebula_core::Id;
use nebula_vector::{Hnsw, HnswConfig, Metric};

fn random_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect()
}

/// Build a 10k-point index and measure query latency. This gives a
/// realistic "is the hot path OK" number; the full recall benchmark
/// lives in the test suite.
fn bench_search(c: &mut Criterion) {
    let dim = 128;
    let n = 10_000;
    let mut rng = StdRng::seed_from_u64(7);
    let data: Vec<Vec<f32>> = (0..n).map(|_| random_vec(&mut rng, dim)).collect();

    let index = Hnsw::new(dim, Metric::L2Sq, HnswConfig::with_m(16)).unwrap();
    for (i, v) in data.iter().enumerate() {
        index.insert(Id(i as u64), v).unwrap();
    }

    let queries: Vec<Vec<f32>> = (0..100).map(|_| random_vec(&mut rng, dim)).collect();
    let mut qi = 0usize;

    c.bench_function("hnsw_search_10k_d128_k10_ef64", |b| {
        b.iter(|| {
            let q = &queries[qi % queries.len()];
            qi += 1;
            let r = index.search(black_box(q), 10, Some(64)).unwrap();
            black_box(r);
        });
    });
}

criterion_group!(benches, bench_search);
criterion_main!(benches);
