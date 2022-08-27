use criterion::{criterion_group, criterion_main, Criterion};
use mudb::{IndexKey, Mudb};

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use cap_tempfile::TempDir;

use tracing::error;

use serde::{Serialize, Deserialize};
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BenchMsg {
    msg: String,
}

const DATA_DIR: &str = ".bench";

pub fn readwrite_benchmark(c: &mut Criterion) {
    let db_name = "benchmark_db";

    let data_path = std::env::var("MUDB_DATA_DIR").unwrap_or(DATA_DIR.to_string());
    let data = Dir::open_ambient_dir(data_path, ambient_authority()).unwrap();

    let dd_rc = Rc::new(data);

    let mut db = Mudb::<BenchMsg>::open(
        dd_rc.clone(),
        "db.ndjson"
    ).unwrap();

    let mut oid = 0;

    c.bench_function("insert", |b| {
        b.iter(|| {
            let id = IndexKey::Num(oid);
            let obj = BenchMsg {
                msg: format!("benchmark message {}", oid)
            };
            let _ = db.insert(Some(id), obj).unwrap();
            oid += 1;
        });
    });

    c.bench_function("update", |b| {
        b.iter(|| {
            let id = IndexKey::Num(oid);
            let update_fn: Box<dyn FnOnce(&BenchMsg) -> BenchMsg> =
                Box::new(|obj: &BenchMsg| {
                    BenchMsg { msg: format!("updated {}", obj.msg) }
                });
            let _ = db.insert(Some(id.clone()), BenchMsg {
                msg: "test message".to_string(),
            });
            let _ = db.update(id.clone(), update_fn).unwrap().unwrap();
            oid -= 1;
        });
    });

    db = Mudb::<BenchMsg>::open(
        dd_rc.clone(),
        "db_c.ndjson"
    ).unwrap();

    let _ = db.compact().unwrap();

    c.bench_function("compact", |b| {
        b.iter(|| {
            for i in 0..16000 {
                let obj = BenchMsg {
                    msg: format!("msg#{}", i),
                };
                let idx = i % 4000;
                let _ = db.insert(Some(IndexKey::Num(idx)), obj).unwrap();
            }
            let _ = db.compact();
        });
    });
}

criterion_group!(
    benches,
    readwrite_benchmark,
);
criterion_main!(benches);
