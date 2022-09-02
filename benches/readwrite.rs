use criterion::{criterion_group, criterion_main, Criterion};
use mudb::{IndexKey, Mudb, VersionedKey};

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use cap_tempfile::TempDir;

use tracing::error;

use serde::{Serialize, Deserialize};
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BenchMsg {
    msg: String,
}

pub fn readwrite_benchmark(c: &mut Criterion) {
    let data_path = ".bench";
    let data = Dir::open_ambient_dir(data_path, ambient_authority()).unwrap();

    let dd_rc = Arc::new(Mutex::new(data));

    let mut db = Mudb::<BenchMsg>::open(
        dd_rc.clone(),
        "db_rw_bench.ndjson"
    ).unwrap();

    let mut oid = 0;

    c.bench_function("insert", |b| {
        b.iter(|| {
            let id = VersionedKey::new(IndexKey::Num(oid));
            let obj = BenchMsg {
                msg: format!("benchmark message {}", oid)
            };
            let _ = db.insert(Some(id), obj).unwrap();
            oid += 1;
        });
    });

    c.bench_function("update", |b| {
        b.iter(|| {
            let id = VersionedKey::new(IndexKey::Num(oid));
            let update_fn: Box<dyn FnOnce(&BenchMsg) -> BenchMsg> =
                Box::new(|obj: &BenchMsg| {
                    BenchMsg { msg: format!("updated {}", obj.msg) }
                });
            let id = db.insert(Some(id.clone()), BenchMsg {
                msg: "test message".to_string(),
            }).unwrap();
            let _ = db.update(id.clone(), update_fn).unwrap().unwrap();
            oid -= 1;
        });
    });

    db = Mudb::<BenchMsg>::open(
        dd_rc.clone(),
        "db_rw_bench_c.ndjson"
    ).unwrap();

    let _ = db.compact().unwrap();

    c.bench_function("compact", |b| {
        b.iter(|| {
            for i in 0..16000 {
                let obj = BenchMsg {
                    msg: format!("msg#{}", i),
                };
                let idx = i % 4000;
                let _ = db.insert(
                    Some(VersionedKey::new(IndexKey::Num(idx))),
                    obj
                ).unwrap();
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
