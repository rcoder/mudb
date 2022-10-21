use mudb::{
    DocType,
    IndexKey,
    Mudb,
    VersionedKey
};

use criterion::{
    criterion_group,
    criterion_main,
    Criterion,
    Throughput
};

use cap_std::ambient_authority;
use cap_std::fs::Dir;

use serde::{Serialize, Deserialize};
use std::rc::Rc;
use std::ops::Range;

const ELEMENTS: Range<i64> = 0..1000;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct BenchMsg {
    msg: String,
}

impl DocType for BenchMsg {}

pub fn readwrite_benchmark(c: &mut Criterion) {
    let data_path = ".bench";
    let data = Dir::open_ambient_dir(data_path, ambient_authority()).unwrap();

    let dd_rc = Rc::new(data);

    let mut db = Mudb::<BenchMsg>::open(
        dd_rc.clone(),
        "db_rw_bench.ndjson"
    ).unwrap();

    let mut g = c.benchmark_group("rate");
    g.throughput(Throughput::Elements(ELEMENTS.end as u64));

    g.bench_function("insert", |b| {
        b.iter(|| {
            for oid in ELEMENTS {
                let id = VersionedKey::new(IndexKey::Num(oid));
                let obj = BenchMsg {
                    msg: format!("benchmark message {}", oid)
                };
                let _ = db.insert(Some(id), obj).unwrap();
            }
            db.commit().unwrap();
        });
    });

    g.bench_function("update", |b| {
        b.iter(|| {
            for oid in ELEMENTS {
                let id = VersionedKey::new(IndexKey::Num(oid));

                let update_fn: Box<dyn FnOnce(&BenchMsg) -> BenchMsg> =
                    Box::new(|obj: &BenchMsg| {
                        BenchMsg { msg: format!("updated {}", obj.msg) }
                    });

                let key = db.insert(Some(id.clone()), BenchMsg {
                    msg: "test message".to_string(),
                }).unwrap();

                let _ = db.update(&key, update_fn);
            }

            db.commit().unwrap();
        });
    });

    db = Mudb::<BenchMsg>::open(
        dd_rc.clone(),
        "db_rw_bench_c.ndjson"
    ).unwrap();

    let _ = db.compact().unwrap();

    g.bench_function("compact", |b| {
        b.iter(|| {
            for i in ELEMENTS {
                let obj = BenchMsg {
                    msg: format!("msg#{}", i),
                };
                let idx = i % 4000;
                let _ = db.insert(
                    Some(VersionedKey::new(IndexKey::Num(idx))),
                    obj
                ).unwrap();
            }

            db.compact().unwrap();
        });
    });
}

criterion_group!(
    benches,
    readwrite_benchmark,
);

criterion_main!(benches);
