use criterion::{criterion_group, criterion_main, Criterion};
use mudb::{DocType, Indexer, IndexKey, Mudb};

use cap_std::ambient_authority;
use cap_std::fs::Dir;

use serde::{Serialize, Deserialize};
use kstring::KString;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct Facets {
    a: String,
    b: i64,
    c: bool,
}

impl DocType for Facets {}

#[derive(Debug, Clone)]
struct FacetIndexer {}

impl Indexer<Facets> for FacetIndexer {
    fn index(&self, obj: &Facets) -> Vec<IndexKey> {
        vec![
            IndexKey::Str(KString::from(format!("a:{}", obj.a))),
            IndexKey::Num(obj.b),
            IndexKey::Num(if obj.c { 1 } else { 0 }),
        ]
    }
}

pub fn view_benchmark(c: &mut Criterion) {
    let data_path = ".bench";

    let data = Dir::open_ambient_dir(
        data_path,
        ambient_authority()
    ).unwrap();

    let dd_rc = Rc::new(data);

    let mut db = Mudb::<Facets>::open(
        dd_rc.clone(),
        "db_v.ndjson"
    ).unwrap();

    let _ = db.add_view(&KString::from("facets"), Box::new(FacetIndexer {})).unwrap();

    c.bench_function("insert_with_view", |b| {
        b.iter(|| {
            for i in 0..25000 {
                let obj = Facets {
                    a: format!("view+{}", i),
                    b: i,
                    c: (i % 3 == 0),
                };
                let _ = db.insert(None, obj).unwrap();
            }

            let _ = db.build_views().unwrap();
            db.compact().unwrap();
        });
    });
}

criterion_group!(
    benches,
    view_benchmark,
);

criterion_main!(benches);
