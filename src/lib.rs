use anyhow::Result;
use cap_std::fs::{Dir, File, OpenOptions};
use cap_tempfile::TempFile;
use rusty_ulid::generate_ulid_string;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::ops::{BitAnd, BitOr, Not};
use std::rc::Rc;
use std::cell::RefCell;
use tracing::instrument;

fn default_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.create(true);
    options.append(true);
    options.read(true);

    options
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, Debug)]
pub enum Flag {
    Binary,
    Deleted,
}

#[derive(
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Debug,
    Clone,
    Ord,
    PartialOrd,
    Hash,
)]
#[serde(untagged)]
pub enum IndexKey {
    Str(String),
    Num(i64),
}

#[derive(
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Debug,
    Clone,
    Ord,
    PartialOrd,
    Hash
)]
pub struct VersionedKey {
    id: IndexKey,
    ver: u64,
}

impl VersionedKey {
    pub fn new(id: IndexKey) -> Self {
        Self {
            id,
            ver: 0,
        }
    }

    pub fn incr(&self) -> Self {
        Self {
            id: self.id.clone(),
            ver: self.ver + 1,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Doc<T: Clone + fmt::Debug> {
    key: VersionedKey,
    flags: HashSet<Flag>,
    obj: Option<T>,
}

impl<T: Serialize + DeserializeOwned + Clone + fmt::Debug> Doc<T> {
    pub fn new(key: VersionedKey, obj: Option<T>) -> Self {
        Self {
            key,
            obj,
            flags: HashSet::new(),
        }
    }

    pub fn has_flag(&self, flag: &Flag) -> bool {
        self.flags.contains(flag)
    }
}

pub trait Filter<'a, T>: fmt::Debug {
    fn matches(&self, obj: &'a T) -> bool;
}

type FilterRef<'a, T> = &'a dyn Filter<'a, T>;

#[derive(Debug, Clone)]
pub enum FilterOp<'a, T> {
    Id(FilterRef<'a, T>),
    Not(FilterRef<'a, T>),
    And(FilterRef<'a, T>, FilterRef<'a, T>),
    Or(FilterRef<'a, T>, FilterRef<'a, T>),
}

impl <'a, T: fmt::Debug> Filter<'a, T> for FilterOp<'a, T> {
    fn matches(&self, obj: &'a T) -> bool {
        match self {
            FilterOp::Id(filt) => filt.matches(obj),
            FilterOp::Not(filt) => !filt.matches(obj),
            FilterOp::And(lhs, rhs) => lhs.matches(obj) && rhs.matches(obj),
            FilterOp::Or(lhs, rhs) => lhs.matches(obj) || rhs.matches(obj),
        }
    }
}

impl <'a, T> From<FilterRef<'a, T>> for FilterOp<'a, T> {
    fn from(filt: FilterRef<'a, T>) -> FilterOp<'a, T> {
        FilterOp::Id(filt)
    }
}

impl <'a, T: 'a> BitAnd for FilterRef<'a, T> {
    type Output = FilterOp<'a, T>;

    fn bitand(self, rhs: Self) -> Self::Output {
        FilterOp::And(self, rhs)
    }
}

impl <'a, T> BitOr for FilterRef<'a, T> {
    type Output = FilterOp<'a, T>;

    fn bitor(self, rhs: Self) -> Self::Output {
        FilterOp::Or(self, rhs)
    }
}

impl <'a, T> Not for FilterRef<'a, T> {
    type Output = FilterOp<'a, T>;

    fn not(self) -> Self::Output {
        FilterOp::Not(self)
    }
}

#[derive(Debug)]
struct View<T: Clone + fmt::Debug> {
    inner: BTreeMap<IndexKey, HashSet<IndexKey>>,
    indexer: Box<dyn Indexer<T>>,
}

impl <T: Clone + fmt::Debug> View<T> {
    pub fn new(indexer: Box<dyn Indexer<T>>) -> Self {
        Self {
            inner: BTreeMap::new(),
            indexer,
        }
    }

    #[instrument]
    pub fn build(&mut self, over: &Vec<Doc<T>>) -> Result<()> {
        let mut inner = BTreeMap::new();

        for doc in over {
            if let Some(obj) = &doc.obj {
                let id = &doc.key.id;

                let keys = self.indexer.index(&obj);
                for key in keys {
                    let val_set = inner
                        .entry(key)
                        .or_insert(HashSet::new());

                    val_set.insert(id.clone());
                }
            }
        }

        self.inner = inner;
        Ok(())
    }

    #[instrument]
    pub fn query(&self, lookup_key: &IndexKey) -> Vec<IndexKey> {
        self.inner
            .get(lookup_key)
            .iter()
            .flat_map(|oids| {
                oids.iter()
                    .map(|id| id.clone())
                    .collect::<Vec<IndexKey>>()
            })
            .collect()
    }
}

pub trait Indexer<T: Clone + fmt::Debug>: fmt::Debug {
    fn index(&self, obj: &T) -> Vec<IndexKey>;
}

pub struct Mudb<T: Serialize + DeserializeOwned + Clone + fmt::Debug> {
    data_dir: Rc<Dir>,
    filename: String,
    write_fh: File,
    data: BTreeMap<VersionedKey, Doc<T>>,
    views: BTreeMap<String, RefCell<View<T>>>,
    modified: bool,
}

impl<T: Serialize + DeserializeOwned + Clone + fmt::Debug> Mudb<T> {
    #[instrument]
    pub fn open(data_dir: Rc<Dir>, filename: &str) -> Result<Self> {
        let mut file = data_dir.open_with(
            filename, &default_open_options()
        )?;

        let mut data = BTreeMap::new();

        let metadata = file.metadata()?;

        if metadata.len() > 0 {
            let _ = file.seek(SeekFrom::Start(0))?;
            let reader = BufReader::new(&file);
            let desr = serde_json::Deserializer::from_reader(reader);
            for doc in desr.into_iter() {
                let doc: Doc<T> = doc?;
                let key = doc.key.clone();
                data.insert(key, doc);
            }
        };

        Ok(Self {
            data_dir,
            filename: filename.to_string(),
            write_fh: file,
            data,
            views: BTreeMap::new(),
            modified: false,
        })
    }

    #[instrument]
    pub fn insert(&mut self, key: Option<VersionedKey>, obj: T) -> Result<VersionedKey> {
        let mut write_fh = BufWriter::new(&mut self.write_fh);
        let data = &mut self.data;

        let key = key.unwrap_or_else(|| VersionedKey {
            id: IndexKey::Str(generate_ulid_string()),
            ver: 0,
        });

        let mut doc = data
            .remove(&key)
            .map(|doc| doc.clone())
            .unwrap_or(Doc::new(key.clone(), None));

        if key.ver < doc.key.ver {
            return Err(anyhow::anyhow!("version key provided older than last stored"));
        }

        let new_key = doc.key.incr();
        doc.key = new_key.clone();
        doc.obj = Some(obj);
        data.insert(new_key.clone(), doc.clone());

        self.modified = true;

        write!(&mut write_fh, "{}\n", serde_json::to_string(&doc)?)?;

        Ok(new_key)
    }

    pub fn count(&self) -> usize {
        self.data.len()
    }

    #[instrument]
    pub fn get(&self, key: VersionedKey) -> Option<T> {
        self.data
            .get(&key)
            .iter()
            .flat_map(|doc| doc.obj.clone())
            .next()
    }

    #[instrument]
    pub fn latest(&self, id: &IndexKey) -> Option<Doc<T>> {
        self.data
            .range(VersionedKey::new(id.clone())..)
            .filter(|(k, _v)| &k.id == id)
            .next_back()
            .map(|(_k, v)| v.clone())
    }

    #[instrument(skip(op))]
    pub fn update(&mut self, key: VersionedKey, op: Box<dyn FnOnce(&T) -> T>) -> Option<Result<VersionedKey>> {
        let mut result: Option<Result<VersionedKey>> = None;
        let obj = self.get(key.clone());

        if let Some(obj) = obj {
            let output = op(&obj).clone();
            let new_key = self.insert(Some(key), output);
            result = Some(new_key);
        }

        return result;
    }

    #[instrument]
    pub fn delete(&mut self, id: VersionedKey) -> Result<Option<T>> {
        let mut write_fh = BufWriter::new(&mut self.write_fh);
        let found = self.data.remove(&id);

        if let Some(mut doc) = found {
            let obj = doc.obj;
            doc.key = doc.key.incr();
            doc.obj = None;
            doc.flags.insert(Flag::Deleted);
            write!(&mut write_fh, "{}\n", serde_json::to_string(&doc)?)?;
            self.data.insert(id.clone(), doc);
            self.modified = true;
            Ok(obj)
        } else {
            Ok(None)
        }
    }

    #[instrument]
    pub fn compact(&mut self) -> Result<()> {
        if self.modified {
            let mut tmpf = TempFile::new(&mut self.data_dir)?;
            for (_key, val) in self.data.iter() {
                write!(tmpf, "{}\n", serde_json::to_string(val)?)?;
            }

            tmpf.replace(&self.filename)?;
            let write_fh = self.data_dir.open(&self.filename)?;
            self.write_fh = write_fh;
            self.modified = false;
        }

        Ok(())
    }

    #[instrument]
    pub fn find<'a>(&'a self, filter: FilterRef<'a, T>) -> Vec<T> {
        self.data.values()
            .flat_map(|doc: &'a Doc<T>| doc.obj.as_ref())
            .filter(|obj| filter.matches(obj))
            .map(|obj| obj.clone())
            .collect()
    }

    #[instrument]
    pub fn add_view(
        &mut self,
        name: &String,
        indexer: Box<dyn Indexer<T>>
    ) -> Result<()> {
        self.views.insert(
            name.clone(),
            RefCell::new(View::new(indexer))
        );
        Ok(())
    }

    #[instrument]
    pub fn build_views(&mut self) -> Result<()> {
        let items = self.data
            .values()
            .map(|obj| obj.clone())
            .collect::<Vec<Doc<T>>>();

        for view in self.views.values() {
            let mut view_ref = view.borrow_mut();
            (*view_ref).build(&items)?;
        }

        Ok(())
    }

    #[instrument]
    pub fn find_by_view(&self, name: &str, lookup_key: IndexKey) -> Vec<T> {
        if let Some(view) = self.views.get(name) {
            let view = (*view).borrow();
            let keys = view.query(&lookup_key);

            keys.iter()
                .flat_map(|key| self.latest(key))
                .flat_map(|doc| doc.obj.clone())
                .collect()
        } else {
            vec![]
        }
    }
}

impl <T: Serialize + DeserializeOwned + Clone + fmt::Debug> fmt::Debug for Mudb<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mudb")
            .field("filename", &self.filename)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use cap_std::ambient_authority;
    use cap_std::fs::Dir;
    use cap_tempfile::TempDir;
    use serde::{Deserialize, Serialize};
    use std::rc::Rc;
    use test_log::test;

    const DATA_DIR: &str = ".data";

    fn data_dir() -> Result<(TempDir, Dir)> {
        let tmpd = TempDir::new(ambient_authority()).unwrap();
        let _ = tmpd.create_dir(DATA_DIR)?;
        let data = tmpd.open_dir(DATA_DIR)?;
        Ok((tmpd, data))
    }

    fn msg_fixture() -> Vec<TestMessage> {
        vec![
            TestMessage::Of {
                kind: 1,
                val: "hello everyone".to_string(),
            },
            TestMessage::Of {
                kind: 1,
                val: "goodbye my friends".to_string(),
            },
            TestMessage::Empty {
                kind: 0,
            }
        ]
    }

    fn init_db(
        dd_rc: Rc<Dir>,
        msgs: Option<Vec<TestMessage>>
    ) -> Result<(
        Mudb<TestMessage>,
        Vec<(VersionedKey, TestMessage)>
    )> {

        let msgs = msgs.unwrap_or_else(|| msg_fixture());

        let mut mudb = Mudb::<TestMessage>::open(
            dd_rc.clone(),
            "test.ndjson",
        )?;

        let view = View::<TestMessage>::new(
            Box::new(MsgKindIndexer{})
        );
        mudb.views.insert("kind".to_string(), RefCell::new(view));

        let results = msgs.iter().map(|msg| {
            let key = mudb.insert(None, msg.clone()).unwrap();
            (key, msg.clone())
        }).collect();

        mudb.build_views()?;

        Ok((mudb, results))
    }

    #[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug, Hash)]
    enum TestMessage {
        Empty { kind: u16, },
        Of { kind: u16, val: String },
    }

    #[derive(Debug, Clone)]
    struct MessageValFilter {
        val: String,
    }

    impl <'a> Filter<'a, TestMessage> for MessageValFilter {
        fn matches(&self, obj: &'a TestMessage) -> bool {
            match obj {
                TestMessage::Empty { kind: _ } => false,
                TestMessage::Of { kind: _, val } =>
                    (*val).contains(&self.val),
            }
        }
    }

    fn val_filter(val: &str) -> MessageValFilter {
        MessageValFilter {
            val: val.to_string(),
        }
    }

    #[derive(Debug, Clone)]
    struct MsgKindIndexer {}

    impl Indexer<TestMessage> for MsgKindIndexer {
        fn index(&self, msg: &TestMessage) -> Vec<IndexKey> {
            match msg {
                TestMessage::Of { kind, val: _ } =>
                    vec![IndexKey::Num(*kind as i64)],
                _ => vec![],
            }
        }
    }

    #[test]
    fn basic_durability() -> Result<()> {
        let (_tmp, data_dir) = data_dir()?;
        let dd_rc = Rc::new(data_dir);

        let fixture = msg_fixture();
        let key1 = {
            let (db, msgs) = init_db(
                dd_rc.clone(),
                Some(fixture.clone())
            )?;

            let (key1, msg1) = msgs.get(0).unwrap();
            let (key2, msg2) = msgs.get(1).unwrap();

            assert_eq!(db.get(key1.clone()), Some(msg1.clone()));
            assert_eq!(db.get(key2.clone()), Some(msg2.clone()));

            key1.clone()
        };

        {
            let (mut db, _msgs) = init_db(dd_rc.clone(), Some(vec![]))?;
            let msg1 = fixture.get(0).unwrap();
            let msg2 = fixture.get(1).unwrap();

            assert_eq!(db.get(key1.clone()), Some(msg1.clone()));

            let key3 = db.insert(Some(key1.clone()), msg2.clone())?;
            assert_eq!(db.get(key3.clone()).unwrap(), msg2.clone());
            assert_eq!(db.count(), fixture.len());
        }

        Ok(())
    }

    #[test]
    fn versioning() -> Result<()> {
        let (_tmp, data_dir) = data_dir()?;
        let dd_rc = Rc::new(data_dir);
        let (mut db, msgs) = init_db(dd_rc.clone(), None)?;

        let (key1, msg1) = msgs.get(0).unwrap();
        let init = db.latest(&key1.id).unwrap().obj.unwrap();
        assert_eq!(init, msg1.clone());

        let key2 = db.update(
            key1.clone(),
            Box::new(|msg: &TestMessage| msg.clone())
        ).unwrap()?;
        assert_eq!(key2.id, key1.id);
        assert!(key2.ver > key1.ver);
        assert_eq!(key1.incr(), key2);

        Ok(())
    }

    #[test]
    fn compact() -> Result<()> {
        let (_tmp, data_dir) = data_dir()?;
        let dd_rc = Rc::new(data_dir);
        let (mut db, msgs) = init_db(dd_rc.clone(), None)?;

        let _ = db.compact()?;
        let (key1, msg1) = msgs.get(0).unwrap();

        assert_eq!(db.count(), msgs.len());
        assert_eq!(db.get(key1.clone()), Some(msg1.clone()));

        Ok(())
    }

    #[test]
    fn update() -> Result<()> {
        let (_tmp, data_dir) = data_dir()?;
        let dd_rc = Rc::new(data_dir);
        let (mut db, msgs) = init_db(dd_rc.clone(), None)?;

        let (key, msg) = msgs.get(0).unwrap();

        let kind = match msg {
            TestMessage::Of { val: _, kind } => *kind,
            TestMessage::Empty { kind } => *kind,
        };
        let updated_val = match msg {
            TestMessage::Of { val, kind: _ } => format!(
                "updated {}",
                val
            ),
            _ => "".to_string(),
        };

        let op: Box<dyn FnOnce(&TestMessage) -> TestMessage> = {
            let updated_val = updated_val.clone();
            Box::new(move |_| TestMessage::Of {
                val: updated_val,
                kind: kind
            })
        };

        let idx = db.update(key.clone(), op)
            .unwrap()
            .unwrap();

        assert_eq!(idx.clone(), key.incr());

        let found = db.get(idx.clone()).unwrap();
        assert_eq!(found, TestMessage::Of {
            val: updated_val.clone(),
            kind
        });

        Ok(())
    }

    #[test]
    fn filter() -> Result<()> {
        let msgs = msg_fixture();
        let msg1 = msgs.get(0).unwrap();
        let msg2 = msgs.get(1).unwrap();

        // basic filtering
        let filt1: FilterRef<'_, TestMessage> = &val_filter("hello");
        assert_eq!(filt1.matches(&msg1), true);
        assert_eq!(filt1.matches(&msg2), false);

        let filt2: FilterRef<'_, TestMessage> = &val_filter("goodbye");
        assert_eq!(filt2.matches(&msg1), false);
        assert_eq!(filt2.matches(&msg2), true);

        // negation
        assert_eq!(!filt1.matches(&msg1), false);
        assert_eq!(!filt2.matches(&msg1), true);

        // logical 'and'
        assert_eq!((filt1 & filt2).matches(&msg1), false);

        // logical 'or'
        assert_eq!((filt1 | filt2).matches(&msg1), true);

        Ok(())
    }

    #[test]
    fn find() -> Result<()> {
        let (_tmp, data_dir) = data_dir()?;
        let dd_rc = Rc::new(data_dir);
        let (db, msgs) = init_db(dd_rc, None)?;

        let filt: FilterRef<'_, TestMessage> = &val_filter("hello");

        let (_key1, msg1) = msgs.get(0).unwrap();
        let (_key2, msg2) = msgs.get(1).unwrap();

        let found = db.find(filt);
        assert_eq!(found.len(), 1);
        assert_eq!(found.get(0).unwrap(), &msg1.clone());

        let inverse = !filt;
        let found = db.find(&inverse);
        assert_eq!(found.len(), 2);
        assert!(found.iter().find(|msg| msg.clone() == msg2).is_some());

        Ok(())
    }

    #[test]
    fn views() -> Result<()> {
        let (_tmp, data_dir) = data_dir()?;
        let dd_rc = Rc::new(data_dir);
        let (db, msgs) = init_db(dd_rc, None)?;

        let (_key1, msg1) = msgs.get(0).unwrap();
        let (_key2, msg2) = msgs.get(1).unwrap();

        let results = db.find_by_view(
            &"kind".to_string(),
            IndexKey::Num(1)
        );

        assert_eq!(results.len(), 2);

        let expected = HashSet::<TestMessage>::from(
            [msg1.clone(), msg2.clone()]
        );

        let found = HashSet::<TestMessage>::from_iter(
            results.iter().map(|msg| msg.clone())
        );

        assert_eq!(expected, found);

        let results = db.find_by_view(
            &"kind".to_string(),
            IndexKey::Num(2)
        );

        assert_eq!(results.len(), 0);

        let results = db.find_by_view(
            &"nonesuch".to_string(),
            IndexKey::Num(1)
        );

        assert_eq!(results.len(), 0);

        Ok(())
    }
}
