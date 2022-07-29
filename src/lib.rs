use anyhow::Result;
use cap_std::fs::{Dir, File, OpenOptions};
use cap_tempfile::TempFile;
use rusty_ulid::generate_ulid_string;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::ops::{BitAnd, BitOr, Not};
use std::rc::Rc;
use std::cell::RefCell;

fn default_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.create(true);
    options.append(true);
    options.read(true);

    options
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone)]
pub enum Flag {
    Binary,
    Deleted,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Ord, PartialOrd, Hash)]
pub enum IndexKey {
    Str(String),
    Num(i64),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Doc<T: Clone> {
    id: IndexKey,
    obj: Option<T>,
    ver: u64,
    flags: HashSet<Flag>,
}

impl<T: Serialize + DeserializeOwned + Clone> Doc<T> {
    pub fn new(id: IndexKey, obj: Option<T>) -> Self {
        Self {
            id,
            obj,
            ver: 0,
            flags: HashSet::new(),
        }
    }

    pub fn has_flag(&self, flag: &Flag) -> bool {
        self.flags.contains(flag)
    }
}

pub trait Filter<'a, T> {
    fn matches(&self, obj: &'a T) -> bool;
}

type FilterRef<'a, T> = &'a dyn Filter<'a, T>;

pub enum FilterOp<'a, T> {
    Id(FilterRef<'a, T>),
    Not(FilterRef<'a, T>),
    And(FilterRef<'a, T>, FilterRef<'a, T>),
    Or(FilterRef<'a, T>, FilterRef<'a, T>),
}

impl <'a, T> Filter<'a, T> for FilterOp<'a, T> {
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

struct View<T> {
    inner: BTreeMap<IndexKey, HashSet<IndexKey>>,
    indexer: Box<dyn Indexer<T>>,
}

impl <T: Clone> View<T> {
    pub fn new(indexer: Box<dyn Indexer<T>>) -> Self {
        Self {
            inner: BTreeMap::new(),
            indexer,
        }
    }

    pub fn build(&mut self, over: &Vec<Doc<T>>) -> Result<()> {
        let mut inner = BTreeMap::new();

        for doc in over {
            if let Some(obj) = &doc.obj {
                let id = &doc.id;

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

pub trait Indexer<T> {
    fn index(&self, obj: &T) -> Vec<IndexKey>;
}

pub struct Mudb<T: Serialize + DeserializeOwned + Clone> {
    data_dir: Rc<Dir>,
    filename: String,
    write_fh: BufWriter<File>,
    data: BTreeMap<IndexKey, Doc<T>>,
    views: BTreeMap<String, RefCell<View<T>>>,
    modified: bool,
}

impl<T: Serialize + DeserializeOwned + Clone> Mudb<T> {
    pub fn open(data_dir: Rc<Dir>, filename: &str) -> Result<Self> {
        let mut file = data_dir.open_with(filename, &default_open_options())?;
        let mut data = BTreeMap::new();

        let metadata = file.metadata()?;

        if metadata.len() > 0 {
            let _ = file.seek(SeekFrom::Start(0))?;
            let reader = BufReader::new(&file);
            let desr = serde_json::Deserializer::from_reader(reader);
            for doc in desr.into_iter() {
                let doc: Doc<T> = doc?;
                let id = doc.id.clone();
                data.insert(id, doc);
            }
        };

        Ok(Self {
            data_dir,
            filename: filename.to_string(),
            write_fh: BufWriter::new(file),
            data,
            views: BTreeMap::new(),
            modified: false,
        })
    }

    pub fn insert(&mut self, id: Option<IndexKey>, obj: T) -> Result<IndexKey> {
        let id = id.unwrap_or_else(|| IndexKey::Str(generate_ulid_string()));

        let mut doc = self
            .data
            .entry(id.clone())
            .or_insert(Doc::new(id.clone(), None));
        doc.obj = Some(obj);
        self.modified = true;

        write!(&mut self.write_fh, "{}\n", serde_json::to_string(doc)?)?;
        self.write_fh.flush()?;

        Ok(id)
    }

    pub fn get(&self, id: IndexKey) -> Option<T> {
        self.data
            .get(&id)
            .iter()
            .flat_map(|doc| doc.obj.clone())
            .next()
    }

    pub fn delete(&mut self, id: IndexKey) -> Result<Option<T>> {
        let found = self.data.remove(&id);

        if let Some(mut doc) = found {
            let obj = doc.obj;
            doc.obj = None;
            doc.flags.insert(Flag::Deleted);
            write!(&mut self.write_fh, "{}\n", serde_json::to_string(&doc)?)?;
            self.write_fh.flush()?;
            self.data.insert(id.clone(), doc);
            self.modified = true;
            Ok(obj)
        } else {
            Ok(None)
        }
    }

    pub fn compact(&mut self) -> Result<()> {
        if self.modified {
            let mut tmpf = TempFile::new(&mut self.data_dir)?;
            for (_key, val) in self.data.iter() {
                write!(tmpf, "{}\n", serde_json::to_string(val)?)?;
            }

            tmpf.replace(&self.filename)?;
            let write_fh = self.data_dir.open(&self.filename)?;

            self.write_fh = BufWriter::new(write_fh);
            self.modified = false;
        }

        Ok(())
    }

    pub fn find<'a>(&'a self, filter: FilterRef<'a, T>) -> Vec<T> {
        // TODO: indices!
        self.data.values()
            .flat_map(|doc: &'a Doc<T>| doc.obj.as_ref())
            .filter(|obj| filter.matches(obj))
            .map(|obj| obj.clone())
            .collect()
    }

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

    pub fn find_by_view(&self, name: &String, lookup_key: IndexKey) -> Vec<T> {
        if let Some(view) = self.views.get(name) {
            let view = (*view).borrow();
            let keys = view.query(&lookup_key);

            keys.iter()
                .flat_map(|key| self.data.get(key))
                .flat_map(|doc| doc.obj.clone())
                .collect()
        } else {
            vec![]
        }
    }

    // TODO: let filters run over index values as well
    // as objects...just run indexer and pass to filter?
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

    const DATA_DIR: &str = ".data";

    fn data_dir() -> Result<(TempDir, Dir)> {
        let tmpd = TempDir::new(ambient_authority()).unwrap();
        let _ = tmpd.create_dir(DATA_DIR)?;
        let data = tmpd.open_dir(DATA_DIR)?;
        Ok((tmpd, data))
    }

    fn init_db(dd_rc: Rc<Dir>) -> Result<(
        Mudb<TestMessage>,
        Vec<(IndexKey, TestMessage)>
    )> {
        let mut mudb = Mudb::<TestMessage>::open(
            dd_rc.clone(),
            "test.ndjson",
        )?;

        let msg1 = TestMessage::Of {
            kind: 1,
            val: "hello everyone".to_string(),
        };

        let msg2 = TestMessage::Of {
            kind: 1,
            val: "goodbye my friends".to_string(),
        };

        let key1 = mudb.insert(None, msg1.clone())?;
        let key2 = mudb.insert(None, msg2.clone())?;

        Ok((mudb, vec![(key1, msg1), (key2, msg2)]))
    }

    #[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
    enum TestMessage {
        Empty,
        Of { kind: u16, val: String },
    }

    #[test]
    fn basic_durability() -> Result<()> {
        let (_tmp, data_dir) = data_dir()?;
        let dd_rc = Rc::new(data_dir);

        {
            let (mut db, msgs) = init_db(dd_rc.clone())?;

            let (key1, msg1) = msgs.get(0).unwrap();
            let (key2, msg2) = msgs.get(1).unwrap();

            assert_eq!(db.get(key1.clone()), Some(msg1.clone()));
            assert_eq!(db.get(key2.clone()), Some(msg2.clone()));
        }

        {
            let (db, msgs) = init_db(dd_rc.clone())?;

            let (key1, msg1) = msgs.get(0).unwrap();

            assert_eq!(db.get(key1.clone()), Some(msg1.clone()));
        }

        Ok(())
    }

    #[test]
    fn compact() -> Result<()> {
        let (_tmp, data_dir) = data_dir()?;
        let dd_rc = Rc::new(data_dir);
        let (mut db, msgs) = init_db(dd_rc.clone())?;

        let _ = db.compact()?;
        let (key1, msg1) = msgs.get(0).unwrap();

        assert_eq!(db.get(key1.clone()), Some(msg1.clone()));

        Ok(())
    }

    #[derive(Clone)]
    struct MessageValFilter {
        val: String,
    }

    impl <'a> Filter<'a, TestMessage> for MessageValFilter {
        fn matches(&self, obj: &'a TestMessage) -> bool {
            match obj {
                TestMessage::Empty => false,
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

    #[test]
    fn filter() -> Result<()> {
        let msg = TestMessage::Of {
            kind: 1,
            val: "hello hello nice to meet you".to_string(),
        };

        // basic filtering
        let filt1: FilterRef<'_, TestMessage> = &val_filter("hello");
        assert_eq!(filt1.matches(&msg), true);

        let filt2: FilterRef<'_, TestMessage> = &val_filter("goodbye");
        assert_eq!(filt2.matches(&msg), false);

        // negation
        assert_eq!(!filt1.matches(&msg), false);
        assert_eq!(!filt2.matches(&msg), true);

        // logical 'and'
        assert_eq!((filt1 & filt2).matches(&msg), false);

        // logical 'or'
        assert_eq!((filt1 | filt2).matches(&msg), true);

        Ok(())
    }

    #[test]
    fn find() -> Result<()> {
        let (_tmp, data_dir) = data_dir()?;
        let dd_rc = Rc::new(data_dir);
        let (db, msgs) = init_db(dd_rc)?;

        let filt: FilterRef<'_, TestMessage> = &val_filter("hello");

        let (key1, msg1) = msgs.get(0).unwrap();
        let (key2, msg2) = msgs.get(1).unwrap();

        let found = db.find(filt);
        assert_eq!(found.len(), 1);
        assert_eq!(found.get(0).unwrap(), &msg1.clone());

        let inverse = !filt;
        let found = db.find(&inverse);
        assert_eq!(found.len(), 1);
        assert_eq!(found.get(0).unwrap(), &msg2.clone());

        Ok(())
    }

    struct MsgKindIndexer {
        kind: u16,
    }

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
    fn views() -> Result<()> {

        Ok(())
    }
}
