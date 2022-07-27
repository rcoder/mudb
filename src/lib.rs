use anyhow::Result;
use cap_std::fs::{Dir, File, OpenOptions};
use rusty_ulid::generate_ulid_string;
use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use serde_json::{json,Value};
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::rc::Rc;

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

#[derive(Serialize, Deserialize, Clone)]
pub struct Doc<T: Clone> {
    id: String,
    obj: Option<T>,
    ver: u64,
    flags: HashSet<Flag>,
}

impl <T: Serialize + DeserializeOwned + Clone> Doc<T> {
    pub fn new(id: String, obj: Option<T>) -> Self {
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

pub struct Mudb<T: Serialize + DeserializeOwned + Clone> {
    data_dir: Rc<Dir>,
    filename: String,
    write_fh: BufWriter<File>,
    data: BTreeMap<String, Doc<T>>,
}

impl <T: Serialize + DeserializeOwned + Clone> Mudb<T> {
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
                let id = doc.id.to_string();
                data.insert(id, doc);
            }
        };

        Ok(Self {
            data_dir,
            filename: filename.to_string(),
            write_fh: BufWriter::new(file),
            data,
        })
    }

    pub fn insert(&mut self, id: Option<String>, obj: T) -> Result<String> {
        let id = id.unwrap_or_else(|| generate_ulid_string());

        let mut doc = self.data.entry(id.clone()).or_insert(Doc::new(id.clone(), None));
        doc.obj = Some(obj);

        write!(self.write_fh, "{}\n", serde_json::to_string(&doc)?)?;
        self.write_fh.flush()?;

        Ok(id)
    }

    pub fn get(&self, id: String) -> Option<T> {
        self.data.get(&id).iter().flat_map(|doc| doc.obj.clone()).next()
    }
}

#[cfg(test)]
mod test {
    use super::Mudb;
    use cap_std::ambient_authority;
    use cap_std::fs::Dir;
    use cap_tempfile::TempDir;
    use anyhow::Result;
    use serde::{Serialize, Deserialize};
    use std::ops::Deref;
    use std::rc::Rc;

    const DATA_DIR: &str = ".data";

    fn data_dir() -> Result<(TempDir, Dir)> {
        let tmpd = TempDir::new(ambient_authority()).unwrap();
        let _ = tmpd.create_dir(DATA_DIR)?;
        let data = tmpd.open_dir(DATA_DIR)?;
        Ok((tmpd, data))
    }

    #[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
    enum TestMessage {
        Empty,
        Of {
            kind: u16,
            val: String,
        },
    }

    #[test]
    fn basic_durability() -> Result<()> {
        let (_tmp, data_dir) = data_dir()?;
        let dd_rc = Rc::new(data_dir);

        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        let msg1 = TestMessage::Empty;
        let msg2 = TestMessage::Of { kind: 1, val: "just passing through".to_string() };

        {
            let mut db: Mudb<TestMessage> = Mudb::<TestMessage>::open(dd_rc.clone(), "_test")?;

            assert_eq!(key1.clone(), db.insert(Some(key1.clone()), msg1.clone())?);
            assert_eq!(key2.clone(), db.insert(Some(key2.clone()), msg2.clone())?);

            assert_eq!(db.get(key1.clone()), Some(msg1.clone()));
            assert_eq!(db.get(key2.clone()), Some(msg2.clone()));
        }

        {
            let db: Mudb<TestMessage> = Mudb::<TestMessage>::open(dd_rc.clone(), "_test")?;
            assert_eq!(db.get(key1.clone()), Some(msg1));
            assert_eq!(db.get(key2.clone()), Some(msg2));
        }

        Ok(())
    }
}
