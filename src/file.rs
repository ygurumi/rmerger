use nix::sys::stat::fstat;
use nix::sys::mman::{ mmap, munmap, PROT_READ, MAP_SHARED };
use nix::libc::size_t;

use std::fmt;
use std::sync::Mutex;
use std::os::unix::io::AsRawFd;
use std::slice::from_raw_parts_mut;
use std::ptr::null_mut;
use std::fs::File;
use std::path::{ PathBuf, Path};
use std::collections::{ HashSet, HashMap };
use std::io::{ Result, Write, Error, ErrorKind };

use super::parser::{ RDBSer, Record, EncodedString, DatabaseNumber, RDBVersion };

// erase lifetime in order to compare keys after closing rdb files
// TODO: decode EncodedString into String and check key duplication
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
enum EncodedStringVec {
    Raw(Vec<u8>),
    Int(Vec<u8>),
    Lzf(Vec<u8>),
}

impl fmt::Display for EncodedStringVec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::EncodedStringVec::*;
        match self {
            &Raw(ref v) => write!(f, "Raw(\"{}\")", String::from_utf8_lossy(&v[..])),
            &Int(ref v) => write!(f, "Int({})",     v.iter().fold(0, |a, j| a << 8 | (*j as i32))),
            &Lzf(ref v) => write!(f, "Lzf({:?})",   v),
        }
    }
}

impl<'a> From<EncodedString<'a>> for EncodedStringVec {
    fn from(s: EncodedString<'a>) -> Self {
        match s {
            EncodedString::Raw(_, v)       => EncodedStringVec::Raw(Vec::from(v)),
            EncodedString::Int(_, v)       => EncodedStringVec::Int(Vec::from(v)),
            EncodedString::Lzf(_, _, _, v) => EncodedStringVec::Lzf(Vec::from(v)),
        }
    }
}


pub fn memory_map_read<F, A>(file: &File, f: F) -> Result<A>
    where F: Fn(&mut [u8]) -> A
{
    let fd = file.as_raw_fd();
    let sz = try!(fstat(fd)).st_size as size_t;
    let mm = try!(mmap(null_mut(), sz, PROT_READ, MAP_SHARED, fd, 0));
    let s = unsafe { from_raw_parts_mut(mm as *mut u8, sz) };
    let result = f(s);
    try!(munmap(mm, sz));
    Ok(result)
}


pub struct PartRDB {
    check_duplication: bool,
    output_dir:        String,
    files:             HashMap<u32, File>,
    keys:              HashMap<u32, HashSet<EncodedStringVec>>,
}

const PART_FILE_PREFIX: &'static str = "PART_";
const PART_FILE_SUFFIX: &'static str = ".rdb";
const MERGE_FILE:        &'static str = "MERGE.rdb";
const MERGE_RDB_VERSION: &'static str = "0006";

impl PartRDB{
    pub fn new(check_duplication: bool, output_dir: String) -> Result<Self> {
        if Path::new(&output_dir).is_dir() {
            Ok(PartRDB {
                check_duplication: check_duplication,
                output_dir:        output_dir,
                files:             HashMap::new(),
                keys:              HashMap::new(),
            })
        } else {
            Err(Error::new(ErrorKind::NotFound, ""))
        }
    }

    fn part_rdb_path(&self, db_num: u32) -> PathBuf {
        let name = format!("{}{:08x}{}", PART_FILE_PREFIX, db_num, PART_FILE_SUFFIX);
        Path::new(&self.output_dir).join(&name)
    }

    fn merge_rdb_path(&self) -> PathBuf {
        Path::new(&self.output_dir).join(MERGE_FILE)
    }

    pub fn write<'a>(&mut self, db_num: DatabaseNumber<'a>, record: &Record) -> Result<()> {
        let DatabaseNumber(_, num) = db_num;

        if !self.files.contains_key(&num) {
            let path = self.part_rdb_path(num);
            info!("create temporary rdb: {:?}", path);
            let mut file = try!(File::create(path));
            try!(db_num.ser(&mut file));
            self.files.insert(num, file);
        }

        if !self.keys.contains_key(&num) {
            self.keys.insert(num, HashSet::new());
        }

        let &Record(key, _, _) = record;
        let key = EncodedStringVec::from(key);
        match (self.keys.get_mut(&num), self.files.get_mut(&num)) {
            (Some(ref mut kset), Some(ref mut file)) => {
                if !self.check_duplication || !kset.contains(&key) {
                    try!(record.ser(file));
                    kset.insert(key);
                } else {
                    warn!("duplicate key, discard: {}", key);
                }
            },
            _ => unreachable!(),
        }

        Ok(())
    }

    pub fn close_part_files(&mut self) {
        self.files = HashMap::new();
    }

    pub fn merge(&self) -> Result<usize> {
        let mfile = Mutex::new(try!(File::create(self.merge_rdb_path())));
        let version = RDBVersion(MERGE_RDB_VERSION.as_bytes());
        let mut n = 0;

        {
            let mut mfile = mfile.lock().unwrap();
            n += try!(version.ser(&mut *mfile));
        }

        for key in self.keys.keys() {
            let sfile = try!(File::open(self.part_rdb_path(*key)));
            let result = memory_map_read(&sfile, |bytes| {
                mfile.lock().unwrap().write(bytes)
            });
            n += try!(try!(result));
        }

        {
            let mut mfile = mfile.lock().unwrap();
            n += try!(mfile.write(&[0xff][..]));
            // Disable CRC64 checksum
            n += try!(mfile.write(&[0x00; 8][..]));
        }

        Ok(n)
    }
}
