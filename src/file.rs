use nix::sys::stat::fstat;
use nix::sys::mman::{ mmap, munmap, PROT_READ, MAP_SHARED };
use nix::libc::size_t;

use std::os::unix::io::AsRawFd;
use std::slice::from_raw_parts_mut;
use std::ptr::null_mut;
use std::fs::File;
use std::path::{ PathBuf, Path};
use std::collections::{ HashSet, HashMap };
use std::io::{ Result, Write, Error, ErrorKind };

use super::parser::{ RDBSer, RDBDec, Record, DatabaseNumber, RDBVersion };

pub fn memory_map_read<F, A>(file: &File, f: F) -> Result<A>
    where F: FnOnce(&mut [u8]) -> A
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
    keys:              HashMap<u32, HashSet<String>>,
}

const PART_FILE_PREFIX:  &'static str = "PART_";
const PART_FILE_SUFFIX:  &'static str = ".rdb";
const MERGE_FILE:        &'static str = "MERGE.rdb";
const MERGE_RDB_VERSION: &'static str = "0006";

fn part_rdb_path(output_dir: &String, db_num: u32) -> PathBuf {
    let name = format!("{}{:08x}{}", PART_FILE_PREFIX, db_num, PART_FILE_SUFFIX);
    Path::new(output_dir).join(&name)
}

fn merge_rdb_path(output_dir: &String) -> PathBuf {
    Path::new(output_dir).join(MERGE_FILE)
}

impl PartRDB{
    pub fn new(check_duplication: bool, output_dir: String) -> Result<Self> {
        assert_result!(Path::new(&output_dir).is_dir(), Error::new(ErrorKind::NotFound, "no such directory"));
        Ok(PartRDB {
            check_duplication: check_duplication,
            output_dir:        output_dir,
            files:             HashMap::new(),
            keys:              HashMap::new(),
        })
    }

    pub fn write<'a>(&mut self, db_num: DatabaseNumber<'a>, record: &Record) -> Result<()> {
        let DatabaseNumber(_, num) = db_num;

        if !self.files.contains_key(&num) {
            let path = part_rdb_path(&self.output_dir, num);
            info!("create temporary rdb: {:?}", path);
            let mut file = try!(File::create(path));
            try!(db_num.ser(&mut file));
            self.files.insert(num, file);
        }

        if !self.keys.contains_key(&num) {
            self.keys.insert(num, HashSet::new());
        }

        let &Record(key, _, _) = record;
        let key = try!(String::decode(&key));
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
        let version = RDBVersion(MERGE_RDB_VERSION.as_bytes());
        let mut mfile = try!(File::create(merge_rdb_path(&self.output_dir)));
        let mut n = try!(version.ser(&mut mfile));

        for key in self.keys.keys() {
            let sfile = try!(File::open(part_rdb_path(&self.output_dir, *key)));
            let result = memory_map_read(&sfile, |bytes| {
                mfile.write(bytes)
            });
            n += try!(try!(result));
        }

        n += try!(mfile.write(&[0xff][..]));
        // Disable CRC64 checksum
        n += try!(mfile.write(&[0x00; 8][..]));

        Ok(n)
    }
}
