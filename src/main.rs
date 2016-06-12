#[macro_use] extern crate log;
extern crate nom;
extern crate rmerger;
extern crate getopts;

use rmerger::logger::StdLogger;
use rmerger::file::{ memory_map_read, PartRDB};
use rmerger::parser::{ rdb, RDB, Database, DatabaseNumber };

use std::collections::HashSet;
use nom::IResult;
use getopts::Options;

fn main() {
    StdLogger::init(None).unwrap();

    let args: Vec<String> = std::env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();

    opts.optmulti("d", "database", "DB number(s) to export specially", "DATABASE" );
    opts.optopt  ("o", "output",   "output/working directory",         "DIRECTORY");
    opts.optflag ("C", "nocheck",  "do not check duplication of keys");
    opts.optflag ("h", "help",     "display this help and exit");

    let matches = opts.parse(&args[1..]).unwrap();
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let target_db = database_set(matches.opt_strs("d")).unwrap();
    if target_db.is_empty() {
        info!("export targets: DB ALL");
    } else {
        info!("export targets: DB {:?}", target_db);
    }

    let check_duplication = !matches.opt_present("C");
    info!("check duplication of keys: {}", check_duplication);

    let output_dir = matches.opt_str("o").unwrap_or("./".to_string());
    info!("output directory: {}", output_dir);

    let srdb = std::sync::Mutex::new(
        PartRDB::new(check_duplication, output_dir).unwrap()
    );

    for arg in matches.free {
        info!("start: {}", arg);
        let file = std::fs::File::open(arg.clone()).unwrap();

        memory_map_read(&file, |s| {
            let mut srdb = srdb.lock().unwrap();
            match rdb(s) {
                IResult::Done(_, RDB(_, dbs, _)) => {
                    for db in dbs {
                        let Database(db_num, records) = db;
                        let DatabaseNumber(_, num) = db_num;
                        if target_db.is_empty() || target_db.contains(&num) {
                            for record in records {
                                srdb.write(db_num, &record).unwrap();
                            }
                        }
                    }
                },
                result => panic!("parse error: {:?}", result),
            }
        }).unwrap();

        info!("finish: {}", arg);
    }

    info!("start: merge");
    {
        let mut srdb = srdb.lock().unwrap();
        srdb.close_part_files();
        srdb.merge().unwrap();
    }
    info!("finish: merge");
}


fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options] FILE.rdb ...", program);
    print!("{}", opts.usage(&brief));
}


fn database_set(strs: Vec<String>) -> Result<HashSet<u32>, std::num::ParseIntError> {
    let mut set = HashSet::new();
    for i in strs {
        set.insert(try!(i.parse()));
    }
    Ok(set)
}
