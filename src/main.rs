extern crate nom;
extern crate rmerger;
extern crate getopts;

use rmerger::file::{ memory_map_read, PartRDB};
use rmerger::parser::{ rdb, RDB, RDBSer, Database, DatabaseNumber };

use std::collections::HashSet;
use nom::IResult;
use getopts::Options;

fn main() {
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
        println!("[info] target DB: ALL");
    } else {
        println!("[info] target DB: {:?}", target_db);
    }

    let check_duplication = !matches.opt_present("C");
    println!("[info] check duplication of keys: {}", check_duplication);

    let output_dir = matches.opt_str("o").unwrap_or("./".to_string());
    println!("[info] output directory: {}", output_dir);

    let mut srdb = PartRDB::new(check_duplication, output_dir).unwrap();

    for arg in matches.free {
        println!("[info] start: {}", arg);
        let file = std::fs::File::open(arg.clone()).unwrap();

        memory_map_read(&file, |s| {
            match rdb(s) {
                IResult::Done(_, RDB(ver, dbs, _)) => {
                    println!("[info] version: {}", ver.to_string().unwrap());
                    for db in dbs {
                        let Database(db_num, records) = db;
                        let DatabaseNumber(_, num) = db_num;
                        if target_db.is_empty() || target_db.contains(&num) {
                            for record in records {
                                srdb.write(db_num, &record, true).unwrap();
                            }
                        }
                    }
                },
                result => panic!("parse error: {:?}", result),
            }
        }).unwrap();

        println!("[info] finish: {}", arg);
    }

    println!("[info] start: merge");
    srdb.close_part_files();
    srdb.merge().unwrap();
    println!("[info] finish: merge");
}


fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [Options] FILE.rdb ...", program);
    print!("{}", opts.usage(&brief));
}


fn database_set(strs: Vec<String>) -> Result<HashSet<u32>, std::num::ParseIntError> {
    let mut set = HashSet::new();
    for i in strs {
        set.insert(i.parse()?);
    }
    Ok(set)
}
