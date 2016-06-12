# rmerger: Redis RDB files merger

## About

rmerger is a tool to merge dump.rdb files (Redis persistent file) written in Rust.

rmerger supports only RDB v6.

## Build & Install

### Dependencies

* rustc and cargo (stable or higher)

### Environment

* Linux or Mac OS X

### Example

```
cargo build --release
cp target/release/rmerger <install_dir>
```

or

```
cargo install
```

## Usage

```
Options:
    -d, --database DATABASE
                        DB number(s) to export specially
    -o, --output DIRECTORY
                        output/working directory
    -C, --nocheck       do not check duplication of keys
    -h, --help          display this help and exit
```

```
rmerger -o ./tmp ./dump1.rdb ./dump2.rdb
```

PART_\<DBNUM\>.rdb and MERGE.rdb will be created into ./tmp directory. PART_\<DBNUM\>.rdb has no header and checksum information.
