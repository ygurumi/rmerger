use nom::*;
use std::io::{
    Write,
    Result as IoResult,
};


bitflags! {
    flags ValueType: u8 {
        const VT_STRING            = 0x00,
        const VT_LIST              = 0x01,
        const VT_SET               = 0x02,
        const VT_SORTEDSET         = 0x03,
        const VT_HASHMAP           = 0x04,
        //const VT_ZIPMAP            = 0x09, // deprecated (>= RDB v4)
        const VT_ZIPLIST           = 0x0a,
        const VT_INTSET            = 0x0b,
        const VT_SORTEDSET_ZIPLIST = 0x0c,
        const VT_HASHMAP_ZIPLIST   = 0x0d,
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum EncodedLength<'a> {
    I(u32, &'a [u8]),
    S(u8,  &'a [u8]),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum EncodedString<'a> {
    Raw(EncodedLength<'a>, &'a [u8]),
    Int(EncodedLength<'a>, &'a [u8]),
    Lzf(EncodedLength<'a>, EncodedLength<'a>, EncodedLength<'a>, &'a [u8]),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EncodedList<'a>(EncodedLength<'a>, Vec<EncodedString<'a>>);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EncodedSet<'a>(EncodedLength<'a>, Vec<EncodedString<'a>>);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EncodedSortedset<'a>(EncodedLength<'a>, Vec<(EncodedString<'a>, u8, &'a [u8])>);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EncodedHashmap<'a>(EncodedLength<'a>, Vec<(EncodedString<'a>, EncodedString<'a>)>);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EncodedZiplist<'a>(EncodedString<'a>);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EncodedIntset<'a>(EncodedString<'a>);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EncodedSortedsetZiplist<'a>(EncodedString<'a>);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EncodedHashmapZiplist<'a>(EncodedString<'a>);

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum EncodedValue<'a> {
    V0(EncodedString<'a>),
    V1(EncodedList<'a>),
    V2(EncodedSet<'a>),
    V3(EncodedSortedset<'a>),
    V4(EncodedHashmap<'a>),
    VA(EncodedZiplist<'a>),
    VB(EncodedIntset<'a>),
    VC(EncodedSortedsetZiplist<'a>),
    VD(EncodedHashmapZiplist<'a>),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ExpiryTime<'a> {
    MS(&'a [u8]),
    S(&'a [u8]),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Record<'a>(pub EncodedString<'a>, pub EncodedValue<'a>, pub Option<ExpiryTime<'a>>);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct DatabaseNumber<'a>(pub EncodedLength<'a>, pub u32);

#[derive(Debug, PartialEq)]
pub struct Database<'a>(pub DatabaseNumber<'a>, pub Vec<Record<'a>>);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct RDBVersion<'a>(pub &'a [u8]);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Checksum<'a>(&'a [u8]);

#[derive(Debug, PartialEq)]
pub struct RDB<'a>(pub RDBVersion<'a>, pub Vec<Database<'a>>, pub Option<Checksum<'a>>);


/// convert function into unsigned int
impl<'a> From<EncodedLength<'a>> for u32 {
    fn from(l: EncodedLength<'a>) -> Self {
        match l {
            EncodedLength::I(n, _) => n,
            EncodedLength::S(_, _) => 0,
        }
    }
}

/// serialize into RDB format
pub trait RDBSer {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize>;
}

impl<'a> RDBSer for EncodedLength<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        match self {
            &EncodedLength::I(_, s) => w.write(s),
            &EncodedLength::S(_, s) => w.write(s),
        }
    }
}

impl<'a> RDBSer for EncodedString<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        match self {
            &EncodedString::Raw(s, v) => Ok(try!(s.ser(w)) + try!(w.write(v))),
            &EncodedString::Int(s, v) => Ok(try!(s.ser(w)) + try!(w.write(v))),
            &EncodedString::Lzf(s, t, u, v) => Ok(
                try!(s.ser(w)) +
                try!(t.ser(w)) +
                try!(u.ser(w)) +
                try!(w.write(v))
            ),
        }
    }
}

impl<'a> RDBSer for EncodedList<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &EncodedList(s, ref v) = self;
        let mut n = try!(s.ser(w));
        for i in v {
            n += try!(i.ser(w));
        }
        Ok(n)
    }
}

impl<'a> RDBSer for EncodedSet<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &EncodedSet(s, ref v) = self;
        let mut n = try!(s.ser(w));
        for i in v {
            n += try!(i.ser(w));
        }
        Ok(n)
    }
}

impl<'a> RDBSer for EncodedSortedset<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &EncodedSortedset(s, ref tuples) = self;
        let mut n = try!(s.ser(w));
        for i in tuples {
            let &(v, u, f) = i;
            n += try!(v.ser(w));
            n += try!(w.write(&[u][..]));
            n += try!(w.write(f));
        }
        Ok(n)
    }
}

impl<'a> RDBSer for EncodedHashmap<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &EncodedHashmap(s, ref tuples) = self;
        let mut n = try!(s.ser(w));
        for i in tuples {
            let &(k, v) = i;
            n += try!(k.ser(w));
            n += try!(v.ser(w));
        }
        Ok(n)
    }
}

impl<'a> RDBSer for EncodedZiplist<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &EncodedZiplist(s) = self;
        s.ser(w)
    }
}


impl<'a> RDBSer for EncodedIntset<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &EncodedIntset(s) = self;
        s.ser(w)
    }
}


impl<'a> RDBSer for EncodedSortedsetZiplist<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &EncodedSortedsetZiplist(s) = self;
        s.ser(w)
    }
}


impl<'a> RDBSer for EncodedHashmapZiplist<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &EncodedHashmapZiplist(s) = self;
        s.ser(w)
    }
}

impl<'a> RDBSer for ExpiryTime<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        match self {
            &ExpiryTime::MS(v) => Ok(try!(w.write(&[0xfc][..])) + try!(w.write(v))),
            &ExpiryTime::S(v)  => Ok(try!(w.write(&[0xfd][..])) + try!(w.write(v))),
        }
    }
}

impl<'a> RDBSer for Record<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &Record(key, ref val, opt) = self;
        let mut n = 0;

        for exp in opt {
            n += try!(exp.ser(w))
        }

        match val {
            &EncodedValue::V0(ref v) => {
                n += try!(w.write(&[VT_STRING.bits()][..]));
                n += try!(key.ser(w));
                n += try!(v.ser(w));
            },
            &EncodedValue::V1(ref v) => {
                n += try!(w.write(&[VT_LIST.bits()][..]));
                n += try!(key.ser(w));
                n += try!(v.ser(w));
            },
            &EncodedValue::V2(ref v) => {
                n += try!(w.write(&[VT_SET.bits()][..]));
                n += try!(key.ser(w));
                n += try!(v.ser(w));
            },
            &EncodedValue::V3(ref v) => {
                n += try!(w.write(&[VT_SORTEDSET.bits()][..]));
                n += try!(key.ser(w));
                n += try!(v.ser(w));
            },
            &EncodedValue::V4(ref v) => {
                n += try!(w.write(&[VT_HASHMAP.bits()][..]));
                n += try!(key.ser(w));
                n += try!(v.ser(w));
            },
            &EncodedValue::VA(ref v) => {
                n += try!(w.write(&[VT_ZIPLIST.bits()][..]));
                n += try!(key.ser(w));
                n += try!(v.ser(w));
            },
            &EncodedValue::VB(ref v) => {
                n += try!(w.write(&[VT_INTSET.bits()][..]));
                n += try!(key.ser(w));
                n += try!(v.ser(w));
            },
            &EncodedValue::VC(ref v) => {
                n += try!(w.write(&[VT_SORTEDSET_ZIPLIST.bits()][..]));
                n += try!(key.ser(w));
                n += try!(v.ser(w));
            },
            &EncodedValue::VD(ref v) => {
                n += try!(w.write(&[VT_HASHMAP_ZIPLIST.bits()][..]));
                n += try!(key.ser(w));
                n += try!(v.ser(w));
            },
        }
        Ok(n)
    }
}

impl<'a> RDBSer for DatabaseNumber<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &DatabaseNumber(num, _) = self;
        let mut n = try!(w.write(&[0xfe][..]));
        n += try!(num.ser(w));
        Ok(n)
    }
}

impl<'a> RDBSer for Database<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &Database(num, ref records) = self;
        let mut n = try!(num.ser(w));
        for record in records {
            n += try!(record.ser(w));
        }
        Ok(n)
    }
}

impl<'a> RDBSer for RDBVersion<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &RDBVersion(v) = self;
        let mut n = try!(w.write(b"REDIS"));
        n += try!(w.write(v));
        Ok(n)
    }
}

impl<'a> RDBSer for Checksum<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &Checksum(v) = self;
        w.write(v)
    }
}

impl<'a> RDBSer for RDB<'a> {
    fn ser<W: Write>(&self, w: &mut W) -> IoResult<usize> {
        let &RDB(v, ref dbs, opt) = self;
        let mut n = try!(v.ser(w));
        for db in dbs {
            n += try!(db.ser(w));
        }
        n += try!(w.write(&[0xff][..]));
        for cs in opt {
            n += try!(cs.ser(w));
        }
        Ok(n)
    }
}


/// parser combinator
named!(
    encoded_length(&[u8]) -> EncodedLength,
    chain!(
        l: switch!(
            peek!(bits!(pair!(take_bits!(u8, 2), take_bits!(u8, 6)))),
            (0b00, v) => map!(
                take!(1),
                |p| EncodedLength::I(v as u32, p)
            ) |
            (0b01, _) => chain!(
                p: peek!(take!(2)) ~
                v: be_u16,
                || EncodedLength::I(v as u32 & 0x3FFF, p)
            ) |
            (0b10, _) => chain!(
                p: peek!(take!(5)) ~
                take!(1) ~
                v: be_u32,
                || EncodedLength::I(v, p)
            ) |
            (0b11, v) => map!(
                take!(1),
                |p| EncodedLength::S(v, p)
            )
        ),
        || l
    )
);

named!(
    value_type(&[u8]) -> ValueType,
    map!(
        bits!(pair!(tag_bits!(u8, 1, 0b0), take_bits!(u8, 7))),
        |(_, n)| ValueType::from_bits_truncate(n)
    )
);

named!(
    encoded_string(&[u8]) -> EncodedString,
    chain!(
        s: encoded_length ~
        r: switch!(
            value!(s),
            EncodedLength::I(n,      _) => map!(take!(n), |v| EncodedString::Raw(s, v)) |
            EncodedLength::S(0b0000, _) => map!(take!(1), |v| EncodedString::Int(s, v)) |
            EncodedLength::S(0b0001, _) => map!(take!(2), |v| EncodedString::Int(s, v)) |
            EncodedLength::S(0b0010, _) => map!(take!(4), |v| EncodedString::Int(s, v)) |
            EncodedLength::S(0b0011, _) => chain!(
                t: encoded_length ~
                u: encoded_length ~
                v: take!(u32::from(t)),
                || EncodedString::Lzf(s, t, u, v)
            )
        ),
        || r
    )
);

named!(
    encoded_sequence(&[u8]) -> (EncodedLength, Vec<EncodedString>),
    chain!(
        s: encoded_length ~
        v: count!(encoded_string, u32::from(s) as usize),
        || (s, v)
    )
);

named!(
    encoded_list(&[u8]) -> EncodedList,
    map!(encoded_sequence, |(s, v)| EncodedList(s, v))
);

named!(
    encoded_set(&[u8]) -> EncodedSet,
    map!(encoded_sequence, |(s, v)| EncodedSet(s, v))
);

named!(
    encoded_sortedset(&[u8]) -> EncodedSortedset,
    chain!(
        s: encoded_length ~
        v: count!(
            chain!(
                w: encoded_string ~
                u: be_u8 ~
                f: take!(u),
                || (w, u, f)
            ),
            u32::from(s.clone()) as usize),
        || EncodedSortedset(s, v)
    )
);

named!(
    encoded_hash(&[u8]) -> EncodedHashmap,
    chain!(
        s: encoded_length ~
        t: count!(pair!(encoded_string, encoded_string), u32::from(s.clone()) as usize),
        || EncodedHashmap(s, t)
    )
);

named!(
    encoded_ziplist(&[u8]) -> EncodedZiplist,
    map!(encoded_string, |s| EncodedZiplist(s))
);

named!(
    encoded_intset(&[u8]) -> EncodedIntset,
    map!(encoded_string, |s| EncodedIntset(s))
);

named!(
    encoded_sortedset_ziplist(&[u8]) -> EncodedSortedsetZiplist,
    map!(encoded_string, |s| EncodedSortedsetZiplist(s))
);

named!(
    encoded_hashmap_ziplist(&[u8]) -> EncodedHashmapZiplist,
    map!(encoded_string, |s| EncodedHashmapZiplist(s))
);

// FC {8 bytes unsigned long}
named!(
    expiry_time_msec(&[u8]) -> ExpiryTime,
    chain!(
        tag!([0xfc]) ~
        e: take!(8),
        || ExpiryTime::MS(e)
    )
);

// FD {4 bytes unsigned int}
named!(
    expiry_time_sec(&[u8]) -> ExpiryTime,
    chain!(
        tag!([0xfc]) ~
        e: take!(4),
        || ExpiryTime::S(e)
    )
);

named!(
    pub record(&[u8]) -> Record,
    chain!(
        o: alt!(expiry_time_msec | expiry_time_sec)? ~
        t: value_type ~
        k: encoded_string ~
        v: switch!(
            value!(t),
            VT_STRING            => map!(encoded_string,            |v| EncodedValue::V0(v)) |
            VT_LIST              => map!(encoded_list,              |v| EncodedValue::V1(v)) |
            VT_SET               => map!(encoded_set,               |v| EncodedValue::V2(v)) |
            VT_SORTEDSET         => map!(encoded_sortedset,         |v| EncodedValue::V3(v)) |
            VT_HASHMAP           => map!(encoded_hash,              |v| EncodedValue::V4(v)) |
            VT_ZIPLIST           => map!(encoded_ziplist,           |v| EncodedValue::VA(v)) |
            VT_INTSET            => map!(encoded_intset,            |v| EncodedValue::VB(v)) |
            VT_SORTEDSET_ZIPLIST => map!(encoded_sortedset_ziplist, |v| EncodedValue::VC(v)) |
            VT_HASHMAP_ZIPLIST   => map!(encoded_hashmap_ziplist,   |v| EncodedValue::VD(v))
        ),
        || Record(k, v, o)
    )
);

// FE {length encoding}
named!(
    pub database_number(&[u8]) -> DatabaseNumber,
    chain!(
        tag!([0xfe]) ~
        n: encoded_length,
        || DatabaseNumber(n, u32::from(n))
    )
);

// FF
named!(
    end_of_rdb(&[u8]) -> &[u8],
    tag!([0xff])
);

named!(
    checksum(&[u8]) -> Checksum,
    map!(take!(8), |v| Checksum(v))
);

// "REDIS0006"
named!(
    rdb_version(&[u8]) -> RDBVersion,
    chain!(
        tag!("REDIS") ~
        v: take!(4),
        || RDBVersion(v)
    )
);

named!(
    pub database(&[u8]) -> Database,
    chain!(
        n: database_number ~
        r: many0!(record),
        || Database(n, r)
    )
);

named!(
    pub rdb(&[u8]) -> RDB,
    chain!(
        v: rdb_version ~
        d: many0!(database) ~
        end_of_rdb ~
        c: checksum? ~
        eof,
        || RDB(v, d, c)
    )
);
