#[macro_use] extern crate nom;
#[macro_use] extern crate bitflags;
extern crate nix;

macro_rules! assert_result {
    ( $expr: expr, $err: expr ) => {
        (if ! $expr { return Err($err) })
    };
}

pub mod parser;
pub mod file;
