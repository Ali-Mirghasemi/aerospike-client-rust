use std::fmt::{self, Display};
use syn::{Ident, Path};

#[derive(Copy, Clone)]
pub struct Symbol(&'static str);

pub const ENTITY: Symbol = Symbol("entity");
pub const NAMESPACE: Symbol = Symbol("namespace");
pub const SET_NAME: Symbol = Symbol("set_name");
pub const NAMESPACE_FN: Symbol = Symbol("namespace_fn");
pub const SET_NAME_FN: Symbol = Symbol("set_name_fn");
pub const RENAME: Symbol = Symbol("rename");
pub const IGNORE: Symbol = Symbol("ignore");
pub const KEY: Symbol = Symbol("key");

impl PartialEq<Symbol> for Ident {
    fn eq(&self, word: &Symbol) -> bool {
        self == word.0
    }
}

impl<'a> PartialEq<Symbol> for &'a Ident {
    fn eq(&self, word: &Symbol) -> bool {
        *self == word.0
    }
}

impl PartialEq<Symbol> for Path {
    fn eq(&self, word: &Symbol) -> bool {
        self.is_ident(word.0)
    }
}

impl<'a> PartialEq<Symbol> for &'a Path {
    fn eq(&self, word: &Symbol) -> bool {
        self.is_ident(word.0)
    }
}

impl Display for Symbol {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(self.0)
    }
}
