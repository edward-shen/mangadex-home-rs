use std::fmt::Display;
use std::num::{NonZeroU16, NonZeroU64};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Wrapper type for a port number.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Port(NonZeroU16);

impl FromStr for Port {
    type Err = <NonZeroU16 as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NonZeroU16::from_str(s).map(Self)
    }
}

impl Display for Port {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Copy, Clone, Deserialize, Default, Debug, Hash, Eq, PartialEq)]
pub struct Mebibytes(usize);

impl Mebibytes {
    pub const fn as_bytes(&self) -> usize {
        self.0 << 20
    }

    pub const fn get(&self) -> usize {
        self.0
    }
}

#[derive(Copy, Clone, Deserialize, Debug, Hash, Eq, PartialEq)]
pub struct Kilobits(NonZeroU64);
