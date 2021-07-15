use std::fmt::Display;
use std::num::{NonZeroU16, NonZeroU64, ParseIntError};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Wrapper type for a port number.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Port(NonZeroU16);

impl Port {
    pub const fn get(self) -> u16 {
        self.0.get()
    }

    #[cfg(test)]
    pub fn new(amt: u16) -> Option<Self> {
        NonZeroU16::new(amt).map(Self)
    }
}

impl Default for Port {
    fn default() -> Self {
        Self(unsafe { NonZeroU16::new_unchecked(443) })
    }
}

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

#[derive(Copy, Clone, Serialize, Deserialize, Default, Debug, Hash, Eq, PartialEq)]
pub struct Mebibytes(usize);

impl Mebibytes {
    #[cfg(test)]
    pub fn new(size: usize) -> Self {
        Mebibytes(size)
    }
}

impl FromStr for Mebibytes {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<usize>().map(Self)
    }
}

pub struct Bytes(pub usize);

impl Bytes {
    pub const fn get(&self) -> usize {
        self.0
    }
}

impl From<Mebibytes> for Bytes {
    fn from(mib: Mebibytes) -> Self {
        Self(mib.0 << 20)
    }
}

#[derive(Copy, Clone, Deserialize, Debug, Hash, Eq, PartialEq)]
pub struct KilobitsPerSecond(NonZeroU64);

impl KilobitsPerSecond {
    #[cfg(test)]
    pub fn new(size: u64) -> Option<Self> {
        NonZeroU64::new(size).map(Self)
    }
}

impl FromStr for KilobitsPerSecond {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<NonZeroU64>().map(Self)
    }
}

#[derive(Copy, Clone, Serialize, Debug, Hash, Eq, PartialEq)]
pub struct BytesPerSecond(NonZeroU64);

impl From<KilobitsPerSecond> for BytesPerSecond {
    fn from(kbps: KilobitsPerSecond) -> Self {
        Self(unsafe { NonZeroU64::new_unchecked(kbps.0.get() * 125) })
    }
}
