use anyhow::{anyhow, Result};
use std::ffi::{OsStr, OsString};

pub fn try_into_string(s: OsString) -> Result<String> {
    s.into_string()
        .map_err(|os_str| anyhow!("Couldn't convert filename {:?} into utf-8 string", os_str))
}

pub fn try_into_str(s: &OsStr) -> Result<&str> {
    s.to_str()
        .ok_or_else(|| anyhow!("Couldn't convert filename {:?} into utf-8 string", s))
}
