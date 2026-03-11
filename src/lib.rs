#![warn(clippy::correctness)]
#![warn(clippy::suspicious)]
#![warn(clippy::perf)]
#![warn(clippy::complexity)]
#![allow(clippy::style)]
#![allow(clippy::pedantic)]
#![allow(clippy::nursery)]
#![cfg_attr(test, allow(clippy::all))]

pub mod cli;
pub mod compute_units;
pub mod controller;
pub mod http_server;
pub mod logs_storage;
pub mod metrics;
pub mod query;
pub mod storage;
pub mod types;
pub mod util;
