// SQD worker, a core part of the SQD network.
// Copyright (C) 2024 Subsquid Labs GmbH

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

//! The worker as a library; `src/main.rs` is a thin binary over it.

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
