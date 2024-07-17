pub mod hash;
pub mod iterator;
mod once;
#[macro_use]
pub mod run_all;
pub mod tests;

pub type UseOnce<T> = once::UseOnce<T>;
