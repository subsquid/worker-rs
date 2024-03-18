pub mod iterator;
mod once;
pub mod tests;

pub type UseOnce<T> = once::UseOnce<T>;
