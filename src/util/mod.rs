pub mod iterator;
mod once;
#[cfg(test)]
pub mod tests;

pub type UseOnce<T> = once::UseOnce<T>;
