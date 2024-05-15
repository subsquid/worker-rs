use itertools::{EitherOrBoth, Itertools};

pub trait WithLookahead: Iterator {
    fn lookahead(self) -> impl Iterator<Item = (Self::Item, Option<Self::Item>)>
    where
        Self: Clone,
    {
        self.clone().zip_longest(self.skip(1)).map(|x| match x {
            EitherOrBoth::Both(cur, next) => (cur, Some(next)),
            EitherOrBoth::Left(cur) => (cur, None),
            EitherOrBoth::Right(_) => unreachable!(),
        })
    }
}

impl<T: ?Sized> WithLookahead for T where T: Iterator {}
