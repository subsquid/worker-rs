use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

#[repr(transparent)]
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct NestedSet<K1, K2>
where
    K1: Eq + Hash,
    K2: Eq + Hash,
{
    inner: HashMap<K1, HashSet<K2>>,
}

impl<K1: Eq + Hash, K2: Eq + Hash> NestedSet<K1, K2> {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn inner(&self) -> &HashMap<K1, HashSet<K2>> {
        &self.inner
    }

    pub fn into_inner(self) -> HashMap<K1, HashSet<K2>> {
        self.inner
    }

    pub fn insert(&mut self, key1: K1, key2: K2) -> bool {
        self.inner.entry(key1).or_default().insert(key2)
    }

    pub fn remove(&mut self, key1: &K1, key2: &K2) -> bool {
        let mut drained = false;
        let mut result = false;
        if let Some(nested) = self.inner.get_mut(key1) {
            result = nested.remove(key2);
            drained = nested.is_empty();
        };
        if drained {
            self.inner.remove(key1);
        }
        result
    }

    pub fn contains(&self, key1: &K1, key2: &K2) -> bool {
        match self.inner.get(key1) {
            Some(nested) => nested.contains(key2),
            None => false,
        }
    }

    pub fn difference(self, other: &Self) -> Self {
        let result = self
            .inner
            .into_iter()
            .filter_map(|(key, mut nested)| {
                if let Some(removed) = other.inner.get(&key) {
                    nested.retain(|x| !removed.contains(x));
                }
                if nested.is_empty() {
                    None
                } else {
                    Some((key, nested))
                }
            })
            .collect();
        Self { inner: result }
    }

    pub fn from_inner(inner: HashMap<K1, HashSet<K2>>) -> Self {
        Self { inner }
    }
}

impl<K1: Clone + Eq + Hash, K2: Eq + Hash> NestedSet<K1, K2> {
    pub fn into_iter(self) -> impl Iterator<Item = (K1, K2)> {
        self.inner.into_iter().flat_map(|(k1, nested)| {
            let k1 = k1;
            nested.into_iter().map(move |k2| (k1.clone(), k2))
        })
    }
}

impl<K1: Eq + Hash, K2: Eq + Hash> FromIterator<(K1, K2)> for NestedSet<K1, K2> {
    fn from_iter<I: IntoIterator<Item = (K1, K2)>>(iter: I) -> Self {
        let mut result = Self::new();
        for (key1, key2) in iter {
            result.insert(key1, key2);
        }
        result
    }
}
