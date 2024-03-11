use std::{collections::HashMap, hash::Hash};

#[repr(transparent)]
#[derive(Default)]
pub struct NestedMap<K1, K2, V> {
    inner: HashMap<K1, HashMap<K2, V>>,
}

impl<K1: Eq + Hash, K2: Eq + Hash, V> NestedMap<K1, K2, V> {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn inner(&self) -> &HashMap<K1, HashMap<K2, V>> {
        &self.inner
    }

    pub fn insert(&mut self, key1: K1, key2: K2, value: V) -> Option<V> {
        self.inner.entry(key1).or_default().insert(key2, value)
    }

    pub fn remove(&mut self, key1: &K1, key2: &K2) -> Option<V> {
        let mut drained = false;
        let mut result = None;
        if let Some(chunks) = self.inner.get_mut(key1) {
            result = chunks.remove(key2);
            drained = chunks.is_empty();
        };
        if drained {
            self.inner.remove(key1);
        }
        result
    }

    pub fn map_values<R>(self, f: impl Fn(V) -> R) -> NestedMap<K1, K2, R> {
        let result = self
            .inner
            .into_iter()
            .map(|(k1, nested)| (k1, nested.into_iter().map(|(k2, v)| (k2, f(v))).collect()))
            .collect();
        NestedMap { inner: result }
    }
}

impl<K1: Clone, K2, V> NestedMap<K1, K2, V> {
    fn into_iter(self) -> impl Iterator<Item = (K1, K2, V)> {
        Box::new(
            self.inner.into_iter().flat_map(|(k1, nested)| {
                nested.into_iter().map(move |(k2, v)| (k1.clone(), k2, v))
            }),
        )
    }
}

impl<K1: Eq + Hash, K2: Eq + Hash, V> FromIterator<(K1, K2, V)> for NestedMap<K1, K2, V> {
    fn from_iter<I: IntoIterator<Item = (K1, K2, V)>>(iter: I) -> Self {
        let mut result = Self::new();
        for (key1, key2, value) in iter {
            result.insert(key1, key2, value);
        }
        result
    }
}
