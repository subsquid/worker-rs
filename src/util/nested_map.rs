use std::collections::BTreeMap;

#[repr(transparent)]
#[derive(Default, Debug, Clone)]
pub struct NestedMap<K1, K2, V> {
    inner: BTreeMap<K1, BTreeMap<K2, V>>,
}

impl<K1: Eq + Ord, K2: Eq + Ord, V> NestedMap<K1, K2, V> {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    pub fn inner(&self) -> &BTreeMap<K1, BTreeMap<K2, V>> {
        &self.inner
    }

    pub fn len(&self) -> usize {
        self.inner.iter().map(|(_key1, nested)| nested.len()).sum()
    }

    pub fn contains(&self, key1: &K1, key2: &K2) -> bool {
        match self.inner.get(key1) {
            Some(nested) => nested.contains_key(key2),
            None => false,
        }
    }

    pub fn get_mut(&mut self, key1: &K1, key2: &K2) -> Option<&mut V> {
        self.inner
            .get_mut(key1)
            .and_then(|nested| nested.get_mut(key2))
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
    pub fn into_iter(self) -> impl Iterator<Item = (K1, K2, V)> {
        self.inner
            .into_iter()
            .flat_map(|(k1, nested)| nested.into_iter().map(move |(k2, v)| (k1.clone(), k2, v)))
    }

    pub fn iter_keys(&self) -> impl Iterator<Item = (&K1, &K2)> {
        self.inner
            .iter()
            .flat_map(|(k1, nested)| nested.iter().map(move |(k2, _)| (k1, k2)))
    }
}

impl<K1: Eq + Ord + Clone, K2: Eq + Ord + Clone, V> NestedMap<K1, K2, V> {
    /// Removes elements specified by the predicate and returns removed entries
    pub fn extract_if(&mut self, mut f: impl FnMut(&K1, &K2, &mut V) -> bool) -> Vec<(K1, K2, V)> {
        let mut result = Vec::new();
        self.inner.retain(|key1, nested| {
            let mut to_remove = Vec::with_capacity(nested.len());
            for (key2, v) in nested.iter_mut() {
                if f(key1, key2, v) {
                    // TODO: optimize out clones when `hash_extract_if` stabilizes
                    to_remove.push(key2.clone());
                }
            }
            for key2 in to_remove.into_iter() {
                let value = nested.remove(&key2).unwrap();
                result.push((key1.clone(), key2, value));
            }
            !nested.is_empty()
        });
        result
    }
}

impl<K1: Eq + Ord, K2: Eq + Ord, V> FromIterator<(K1, K2, V)> for NestedMap<K1, K2, V> {
    fn from_iter<I: IntoIterator<Item = (K1, K2, V)>>(iter: I) -> Self {
        let mut result = Self::new();
        for (key1, key2, value) in iter {
            result.insert(key1, key2, value);
        }
        result
    }
}
