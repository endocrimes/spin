use std::collections::HashMap;

pub struct Table<V> {
    next_key: u32,
    tuples: HashMap<u32, V>,
}

impl<V> Table<V> {
    pub fn new() -> Self {
        Self {
            next_key: 0,
            tuples: HashMap::new(),
        }
    }

    pub fn push(&mut self, value: V) -> Result<u32, ()> {
        if self.tuples.len() == u32::MAX as usize {
            Err(())
        } else {
            loop {
                let key = self.next_key;
                self.next_key = self.next_key.wrapping_add(1);
                if self.tuples.contains_key(&key) {
                    continue;
                }
                self.tuples.insert(key, value);
                return Ok(key);
            }
        }
    }

    pub fn get(&self, key: u32) -> Option<&V> {
        self.tuples.get(&key)
    }

    pub fn remove(&mut self, key: u32) -> Option<V> {
        self.tuples.remove(&key)
    }
}
