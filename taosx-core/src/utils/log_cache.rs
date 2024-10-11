//！ 缓存连接器最后 n 行日志
use std::collections::LinkedList;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct LogCache {
    cache: Arc<Mutex<LinkedList<String>>>,
    max_size: usize,
}

impl LogCache {
    pub fn new(max_size: usize) -> Self {
        LogCache {
            cache: Arc::new(Mutex::new(LinkedList::new())),
            max_size,
        }
    }

    pub fn push(&self, log: String) {
        let mut cache = self.cache.lock().unwrap();
        cache.push_back(log);
        if cache.len() > self.max_size {
            cache.pop_front();
        }
    }

    pub fn get(self) -> String {
        let cache = self.cache.lock().unwrap();
        cache.iter().fold(String::new(), |acc, x| acc + x.as_str())
    }
}

unsafe impl std::marker::Send for LogCache {}
