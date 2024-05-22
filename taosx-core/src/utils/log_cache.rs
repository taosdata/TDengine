//！ 缓存连接器最后 n 行日志
use std::cell::RefCell;
use std::collections::LinkedList;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LogCache {
    cache: Arc<RefCell<LinkedList<String>>>,
    max_size: usize,
}

impl LogCache {
    pub fn new(max_size: usize) -> Self {
        LogCache {
            cache: Arc::new(RefCell::new(LinkedList::new())),
            max_size,
        }
    }

    pub fn push(&self, log: String) {
        let mut cache = self.cache.borrow_mut();
        cache.push_back(log);
        if cache.len() > self.max_size {
            cache.pop_front();
        }
    }

    pub fn get(self) -> String {
        let cache = self.cache.borrow();
        cache.iter().fold(String::new(), |acc, x| acc + x.as_str())
    }
}

unsafe impl std::marker::Send for LogCache {}
