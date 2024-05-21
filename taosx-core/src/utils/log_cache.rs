//！ 缓存连接器最后 n 行日志
use std::collections::LinkedList;

pub struct LogCache {
    capacity: usize,
    inner: LinkedList<String>,
}

impl LogCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: LinkedList::new(),
        }
    }

    pub fn push(&mut self, log: String) {
        if self.inner.len() >= self.capacity {
            self.inner.pop_front();
        }
        self.inner.push_back(log);
    }

    pub fn get(self) -> String {
        self.inner
            .iter()
            .fold(String::new(), |acc, x| acc + x.as_str())
    }
}
