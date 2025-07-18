use super::TimeRange;
use chrono::{DateTime, Duration, Utc};

pub struct TimeChunks {
    range: TimeRange,
    inner: Option<ChunkIter>,
    visited: bool,
}

impl TimeChunks {
    pub fn new(range: TimeRange, unit: Duration) -> Self {
        let inner = if range.has_start() && range.has_end() {
            Some(ChunkIter::new(
                range.get_start().unwrap(),
                range.get_end().unwrap(),
                unit,
            ))
        } else {
            None
        };
        Self {
            range,
            inner,
            visited: false,
        }
    }
}

impl Iterator for TimeChunks {
    type Item = TimeRange;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner {
            Some(ref mut iter) => iter.next(),
            None => {
                if self.visited {
                    return None;
                }
                self.visited = true;
                Some(self.range)
            }
        }
    }
}

unsafe impl Send for TimeChunks {}

struct ChunkIter {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    unit: Duration,
}

impl ChunkIter {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>, unit: Duration) -> Self {
        Self { start, end, unit }
    }
}

impl Iterator for ChunkIter {
    type Item = TimeRange;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start >= self.end {
            return None;
        }
        let chunk_end = self.start + self.unit;
        let range = TimeRange::new().start(self.start);
        if chunk_end >= self.end {
            self.start = self.end;
            Some(range.end(self.end))
        } else {
            let range = range.end(chunk_end);
            self.start = chunk_end;
            Some(range.end(chunk_end))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_iter() {
        let start = Utc::now();
        let end = start + Duration::seconds(10);
        let unit = Duration::seconds(2);
        let iter = ChunkIter::new(start, end, unit);
        let mut chunks = Vec::new();
        for chunk in iter {
            chunks.push(chunk);
        }
        dbg!(&chunks);
    }

    #[test]
    fn test_time_chunks() {
        let start = Utc::now();
        let end = start + Duration::seconds(10);
        let range = TimeRange::new().start(start).end(end);
        let unit = Duration::seconds(2);
        let iter = TimeChunks::new(range, unit);
        let mut chunks = Vec::new();
        for chunk in iter {
            chunks.push(chunk);
        }
        dbg!(&chunks);
    }

    #[test]
    fn test_time_chunks_with_invalid_range() {
        let range = TimeRange::new();
        let range = range.start(Utc::now());
        let unit = Duration::seconds(2);
        let iter = TimeChunks::new(range, unit);
        let mut chunks = Vec::new();
        for chunk in iter {
            chunks.push(chunk);
        }
        dbg!(&chunks);
    }
}
