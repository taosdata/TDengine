pub struct Defer<F: Fn()>(F);

pub fn defer<F>(f: F) -> Defer<F>
where
    F: Fn(),
{
    Defer(f)
}

impl<F: Fn()> Drop for Defer<F> {
    fn drop(&mut self) {
        (self.0)()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn defer_calls_closure_when_dropped() {
        let calls = Arc::new(AtomicUsize::new(0));
        let captured = calls.clone();

        {
            let _defer = defer(move || {
                captured.fetch_add(1, Ordering::SeqCst);
            });
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
