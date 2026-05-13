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
    use std::cell::{Cell, RefCell};

    #[test]
    fn runs_on_drop() {
        let called = Cell::new(false);
        {
            let _d = defer(|| {
                called.set(true);
            });
            assert!(!called.get(), "should not call until drop");
        }
        assert!(called.get(), "defer should execute on drop");
    }

    #[test]
    fn multiple_defers_execute_in_lifo() {
        let order = RefCell::new(Vec::new());
        {
            let _d1 = defer(|| order.borrow_mut().push(1));
            let _d2 = defer(|| order.borrow_mut().push(2));
            assert!(order.borrow().is_empty());
        }
        assert_eq!(order.into_inner(), vec![2, 1]);
    }
}
