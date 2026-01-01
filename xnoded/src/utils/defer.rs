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
