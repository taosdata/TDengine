use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug, PartialEq)]
pub enum ExecState {
    RunOnce,
    Waiting,
}

pub struct FrameFuture;
impl Unpin for FrameFuture {}

impl Future for FrameFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let context: &mut ExecState = unsafe { std::mem::transmute(context) };

        if *context == ExecState::RunOnce {
            *context = ExecState::Waiting;
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

pub struct FutureWithReturn<T> {
    pub item: std::rc::Rc<std::cell::RefCell<Option<T>>>,
}

impl<T> Unpin for FutureWithReturn<T> {}

impl<T> Future for FutureWithReturn<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let context: &mut ExecState = unsafe { std::mem::transmute(context) };

        if *context == ExecState::Waiting {
            Poll::Pending
        } else if let Some(item) = self.item.borrow_mut().take() {
            *context = ExecState::Waiting;
            Poll::Ready(item)
        } else {
            Poll::Pending
        }
    }
}

/// returns true if future is done
pub fn resume<'a>(future: &mut Pin<Box<dyn Future<Output = ()> + 'a>>) -> bool {
    let mut futures_context = ExecState::RunOnce;
    let futures_context_ref: &mut _ = unsafe { std::mem::transmute(&mut futures_context) };

    matches!(future.as_mut().poll(futures_context_ref), Poll::Ready(_))
}
