// Copyright (c) 2018 Nuclear Furnace
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//! Turnstyles are a way to provide futures-aware gated access in a sequential fashion.
//!
//! Callers will "join" the queue, and receive a future that will complete once they make it
//! through the turnstyle.  The turnstyle is controlled externally by some coordinator, and can
//! "turn" it to let the next waiter in line through.  Thus, you can have multiple waiters, which
//! can join the line at any time, and allow the coordinator to continually admit them through.
//!
//! This can be used to synchronize access to a resource, or to provide a synchronization point
//! to repeatedly calculation.
//!
//! One example is a network daemon that reloads its configuration and reestablishes all of its
//! listening sockets.  Normally, you might join (using `select`) both the listening socket
//! future and a close future so that you can shutdown the socket when its no longer required.
//!
//! With a turnstyle, you can join the queue every time you reload the configuration, and then
//! share that future to all of the newly created listeners.  Once the new listeners are ready, you
//! also do one turn on the turnstyle, which signals the last waiter in line -- a future shared
//! with all of the old listeners -- that they can now shutdown.  That same turnstyle can perform
//! this over and over without issue.
//!
//! Turnstyles internally protect themselves via a `Mutex` but are fast enough in normal cases that
//! you can `join` or `turn` from within a future without fear of stalling the executor.  If you're
//! joining at an extremely high frequency, you could potentially cause performance degradation.
extern crate futures;

use futures::{prelude::*, sync::oneshot};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, atomic::AtomicUsize, atomic::Ordering::SeqCst},
};

/// A future that waits to be notified, based on its place in line.
pub struct Waiter {
    inner: oneshot::Receiver<usize>,
}

impl Future for Waiter {
    type Error = ();
    type Item = usize;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> { self.inner.poll().map_err(|_| ()) }
}

/// An ordered queue of waiting participants.
///
/// Every turn of the turnstyle, the next participant in queue is notified and removed from the
/// queue.  If the queue is empty, `turn` is a noop.  Waiters receive their all-time position
/// through the turnstyle as their item i.e. the first waiter receives 0, the second receives 1,
/// etc.
///
/// Turnstyles can be cloned and are safe to share across threads.
#[derive(Clone)]
pub struct Turnstyle {
    waiters: Arc<Mutex<VecDeque<oneshot::Sender<usize>>>>,
    version: Arc<AtomicUsize>,
}

impl Turnstyle {
    /// Creates a new, empty turnstyle.
    pub fn new() -> Turnstyle {
        Turnstyle {
            waiters: Arc::new(Mutex::new(VecDeque::new())),
            version: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Joins the waiting queue.
    ///
    /// Returns a `Waiter` to the caller, which will complete when the turnstyle turns and reaches
    /// the caller's position in the queue.
    pub fn join(&self) -> Waiter {
        let (tx, rx) = oneshot::channel();
        {
            let mut waiters = self.waiters.lock().expect("turnstyle unable to join line");
            waiters.push_back(tx);
        }

        Waiter { inner: rx }
    }

    /// Turns once, letting a single waiter through.
    ///
    /// The `Waiter` is notified by the future completing.  The function returns `true` if a waiter
    /// was found/notified, `false` otherwise.
    pub fn turn(&self) -> bool {
        let waiter = {
            let mut waiters = self.waiters.lock().unwrap();
            waiters.pop_front()
        };

        if let Some(w) = waiter {
            let version = self.version.fetch_add(1, SeqCst);
            w.send(version).expect("turnstyle failed to signal next in line");
            true
        } else {
            false
        }
    }
}

impl Drop for Turnstyle {
    fn drop(&mut self) {
        while self.turn() {}
    }
}

#[cfg(test)]
mod tests {
    use super::Turnstyle;
    use futures::{future, Future, Async};

    #[test]
    fn single_waiter() {
        future::lazy(|| {
            let ts = Turnstyle::new();

            let mut w = ts.join();
            assert!(!w.poll().unwrap().is_ready());

            ts.turn();
            assert!(w.poll().unwrap().is_ready());

            future::ok::<_, ()>(())
        }).wait()
            .unwrap();
    }

    #[test]
    fn multiple_waiters() {
        future::lazy(|| {
            let ts = Turnstyle::new();
            let mut w1 = ts.join();
            let mut w2 = ts.join();
            let mut w3 = ts.join();

            assert!(!w1.poll().unwrap().is_ready());
            assert!(!w2.poll().unwrap().is_ready());
            assert!(!w2.poll().unwrap().is_ready());

            ts.turn();
            assert!(w1.poll().unwrap().is_ready());
            assert!(!w2.poll().unwrap().is_ready());
            assert!(!w3.poll().unwrap().is_ready());

            ts.turn();
            assert!(w2.poll().unwrap().is_ready());
            assert!(!w3.poll().unwrap().is_ready());

            ts.turn();
            assert!(w3.poll().unwrap().is_ready());

            future::ok::<_, ()>(())
        }).wait()
            .unwrap();
    }

    #[test]
    fn versions() {
        future::lazy(|| {
            let ts = Turnstyle::new();
            let mut w1 = ts.join();
            let mut w2 = ts.join();
            let mut w3 = ts.join();

            ts.turn();
            ts.turn();
            ts.turn();

            if let Async::Ready(w1v) = w1.poll().unwrap() {
                assert_eq!(w1v, 0);
            } else {
                panic!("waiter 1 was not ready");
            }

            if let Async::Ready(w2v) = w2.poll().unwrap() {
                assert_eq!(w2v, 1);
            } else {
                panic!("waiter 2 was not ready");
            }

            if let Async::Ready(w3v) = w3.poll().unwrap() {
                assert_eq!(w3v, 2);
            } else {
                panic!("waiter 3 was not ready");
            }

            future::ok::<_, ()>(())
        }).wait()
            .unwrap();
    }

    #[test]
    fn on_drop() {
        future::lazy(|| {
            let ts = Turnstyle::new();
            let mut w1 = ts.join();
            let mut w2 = ts.join();
            let mut w3 = ts.join();

            assert!(!w1.poll().unwrap().is_ready());
            assert!(!w2.poll().unwrap().is_ready());
            assert!(!w2.poll().unwrap().is_ready());

            drop(ts);

            assert!(w1.poll().unwrap().is_ready());
            assert!(w2.poll().unwrap().is_ready());
            assert!(w3.poll().unwrap().is_ready());

            future::ok::<_, ()>(())
        }).wait()
            .unwrap();
    }
}
