use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{Mutex, Semaphore};
use super::piece_collector::CollectedPiece;

/// Piece Selector is designed to be used in a single-producer, single-consumer pattern:
/// - Task calls `insert()` to push pieces which is received from Piece Collector.
/// - Task calls `select_with()` to pop a piece according to custom selection rule.
/// - Semaphore is used as a counting wakeup mechanism (no missed wakeups).
pub struct PieceSelector {
    buf: Arc<Mutex<Vec<CollectedPiece>>>,
    available: Arc<Semaphore>, // counts number of buffered pieces (or wake-ups after close)
    closed: Arc<AtomicBool>,
}

impl PieceSelector {
    /// Creates a new selector.
    pub fn new() -> Self {
        Self {
            buf: Arc::new(Mutex::new(Vec::new())),
            available: Arc::new(Semaphore::new(0)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Inserts a collected piece into the selector.
    /// Collector side only (SPSC).
    pub async fn insert(&self, piece: CollectedPiece) {
        {
            let mut g = self.buf.lock().await;
            g.push(piece);
        }
        self.available.add_permits(1);
    }

    /// Close the selector. After close:
    /// - `select_with()` will keep draining existing pieces
    /// - then return `None` once empty
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        // Wake the consumer if it's waiting.
        self.available.add_permits(1);
    }

    /// Returns current buffered length (best-effort).
    pub async fn len(&self) -> usize {
        self.buf.lock().await.len()
    }

    /// Returns None only when closed AND buffer is empty.
    pub async fn select_with<F>(&self, mut select: F) -> Option<CollectedPiece>
    where
        F: FnMut(&[CollectedPiece]) -> usize,
    {
        loop {
            // Wait for "at least one item" or a wakeup caused by close().
            let _permit = self.available.acquire().await.ok()?;

            let mut g = self.buf.lock().await;

            if g.is_empty() {
                // This can happen only if we were woken by close() (extra permit).
                if self.closed.load(Ordering::Relaxed) {
                    return None;
                }
                // Spurious wakeup (should be rare). Continue waiting.
                continue;
            }

            let i = select(&g);
            return Some(g.swap_remove(i));
        }
    }

    /// Randomly pop one piece (uniform over indices).
    pub async fn select_random(&self) -> Option<CollectedPiece> {
        self.select_with(|buf| fastrand::usize(..buf.len())).await
    }

    /// FIFO pop (take the oldest)
    pub async fn select_fifo(&self) -> Option<CollectedPiece> {
        self.select_with(|_| 0).await
    }
}
