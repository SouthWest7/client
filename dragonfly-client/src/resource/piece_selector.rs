use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{Semaphore};
use dashmap::DashMap;
use super::piece_collector::{CollectedPiece};

/// Piece Selector is designed to be used in a SPSC (single-producer, single-consumer) pattern:
/// - Task calls `insert()` to push pieces which is received from Piece Collector.
/// - Task calls `select_with()` to pop a piece according to custom selection rule.
/// - Semaphore is used as a counting wakeup mechanism (no missed wakeups).
pub struct PieceSelector {
    collected_pieces: Arc<DashMap<u32, CollectedPiece>>,
    children_need_count: Arc<DashMap<u32, u32>>,
    is_piece_selected: Arc<DashMap<u32, bool>>,
    available: Arc<Semaphore>, // counts number of buffered pieces (or wake-ups after close)
    closed: Arc<AtomicBool>,
}

impl PieceSelector {
    /// Creates a new selector.
    pub fn new() -> Self {
        Self {
            collected_pieces: Arc::new(DashMap::new()),
            children_need_count: Arc::new(DashMap::new()),
            is_piece_selected: Arc::new(DashMap::new()),
            available: Arc::new(Semaphore::new(0)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Inserts a collected piece into the selector.
    /// Collector side only (SPSC).
    pub async fn insert(&self, piece: CollectedPiece) {
        // Check if piece is already selected
        if self.is_piece_selected.contains_key(&piece.number) {
            return;
        }
        // Check if piece number already exists in collected_pieces
        if let Some(mut entry) = self.collected_pieces.get_mut(&piece.number) {
            // Piece exists, merge the new parents with existing ones
            entry.parents.extend(piece.parents);
        } else {
            // Piece doesn't exist, create new entry with (length, parents)
            self.collected_pieces.insert(piece.number, CollectedPiece{
                number: piece.number,
                length: piece.length,
                parents: piece.parents,
            });
            // Initially, piece is not selected
            self.is_piece_selected.insert(piece.number, false);
            // increment "available pieces" counter
            self.available.add_permits(1);  
        }
    }

    /// Close the selector. After close:
    /// - `select_with()` will keep draining existing pieces
    /// - then return `None` once empty
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        // wake up task if it's waiting
        self.available.add_permits(1);
    }

    /// Returns current buffered length (best-effort).
    pub async fn len(&self) -> usize {
        self.collected_pieces.len()
    }

    /// Selects and removes a piece based on your custom selection function.
    ///
    /// `select` receives a slice of buffered pieces and returns:
    /// - `Some(index)` -> pop that element
    /// - `None` -> "nothing selectable right now"
    ///
    /// Returns `None` if closed and buffer becomes empty (or no selectable items remain).
    pub async fn select_with<F>(&self, mut select: F) -> Option<CollectedPiece>
    where
        F: FnMut(&[CollectedPiece]) -> Option<usize>,
    {
        loop {
            // Wait until at least one piece is available, OR closed wakes us up.
            let _permit = self.available.acquire().await.ok()?;

            // Build a temporary Vec<CollectedPiece> from DashMap for selection
            let mut pieces = Vec::new();
            for entry in self.collected_pieces.iter() {
                pieces.push(CollectedPiece {
                    number: entry.value().number,
                    length: entry.value().length,
                    parents: entry.value().parents.clone(),
                });
            }

            // If there are pieces, try to select one.
            if let Some(i) = select(&pieces) {
                if i < pieces.len() {
                    let selected = pieces.swap_remove(i);
                    let piece_number = selected.number;
                    // Remove the selected piece from collected_pieces
                    self.collected_pieces.remove(&piece_number);
                    
                    // Mark the piece as selected
                    self.is_piece_selected.insert(piece_number, true);
                    return Some(selected);
                }
                // If select returned an invalid index, treat as "not selectable".
            }

            // No selectable piece at the moment.
            // If closed and nothing selectable, we should stop.
            if self.closed.load(Ordering::Relaxed) {
                return None;
            }

            // Put the permit back so the counter doesn't drift and cause deadlocks.
            self.available.add_permits(1);

            // Yield to avoid a hot loop if select keeps returning None.
            tokio::task::yield_now().await;
        }
    }

    /// Randomly pop one piece (uniform over indices).
    pub async fn select_random(&self) -> Option<CollectedPiece> {
        self.select_with(|buf| {
            if buf.is_empty() {
                None
            } else {
                Some(fastrand::usize(..buf.len()))
            }
        })
        .await
    }

    /// FIFO pop (take the oldest). This uses O(n) removal.
    pub async fn select_fifo(&self) -> Option<CollectedPiece> {
        self.select_with(|buf| if buf.is_empty() { None } else { Some(0) })
            .await
    }
}
