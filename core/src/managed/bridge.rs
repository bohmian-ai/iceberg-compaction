//! Bridges the Iceberg rolling writer's events onto the managed observer.
//!
//! One rewrite fans out across several concurrent writer tasks, and each task's
//! rolling writer numbers its own outputs from zero. Those per-writer ordinals
//! are not comparable, so the bridge re-stamps every output with an ordinal
//! that is unique and open-ordered across the whole attempt. That is what makes
//! a reported output identity usable as evidence: it names one object produced
//! by one attempt, with no collision against a sibling task or a later attempt.
//!
//! The bridge is also where the attempt's output ledger lives. Every opened
//! output is recorded immediately and marked settled only when its close
//! completes, so a cancelled or failed attempt can still report the objects it
//! may have left behind.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use iceberg::writer::file_writer::rolling_writer::{RollingWriterEvent, RollingWriterObserver};

use super::observer::{AttemptId, OutputIdentity, RewriteEvent, RewriteObserver};

/// Shared, attempt-wide output ledger and ordinal allocator.
///
/// Held by every per-writer bridge in one rewrite so their outputs share a
/// single ordinal space.
#[derive(Debug)]
pub struct AttemptLedger {
    attempt_id: AttemptId,
    observer: Arc<dyn RewriteObserver>,
    next_ordinal: AtomicU64,
    next_completion_ordinal: AtomicU64,
    outputs: Mutex<HashMap<u64, OutputIdentity>>,
}

impl AttemptLedger {
    /// Creates a ledger for one attempt.
    #[must_use]
    pub fn new(attempt_id: AttemptId, observer: Arc<dyn RewriteObserver>) -> Arc<Self> {
        Arc::new(Self {
            attempt_id,
            observer,
            next_ordinal: AtomicU64::new(0),
            next_completion_ordinal: AtomicU64::new(0),
            outputs: Mutex::new(HashMap::new()),
        })
    }

    /// Returns the attempt this ledger belongs to.
    #[must_use]
    pub fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns every output the attempt opened, in ordinal order.
    ///
    /// Unsettled entries are included: an object whose close never completed
    /// may still exist in storage, and omitting it would understate what the
    /// attempt could have left behind.
    ///
    /// # Panics
    ///
    /// Panics only if a bridge panicked while holding the ledger lock, which
    /// would mean the writer path itself had already failed unrecoverably.
    #[must_use]
    pub fn outputs(&self) -> Vec<OutputIdentity> {
        let guard = self.outputs.lock().expect("attempt ledger lock poisoned");
        let mut outputs: Vec<OutputIdentity> = guard.values().cloned().collect();
        drop(guard);
        outputs.sort_by(|left, right| left.logical_ordinal.cmp(&right.logical_ordinal));
        outputs
    }

    /// Emits one event to the attempt's observer.
    pub fn emit(&self, event: RewriteEvent) {
        self.observer.on_event(event);
    }

    /// Creates a per-writer bridge that feeds this ledger.
    ///
    /// Each writer gets its own bridge because writer-local ordinals collide
    /// across writers; the bridges share this ledger so their re-stamped
    /// ordinals do not.
    #[must_use]
    pub fn writer_bridge(self: &Arc<Self>) -> Arc<RollingObserverBridge> {
        Arc::new(RollingObserverBridge {
            ledger: Arc::clone(self),
            ordinals: Mutex::new(HashMap::new()),
        })
    }

    fn allocate_ordinal(&self) -> u64 {
        self.next_ordinal.fetch_add(1, Ordering::Relaxed)
    }

    fn allocate_completion_ordinal(&self) -> u64 {
        self.next_completion_ordinal.fetch_add(1, Ordering::Relaxed)
    }

    fn record_opened(&self, ordinal: u64, path: String) {
        let mut guard = self.outputs.lock().expect("attempt ledger lock poisoned");
        guard.insert(ordinal, OutputIdentity {
            logical_ordinal: ordinal,
            path,
            settled: false,
        });
    }

    fn record_settled(&self, ordinal: u64) {
        let mut guard = self.outputs.lock().expect("attempt ledger lock poisoned");
        if let Some(entry) = guard.get_mut(&ordinal) {
            entry.settled = true;
        }
    }
}

/// Per-writer adapter from rolling-writer events to attempt-wide events.
#[derive(Debug)]
pub struct RollingObserverBridge {
    ledger: Arc<AttemptLedger>,
    ordinals: Mutex<HashMap<u64, u64>>,
}

impl RollingObserverBridge {
    /// Maps a writer-local ordinal to this attempt's ordinal.
    ///
    /// Returns `None` for an ordinal that was never opened, which cannot happen
    /// for a well-behaved writer; the bridge drops such an event rather than
    /// inventing an identity for it.
    fn attempt_ordinal(&self, local_ordinal: u64) -> Option<u64> {
        let guard = self.ordinals.lock().expect("bridge ordinal lock poisoned");
        guard.get(&local_ordinal).copied()
    }
}

impl RollingWriterObserver for RollingObserverBridge {
    fn on_event(&self, event: RollingWriterEvent) {
        let attempt_id = self.ledger.attempt_id();
        match event {
            RollingWriterEvent::OutputOpened {
                logical_ordinal,
                path,
            } => {
                let ordinal = self.ledger.allocate_ordinal();
                {
                    let mut guard = self.ordinals.lock().expect("bridge ordinal lock poisoned");
                    guard.insert(logical_ordinal, ordinal);
                }
                self.ledger.record_opened(ordinal, path.clone());
                self.ledger.emit(RewriteEvent::OutputOpened {
                    attempt_id,
                    logical_ordinal: ordinal,
                    path,
                });
            }
            RollingWriterEvent::CloseDecided {
                logical_ordinal,
                path,
                reason,
                target_file_size,
                written_size_estimate,
            } => {
                let Some(ordinal) = self.attempt_ordinal(logical_ordinal) else {
                    return;
                };
                self.ledger.emit(RewriteEvent::RollDecided {
                    attempt_id,
                    logical_ordinal: ordinal,
                    path,
                    reason,
                    target_file_size_bytes: target_file_size,
                    written_size_estimate_bytes: written_size_estimate,
                });
            }
            RollingWriterEvent::CloseSettled {
                logical_ordinal,
                completion_ordinal: _,
                path,
                reason,
                output_files,
            } => {
                let Some(ordinal) = self.attempt_ordinal(logical_ordinal) else {
                    return;
                };
                // The writer-local completion ordinal is discarded: it orders
                // one writer's closes, not the attempt's. The attempt-wide one
                // is allocated here, at the moment this close actually settled.
                let completion_ordinal = self.ledger.allocate_completion_ordinal();
                if output_files.is_some() {
                    self.ledger.record_settled(ordinal);
                }
                self.ledger.emit(RewriteEvent::OutputClosed {
                    attempt_id,
                    logical_ordinal: ordinal,
                    completion_ordinal,
                    path,
                    reason,
                    output_files,
                });
            }
        }
    }
}
