use std::path::PathBuf;

use tokio::sync::{mpsc, oneshot};

use crate::network::BandwidthAllowed;
use crate::store::migrate::MigrationError;
use crate::stream::GetReaderError;
use crate::{BandwidthLimit, Reader, StreamError, StreamId};

use super::task::bandwidth;
use super::Command;

#[derive(Debug)]
pub enum HandleOp {
    Pause {
        id: StreamId,
        tx: oneshot::Sender<()>,
    },
    Unpause {
        id: StreamId,
        tx: oneshot::Sender<()>,
    },
    TryGetReader {
        id: StreamId,
        tx: oneshot::Sender<Result<Reader, GetReaderError>>,
    },
    MigrateToLimitedMemBackend {
        size: usize,
        id: StreamId,
        tx: oneshot::Sender<Result<(), MigrationError>>,
    },
    MigrateToUnlimitedMemBackend {
        id: StreamId,
        tx: oneshot::Sender<Result<(), MigrationError>>,
    },
    MigrateToDiskBackend {
        id: StreamId,
        path: PathBuf,
        tx: oneshot::Sender<Result<(), MigrationError>>,
    },
    Flush {
        id: StreamId,
        tx: oneshot::Sender<Result<(), StreamError>>,
    },
}

impl HandleOp {
    pub(super) fn id(&self) -> &StreamId {
        match self {
            HandleOp::Pause { id, .. } => id,
            HandleOp::Unpause { id, .. } => id,
            HandleOp::TryGetReader { id, .. } => id,
            HandleOp::MigrateToLimitedMemBackend { id, .. } => id,
            HandleOp::MigrateToUnlimitedMemBackend { id, .. } => id,
            HandleOp::MigrateToDiskBackend { id, .. } => id,
            HandleOp::Flush { id, .. } => id,
        }
    }

    pub(super) fn tx_closed(&self) -> bool {
        match self {
            HandleOp::Pause { tx, .. } => tx.is_closed(),
            HandleOp::Unpause { tx, .. } => tx.is_closed(),
            HandleOp::TryGetReader { tx, .. } => tx.is_closed(),
            HandleOp::MigrateToLimitedMemBackend { tx, .. } => tx.is_closed(),
            HandleOp::MigrateToUnlimitedMemBackend { tx, .. } => tx.is_closed(),
            HandleOp::MigrateToDiskBackend { tx, .. } => tx.is_closed(),
            HandleOp::Flush { tx, .. } => tx.is_closed(),
        }
    }
}

pub struct ManagedHandle {
    pub(crate) id: StreamId,
    pub(crate) cmd_tx: mpsc::Sender<Command>,
    pub(crate) bandwidth_tx: mpsc::Sender<bandwidth::Update>,
}

impl Drop for ManagedHandle {
    fn drop(&mut self) {
        self.cmd_tx
            .blocking_send(Command::CancelStream(self.id))
            .unwrap();
    }
}

use bandwidth::Update as B;
use Command as C;
use HandleOp as H;
impl ManagedHandle {
    pub async fn set_priority(&self, priority: usize) {
        self.bandwidth_tx
            .send(B::NewPriority {
                priority,
                id: self.id,
            })
            .await
            .expect("Stream handle should not drop before managed handle");
    }
    pub async fn limit_bandwidth(&self, limit: BandwidthLimit) {
        self.bandwidth_tx
            .send(B::NewStreamLimit {
                id: self.id,
                bandwidth: BandwidthAllowed::Limited(limit),
            })
            .await
            .expect("Stream handle should not drop before managed handle");
    }
    pub async fn remove_bandwidth_limit(&self) {
        self.bandwidth_tx
            .send(B::NewStreamLimit {
                id: self.id,
                bandwidth: BandwidthAllowed::UnLimited,
            })
            .await
            .expect("Stream handle should not drop before managed handle");
    }
    pub async fn pause(&mut self) {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(C::Handle(H::Pause { id: self.id, tx }))
            .await
            .expect("Stream handle should not drop before managed handle");
        rx.await
            .expect("Stream handle should not drop before managed handle")
    }
    pub async fn unpause(&mut self) {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(C::Handle(H::Unpause { id: self.id, tx }))
            .await
            .expect("Stream handle should not drop before managed handle");

        rx.await
            .expect("Stream handle should not drop before managed handle")
    }
    pub async fn try_get_reader(&mut self) -> Result<Reader, GetReaderError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(C::Handle(H::TryGetReader { id: self.id, tx }))
            .await
            .expect("Stream handle should not drop before managed handle");
        rx.await
            .expect("Stream handle should not drop before managed handle")
    }
    pub async fn migrate_to_limited_mem_backend(&self, size: usize) -> Result<(), MigrationError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(C::Handle(H::MigrateToLimitedMemBackend {
                id: self.id,
                tx,
                size,
            }))
            .await
            .expect("Stream handle should not drop before managed handle");
        rx.await
            .expect("Stream handle should not drop before managed handle")
    }
    pub async fn migrate_to_unlimited_mem_backend(&mut self) -> Result<(), MigrationError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(C::Handle(H::MigrateToUnlimitedMemBackend {
                id: self.id,
                tx,
            }))
            .await
            .expect("Stream handle should not drop before managed handle");
        rx.await
            .expect("Stream handle should not drop before managed handle")
    }
    pub async fn migrate_to_disk_backend(&mut self, path: PathBuf) -> Result<(), MigrationError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(C::Handle(H::MigrateToDiskBackend {
                id: self.id,
                tx,
                path,
            }))
            .await
            .expect("Stream handle should not drop before managed handle");
        rx.await
            .expect("Stream handle should not drop before managed handle")
    }
    pub async fn flush(&mut self) -> Result<(), StreamError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(C::Handle(H::Flush { id: self.id, tx }))
            .await
            .expect("Stream handle should not drop before managed handle");
        rx.await
            .expect("Stream handle should not drop before managed handle")
    }
}
