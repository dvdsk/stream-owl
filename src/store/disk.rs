use std::ffi::OsStr;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use derivative::Derivative;
use rangemap::RangeSet;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, instrument};

use crate::store::disk::progress::FlushNeeded;

use self::progress::Progress;

use super::range_watch;

pub mod progress;

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Disk {
    file_pos: u64,
    last_write: u64,
    #[derivative(Debug = "ignore")]
    file: File,
    progress: Progress,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Could not write downloaded data to disk, os returned: {0}")]
    WritingData(std::io::Error),
    #[error("Could not read downloaded data from disk, os returned: {0}")]
    ReadingData(std::io::Error),
    #[error("Could not store download/stream progress to disk: {0}")]
    UpdatingProgress(progress::Error),
    #[error("Could not seek while preparing write, os returned: {0}")]
    SeekForWriting(std::io::Error),
    #[error("Could not seek while preparing read, os returned: {0}")]
    SeekForReading(std::io::Error),
    #[error("Could not flush data to disk, os returned: {0}")]
    FlushingData(std::io::Error),
    #[error("Could not flush progress info to disk: {0}")]
    FlushingProgress(progress::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum OpenError {
    #[error("Could not open file to write stream to as writable")]
    OpenForWriting(std::io::Error),
    #[error("Could not open file to write stream to as readable")]
    OpenForReading(std::io::Error),
    #[error("Path may not end with .progress")]
    InvalidPath,
    #[error("Could not load progress from disk or prepare for storing it")]
    OpeningProgress(progress::Error),
}

impl Disk {
    pub(super) async fn new(
        path: PathBuf,
        range_tx: range_watch::Sender,
    ) -> Result<Self, OpenError> {
        if path.extension() == Some(OsStr::new("progress")) {
            return Err(OpenError::InvalidPath);
        }

        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .truncate(false)
            .open(&path)
            .await
            .map_err(OpenError::OpenForWriting)?;

        let progress = Progress::new(path, range_tx, 0)
            .await
            .map_err(OpenError::OpeningProgress)?;

        Ok(Self {
            file_pos: 0,
            last_write: 0,
            file,
            progress,
        })
    }

    #[instrument(level = "debug")]
    async fn seek_for_write(&mut self, to: u64) -> Result<(), Error> {
        self.file
            .seek(SeekFrom::Start(to))
            .await
            .map_err(Error::SeekForWriting)?;
        if let FlushNeeded::Yes = self.progress.end_section(self.file_pos, to).await {
            self.flush().await?;
        }
        self.file_pos = to;
        self.last_write = to;
        Ok(())
    }

    #[instrument(level="trace", skip(buf), fields(buf_len = buf.len()), ret)]
    pub(super) async fn write_at(&mut self, buf: &[u8], pos: u64) -> Result<NonZeroUsize, Error> {
        if pos != self.last_write {
            self.seek_for_write(pos).await?;
        } else if pos != self.file_pos {
            self.file
                .seek(SeekFrom::Start(pos))
                .await
                .map_err(Error::SeekForWriting)?;
        }
        let written = self.file.write(buf).await.map_err(Error::WritingData)?;
        self.last_write += written as u64;
        self.file_pos = self.last_write;
        if let FlushNeeded::Yes = self.progress.update(self.file_pos).await {
            self.flush().await?;
        }
        Ok(NonZeroUsize::new(written).expect("File should always accept more bytes"))
    }

    pub(super) async fn read_at(&mut self, buf: &mut [u8], pos: u64) -> Result<usize, Error> {
        if pos != self.file_pos {
            self.file
                .seek(SeekFrom::Start(pos))
                .await
                .map_err(Error::SeekForReading)?;
            self.file_pos = pos;
        }
        self.file.read(buf).await.map_err(Error::ReadingData)
    }

    pub(super) fn ranges(&self) -> RangeSet<u64> {
        self.progress.ranges.clone()
    }

    pub(super) fn gapless_from_till(&self, pos: u64, last_seek: u64) -> bool {
        self.progress
            .ranges
            .gaps(&(pos..last_seek))
            .next()
            .is_none()
    }

    pub(super) fn set_range_tx(&mut self, tx: range_watch::Sender) {
        self.progress.range_tx = tx
    }

    pub(super) fn last_read_pos(&self) -> u64 {
        self.file_pos
    }

    pub(super) fn n_supported_ranges(&self) -> usize {
        usize::MAX
    }

    pub(super) fn into_parts(self) -> range_watch::Sender {
        let Self {
            progress: Progress { range_tx, .. },
            ..
        } = self;
        range_tx
    }

    // OPT: see if we can use this to optimize write at
    // (get rid of the seek check)
    pub(super) fn writer_jump(&mut self, _to_pos: u64) {}

    #[instrument(level = "debug")]
    pub(crate) async fn flush(&mut self) -> Result<(), Error> {
        self.file.flush().await.map_err(Error::FlushingData)?;
        self.progress
            .flush()
            .await
            .map_err(Error::FlushingProgress)?;
        debug!("flushed data and progress to disk");
        Ok(())
    }
}
