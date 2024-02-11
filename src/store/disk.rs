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

use crate::util::MaybeLimited;
use crate::RangeUpdate;

use self::progress::Progress;

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
    #[instrument]
    pub(super) async fn new(path: PathBuf) -> Result<(Self, RangeSet<u64>), OpenError> {
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

        let progress = Progress::new(path, 0)
            .await
            .map_err(OpenError::OpeningProgress)?;
        let already_downloaded = progress.ranges.clone();

        Ok((
            Self {
                file_pos: 0,
                last_write: 0,
                file,
                progress,
            },
            already_downloaded,
        ))
    }

    #[instrument(level = "debug")]
    async fn seek_for_write(&mut self, to: u64) -> Result<(), Error> {
        self.file
            .seek(SeekFrom::Start(to))
            .await
            .map_err(Error::SeekForWriting)?;
        let flush_needed = self.progress.end_section(self.file_pos, to).await;
        if flush_needed {
            self.flush().await?;
        }
        self.file_pos = to;
        self.last_write = to;
        Ok(())
    }

    #[instrument(level="trace", skip(buf), fields(buf_len = buf.len()), ret)]
    pub(super) async fn write_at(
        &mut self,
        buf: &[u8],
        pos: u64,
    ) -> Result<(usize, RangeUpdate), Error> {
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
        let (range_update, flush_needed) = self.progress.update(self.file_pos).await;

        if flush_needed {
            self.flush().await?;
        }

        Ok((written, range_update))
    }

    #[instrument(level = "trace", skip(self, buf))]
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

    pub(super) fn last_read_pos(&self) -> u64 {
        self.file_pos
    }

    pub(super) fn n_supported_ranges(&self) -> MaybeLimited<NonZeroUsize> {
        MaybeLimited::NotLimited
    }

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
