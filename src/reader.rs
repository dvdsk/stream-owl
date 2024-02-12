use std::io::{self, Read, Seek};
use std::time::Duration;

use derivative::Derivative;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, OwnedMutexGuard};
use tracing::instrument;

use crate::store::{self, ReadError, Store, StoreReader};

mod prefetch;
use prefetch::Prefetch;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Reader {
    #[derivative(Debug = "ignore")]
    rt: Runtime,
    prefetch: Prefetch,
    #[derivative(Debug = "ignore")]
    seek_tx: mpsc::Sender<u64>,
    last_seek: u64,
    minimum_size: u64,
    #[derivative(Debug = "ignore")]
    store_reader: OwnedMutexGuard<StoreReader>,
    curr_pos: u64,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct CouldNotCreateRuntime(io::Error);

impl Reader {
    #[instrument(level = "trace", skip(store_reader))]
    pub(crate) fn new(
        prefetch: usize,
        seek_tx: mpsc::Sender<u64>,
        store_reader: OwnedMutexGuard<StoreReader>,
    ) -> Result<Self, CouldNotCreateRuntime> {
        Ok(Self {
            rt: Runtime::new().map_err(CouldNotCreateRuntime)?,
            prefetch: Prefetch::new(prefetch),
            seek_tx,
            last_seek: 0,
            minimum_size: 0,
            store_reader,
            curr_pos: 0,
        })
    }
}

fn size_unknown() -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        "could not seek from end, as size is unknown",
    )
}

fn stream_ended(_: tokio::sync::mpsc::error::SendError<u64>) -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "stream was ended")
}

impl Reader {
    #[instrument(level = "trace", skip(self))]
    fn absolute_pos(&mut self, rel_pos: io::SeekFrom) -> io::Result<u64> {
        let just_started = self.store_reader.stream_size().requests_analyzed() < 1;

        Ok(match rel_pos {
            io::SeekFrom::Start(bytes) => bytes,
            io::SeekFrom::Current(bytes) => self.curr_pos + bytes as u64,
            io::SeekFrom::End(bytes) => {
                let size = match self.store_reader.stream_size().known() {
                    Some(size) => size,
                    None if just_started => self
                        .store_reader
                        .stream_size()
                        .wait_for_known(&mut self.rt, Duration::from_secs(1))
                        .map_err(|_timeout| size_unknown())?,
                    None => Err(size_unknown())?,
                };
                size.saturating_sub(bytes as u64)
            }
        })
    }

    #[instrument(level = "trace", skip(self))]
    fn seek_in_stream(&mut self, pos: u64) -> io::Result<()> {
        self.seek_tx.blocking_send(pos).map_err(stream_ended)?;
        self.last_seek = pos;
        self.prefetch.reset();
        Ok(())
    }
}

fn seek_is_into_undownloaded(pos: u64, store: &OwnedMutexGuard<Store>) -> bool {
    !store.ranges().contains(&pos)
}

fn undownloaded_after_seek(pos: u64, stream_end: u64, store: &OwnedMutexGuard<Store>) -> bool {
    !store.gapless_from_till(pos, stream_end)
}

fn undownloaded_not_being_downloaded(
    last_seek: u64,
    pos: u64,
    stream_end: u64,
    store: &OwnedMutexGuard<Store>,
) -> bool {
    let first_gap = store
        .ranges()
        .gaps(&(pos..stream_end))
        .next()
        .expect("there is a gap as undownloaded after seek is true");

    last_seek > first_gap.start
}

impl Seek for Reader {
    // this moves the seek in the stream
    #[instrument(level = "debug", fields(absolute_pos))]
    fn seek(&mut self, rel_pos: io::SeekFrom) -> io::Result<u64> {
        let pos = self.absolute_pos(rel_pos)?;
        tracing::Span::current().record("absolute_pos", pos);

        self.minimum_size = self.minimum_size.max(self.curr_pos);
        self.curr_pos = pos;

        let store = self.store_reader.curr_store.clone().blocking_lock_owned();
        if seek_is_into_undownloaded(pos, &store) {
            tracing::debug!("Seek pos not yet in store: seeking in stream");
            self.seek_in_stream(pos)?;
            // if the writers where blocked by lack of space they
            // can write now. Since all the old can be overwritten
            self.store_reader.capacity.send_available();
            return Ok(pos);
        }

        let Some(stream_end) = self.store_reader.stream_size().known() else {
            /* TODO: jump to end of downloaded bit <dvdsk noreply@davidsk.dev> */
            tracing::debug!("Unknown stream end: seeking in stream");
            self.seek_in_stream(pos)?;
            return Ok(pos);
        };

        if undownloaded_after_seek(pos, stream_end, &store)
            && undownloaded_not_being_downloaded(self.last_seek, pos, stream_end, &store)
        {
            tracing::debug!(
                "section after seek pos not yet in store and not being downloaded: seeking in stream"
            );
            /* TODO: jump to end of downloaded bit <dvdsk noreply@davidsk.dev> */
            self.seek_in_stream(pos)?;
            return Ok(pos);
        }

        tracing::debug!("No need to seek in stream");
        return Ok(pos);
    }
}

impl Read for Reader {
    // A read of zero will only happen at the end of file
    #[instrument(level = "trace", skip(buf), ret)]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n_read1 = self.prefetch.read_from_prefetched(buf, self.curr_pos);
        self.curr_pos += n_read1 as u64;

        let n_read2 = self.rt.block_on(async {
            let res = self
                .store_reader
                .read_at(&mut buf[n_read1..], self.curr_pos)
                .await;
            let bytes = match res {
                Ok(bytes) => bytes,
                Err(ReadError::EndOfStream) => return Ok(0),
                // returns out of block_on closure, not function
                Err(ReadError::Store(other)) => return Err(handle_read_error(other).await),
            };

            self.curr_pos += bytes as u64;
            tracing::trace!("reader read: {bytes} bytes");

            self.prefetch
                .perform_if_needed(&mut self.store_reader, self.curr_pos, n_read1 + bytes)
                .await?;

            Ok(bytes)
        })?;

        Ok(n_read1 + n_read2)
    }
}

async fn handle_read_error(_error: store::Error) -> io::Error {
    todo!(
        "notify stream/end stream future with error (some sending required?)
          and turn into an appropriate io::Error for the reader"
    )
}
