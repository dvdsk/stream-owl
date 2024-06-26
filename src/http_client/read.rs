use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::{Body, Incoming};

use crate::{BandwidthCallback, RangeCallback};
use crate::target::StreamTarget;
use super::size::Size;
use super::{error, FutureTimeout};
// todo fix error, should be task stream error?
use super::{error::Error, Client};

#[derive(Debug)]
pub(crate) struct InnerReader {
    stream: Incoming,
    client: Client,
}

#[derive(Debug)]
pub(crate) enum Reader {
    PartialData {
        inner: InnerReader,
        range: Range<u64>,
    },
    AllData {
        inner: InnerReader,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("Can not turn reader into a client while the server is still sending data to be read")]
pub struct StreamNotEmpty;

impl Reader {
    pub(crate) fn try_into_client(mut self) -> Result<Client, StreamNotEmpty> {
        if !self.inner().stream.is_end_stream() {
            return Err(StreamNotEmpty);
        }

        Ok(match self {
            Reader::PartialData {
                inner: InnerReader {
                    client, stream: _, ..
                },
                ..
            } => client,
            Reader::AllData {
                inner: InnerReader { client, .. },
                ..
            } => client,
        })
    }

    fn inner(&mut self) -> &mut InnerReader {
        match self {
            Reader::PartialData { inner, .. } => inner,
            Reader::AllData { inner, .. } => inner,
        }
    }

    pub(crate) fn stream_size(&self) -> Size {
        match self {
            Reader::PartialData { inner, .. } => inner.client.size.clone(),
            Reader::AllData { inner, .. } => inner.client.size.clone(),
        }
    }

    #[tracing::instrument(level = "trace", skip(target, self))]
    pub(crate) async fn stream_to_writer<B: BandwidthCallback, R: RangeCallback>(
        &mut self,
        target: &mut StreamTarget<B, R>,
        timeout: Duration,
    ) -> Result<(), Error> {
        if let Reader::PartialData { range, .. } = self {
            target.set_pos(range.start)
        }

        let stream = &mut self.inner().stream;
        loop {
            let Some(data) = get_next_data_frame(stream, timeout).await? else {
                return Ok(());
            };

            target
                .append(&data)
                .await
                .map_err(Error::WritingData)?;
        }
    }
}

impl InnerReader {
    pub(crate) fn new(stream: Incoming, client: Client) -> Self {
        Self { stream, client }
    }
}

#[tracing::instrument(level = "debug", err)]
async fn get_next_data_frame(
    stream: &mut Incoming,
    timeout: Duration,
) -> Result<Option<Bytes>, Error> {
    loop {
        let Some(frame) = stream
            .frame()
            .with_timeout(timeout)
            .await
            .map_err(error::ReadingBody::timed_out)?
        else {
            tracing::trace!("no more data frames");
            return Ok(None);
        };
        let frame = frame.map_err(error::ReadingBody::Other)?;

        match frame.into_data() {
            Ok(data) => return Ok(Some(data)),
            Err(_not_data) => tracing::trace!("Got non data frame"),
        }
    }
}
