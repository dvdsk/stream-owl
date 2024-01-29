use std::io;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::task::Poll;

use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tracing::{info, instrument};

/// how many bytes after the previous disconnect to force another
type DisconnAt = u64;
#[derive(Debug, Clone)]
pub struct ConnControls {
    disconn_at: Arc<[DisconnAt]>,
    next: usize,
    wrote: u64,
}

#[derive(Debug)]
enum Should {
    Cut(usize),
    Disconn,
    DoNothing,
}

impl ConnControls {
    pub fn new(disconn_offsets: Vec<DisconnAt>) -> Self {
        Self {
            disconn_at: disconn_offsets.into_boxed_slice().into(),
            next: 1,
            wrote: 0,
        }
    }

    #[instrument(level = "trace")]
    fn should_disconn(&mut self, to_write: usize) -> Should {
        let Some(next_disconn) = self.disconn_at.get(self.next).copied() else {
            return Should::DoNothing;
        };

        if self.wrote >= next_disconn {
            self.next += 1;
            info!("will disconnect");
            Should::Disconn
        } else if self.wrote + to_write as u64 >= next_disconn {
            let allowed_len = next_disconn - self.wrote;
            info!("will disconnect at next request, cutting body to: {allowed_len}");
            Should::Cut(allowed_len as usize)
        } else {
            Should::DoNothing
        }
    }
}

pin_project! {
    pub struct TestConn {
        #[pin]
        stream: TcpStream,
        conn_controls: ConnControls,
        starting_write: AtomicBool,
    }
}

impl TestConn {
    pub(crate) fn new(stream: TcpStream, conn_controls: ConnControls) -> Self {
        Self {
            stream,
            conn_controls,
            starting_write: AtomicBool::new(true),
        }
    }
}

impl AsyncRead for TestConn {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();
        AsyncRead::poll_read(this.stream, cx, buf)
    }
}

fn disconn_err() -> Poll<Result<usize, std::io::Error>> {
    Poll::Ready(Err(io::Error::new(
        io::ErrorKind::Other,
        "Test server forced a disconnect",
    )))
}

impl AsyncWrite for TestConn {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = self.project();
        let buf = match this.conn_controls.should_disconn(buf.len()) {
            Should::Cut(allowed_len) => &buf[0..allowed_len],
            Should::Disconn => return disconn_err(),
            Should::DoNothing => buf,
        };
        let res = AsyncWrite::poll_write(this.stream, cx, buf);
        if let Poll::Ready(Ok(n_written)) = res {
            this.conn_controls.wrote += n_written as u64;
        };

        res
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.project();
        AsyncWrite::poll_flush(this.stream, cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.project();
        AsyncWrite::poll_shutdown(this.stream, cx)
    }
}
