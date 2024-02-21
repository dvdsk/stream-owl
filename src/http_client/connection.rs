use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use derivative::Derivative;
use http::method::Method;
use http::{header, HeaderValue, Request};
use http_body_util::Empty;
use hyper::body::Incoming;
use hyper::client::conn::http1::{self, SendRequest};
use tokio::net::{TcpSocket, TcpStream};
use tokio::task::JoinSet;
use tracing::instrument;

use super::{error, FutureTimeout};
use super::{error::Error, Cookies};
use crate::BandwidthCallback;
use crate::network::{BandwidthLim, Network};

mod throttle_io;
use throttle_io::ThrottlableIo;

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct BandwidthMonitor<B> {
    last_reported_bandwidth: Arc<AtomicUsize>,
    last_update_at: Instant,
    read_since_last_report: usize,
    #[derivative(Debug = "ignore")]
    report: B,
}

impl<B: BandwidthCallback> BandwidthMonitor<B> {
    fn new(report: B, last_reported_bandwidth: Arc<AtomicUsize>) -> BandwidthMonitor<B> {
        Self {
            // init with something crazy so the
            // first update will trigger a report
            last_reported_bandwidth,
            last_update_at: Instant::now(),
            read_since_last_report: 0,
            report,
        }
    }

    fn last_reported_bandwidth(&self) -> usize {
        self.last_reported_bandwidth.load(Ordering::Relaxed)
    }

    fn set_last_reported_bandwidth(&self, val: usize) {
        self.last_reported_bandwidth.store(val, Ordering::Relaxed)
    }

    /// future work: test/refine, enable multiple strategies?
    fn update(&mut self, read: usize) {
        self.read_since_last_report += read;

        let margin = self.last_reported_bandwidth() / 10;
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_update_at);

        // do not update more then once per 200ms
        if elapsed < Duration::from_millis(200) {
            return;
        }

        // in 0.001 bytes per millisec aka bytes/sec
        let curr_bandwidth = self.read_since_last_report * 1000 / (elapsed.as_millis() as usize);
        if self.last_reported_bandwidth().abs_diff(curr_bandwidth) > margin {
            let _ignore_no_space_left_error =
                self.report.perform(curr_bandwidth);
            self.set_last_reported_bandwidth(curr_bandwidth);
            self.last_update_at = now;
            self.read_since_last_report = 0;
        }
    }
}

#[derive(Debug)]
pub(crate) struct Connection {
    pub request_sender: SendRequest<Empty<Bytes>>,
    // when the joinset drops the connection is ended
    _connection: JoinSet<()>,
}

pub(crate) type HyperResponse = hyper::Response<Incoming>;
impl Connection {
    pub(crate) async fn new<B: BandwidthCallback>(
        url: &hyper::Uri,
        restriction: &Option<Network>,
        bandwidth_lim: &BandwidthLim,
        bandwidth: Arc<AtomicUsize>,
        bandwidth_callback: B,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let bandwidth_monitor = BandwidthMonitor::new(bandwidth_callback, bandwidth);
        let tcp = new_tcp_stream(&url, &restriction).await?;
        let io = ThrottlableIo::new(tcp, bandwidth_lim, bandwidth_monitor)
            .map_err(Error::SocketConfig)?;
        let (request_sender, conn) = http1::handshake(io)
            .with_timeout(timeout)
            .await
            .map_err(error::Handshake::timed_out)?
            .map_err(error::Handshake::Other)?;

        let mut connection = JoinSet::new();
        connection.spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("Error in connection: {}", e);
            }
        });
        Ok(Self {
            request_sender,
            _connection: connection,
        })
    }

    #[instrument(level = "trace", skip(self), ret, err)]
    pub(crate) async fn send_initial_request(
        &mut self,
        url: &hyper::Uri,
        cookies: &Cookies,
        range: &str,
        timeout: Duration,
    ) -> Result<HyperResponse, Error> {
        let host = url.host().ok_or(Error::UrlWithoutHost)?;
        let host = HeaderValue::from_str(host).map_err(Error::InvalidHost)?;
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header(header::HOST, host.clone())
            .header(header::USER_AGENT, "stream-owl")
            .header(header::ACCEPT, "*/*")
            .header(header::CONNECTION, "keep-alive")
            .header(header::RANGE, range);

        cookies.add_to(&mut request);
        let request = request.body(Empty::<Bytes>::new())?;

        let response = self
            .request_sender
            .send_request(request)
            .with_timeout(timeout)
            .await
            .map_err(error::SendingRequest::timed_out)?
            .map_err(error::SendingRequest::Other)?;
        Ok(response)
    }

    pub(crate) async fn send_range_request(
        &mut self,
        url: &hyper::Uri,
        host: &HeaderValue,
        cookies: &Cookies,
        range: &str,
        timeout: Duration,
    ) -> Result<HyperResponse, Error> {
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(url.clone())
            .header(header::HOST, host.clone())
            .header(header::USER_AGENT, "stream-owl")
            .header(header::ACCEPT, "*/*")
            .header(header::CONNECTION, "keep-alive")
            .header(header::RANGE, range);

        cookies.add_to(&mut request);
        let request = request.body(Empty::<Bytes>::new())?;

        let response = self
            .request_sender
            .send_request(request)
            .with_timeout(timeout)
            .await
            .map_err(error::SendingRequest::timed_out)?
            .map_err(error::SendingRequest::Other)?;
        Ok(response)
    }
}

async fn resolve_dns(host: &str) -> Result<IpAddr, Error> {
    use hickory_resolver::config::{ResolverConfig, ResolverOpts};
    use hickory_resolver::TokioAsyncResolver;
    let resolver = TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());

    resolver
        .lookup_ip(host)
        .await?
        .iter()
        .next()
        .ok_or(Error::DnsEmpty)
}

async fn new_tcp_stream(
    url: &hyper::Uri,
    restriction: &Option<Network>,
) -> Result<TcpStream, Error> {
    let bind_addr = restriction
        .as_ref()
        .map(Network::addr)
        .unwrap_or(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));
    let bind_addr = SocketAddr::new(bind_addr, 0);

    let host = url.host().expect("stream urls always have a host");
    let host = resolve_dns(host).await?;
    let port = url.port().map(|p| p.as_u16()).unwrap_or(80);
    let connect_addr = SocketAddr::new(host, port);

    let socket = TcpSocket::new_v4().map_err(Error::SocketCreation)?;
    socket.bind(bind_addr).map_err(Error::Restricting)?;
    Ok(socket
        .connect(connect_addr)
        .await
        .map_err(Error::Connecting)?)
}
