use std::ops::Range;

use derivative::Derivative;
use http::header::InvalidHeaderValue;
use http::uri::InvalidUri;
use http::{header, HeaderValue, StatusCode};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use tracing::debug;

use crate::network::Network;
use crate::target::StreamTarget;
mod io;
mod read;
use read::Reader;
mod size;
pub(crate) use size::Size;

mod connection;
use connection::Connection;

use self::connection::HyperResponse;
use self::read::InnerReader;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // #[error("Error in connection to stream server, {source}")]
    // Hyper {
    //     #[from]
    //     source: hyper::Error,
    // },
    #[error("Error setting up the stream request, {0}")]
    Http(#[from] http::Error),
    #[error("Error creating socket, {0}")]
    SocketCreation(std::io::Error),
    #[error("Could not restrict traffic to one network interface, {0}")]
    Restricting(std::io::Error),
    #[error("Could not connect to server, {0}")]
    Connecting(std::io::Error),
    #[error("Could not resolve dns, resolve error, {0}")]
    DnsResolve(#[from] hickory_resolver::error::ResolveError),
    #[error("Could not resolve dns, no ip addresses for server")]
    DnsEmpty,
    #[error("Url had no server part")]
    UrlWithoutHost,
    #[error("server returned error,\n\tcode: {code}\n\tbody: {body:?}")]
    StatusNotOk {
        code: StatusCode,
        body: Option<String>,
    },
    #[error("server contained invalid characters, {0}")]
    InvalidHost(InvalidHeaderValue),
    #[error("server does not report we can seek in streams")]
    RangesNotAccepted,
    #[error("Invalid range")]
    InvalidRange,
    #[error("server redirected us however did not send location")]
    MissingRedirectLocation,
    #[error("The redirect location contained invalid characters, {0}")]
    BrokenRedirectLocation(header::ToStrError),
    #[error("The redirect location is not a url, {0}")]
    InvalidUriRedirectLocation(InvalidUri),
    #[error("server redirected us more then 10 times")]
    TooManyRedirects,
    #[error("Server did not send any data")]
    MissingFrame,
    #[error("Could not send request to server: {0}")]
    SendingRequest(hyper::Error),
    #[error("Could not set up connection to server: {0}")]
    Handshake(hyper::Error),
    #[error("Could not read response body: {0}")]
    ReadingBody(hyper::Error),
    #[error("Could now write the received data to storage: {0}")]
    WritingData(std::io::Error),
    #[error("Could not throw away body: {0}")]
    EmptyingBody(hyper::Error),
}

impl Error {
    async fn status_not_ok(response: hyper::Response<Incoming>) -> Self {
        let code = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .ok()
            .map(|body| body.to_bytes().to_vec())
            .map(|bytes| String::from_utf8(bytes).ok())
            .flatten();
        return Self::StatusNotOk { code, body };
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Cookies(Vec<String>);
impl Cookies {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn get_from(&mut self, response: &HyperResponse) {
        let new = response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|line| line.to_str().ok())
            .filter_map(|line| line.split_once(";"))
            .map(|(cookie, _meta)| cookie.to_string());
        self.0.extend(new);
    }

    fn add_to(&self, request: &mut http::request::Builder) {
        let headers = request.headers_mut().expect("builder never has an error");
        for cookie in &self.0 {
            let cookie =
                HeaderValue::from_str(cookie.as_str()).expect("was a valid header value before");
            headers.insert(header::COOKIE, cookie.clone());
        }
    }
}

/// A client that is currently streaming partial content
/// (the result of a range request)
#[derive(Debug)]
pub(crate) struct RangeSupported {
    stream: Incoming,
    client: Client,
}

impl RangeSupported {
    #[tracing::instrument(level = "trace")]
    pub(crate) fn into_reader(self) -> Reader {
        let Self { stream, client } = self;
        Reader::PartialData(InnerReader::new(stream, client))
    }

    pub(crate) fn stream_size(&self) -> Size {
        self.client.size.clone()
    }

    pub(crate) fn builder(&self) -> ClientBuilder {
        ClientBuilder {
            restriction: self.client.restriction.clone(),
            url: self.client.url.clone(),
            cookies: self.client.cookies.clone(),
            size: self.client.size.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RangeRefused {
    stream: Incoming,
    client: Client,
}

impl RangeRefused {
    pub(crate) fn into_reader(self) -> Reader {
        let Self { stream, client } = self;
        Reader::AllData(InnerReader::new(stream, client))
    }

    pub(crate) fn builder(&self) -> ClientBuilder {
        ClientBuilder {
            restriction: self.client.restriction.clone(),
            url: self.client.url.clone(),
            cookies: self.client.cookies.clone(),
            size: self.client.size.clone(),
        }
    }

    pub(crate) fn stream_size(&self) -> Size {
        self.client.size.clone()
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Client {
    host: HeaderValue,
    restriction: Option<Network>,
    url: hyper::Uri,
    #[derivative(Debug = "ignore")]
    conn: Connection,
    size: Size,
    cookies: Cookies,
}

impl Client {
    #[tracing::instrument(level = "trace", skip(self), ret)]
    async fn send_range_request(
        &mut self,
        range: &str,
    ) -> Result<hyper::Response<Incoming>, Error> {
        let response = self
            .conn
            .send_range_request(&self.url, &self.host, &self.cookies, range)
            .await?;
        self.cookies.get_from(&response);
        Ok(response)
    }
}

#[derive(Debug)]
pub(crate) enum StreamingClient {
    RangeSupported(RangeSupported),
    RangeRefused(RangeRefused),
}

#[derive(Debug, Clone)]
pub(crate) struct ClientBuilder {
    restriction: Option<Network>,
    url: hyper::Uri,
    cookies: Cookies,
    size: Size,
}

impl ClientBuilder {
    #[tracing::instrument(level = "debug")]
    pub(crate) async fn connect(
        self,
        target: StreamTarget,
    ) -> Result<StreamingClient, Error> {
        let Self {
            restriction,
            mut url,
            mut cookies,
            mut size,
        } = self;

        let Range { start, end } = target.next_range(size.known());
        let first_range = format!("bytes={start}-{end}");

        let mut conn = Connection::new(&url, &restriction).await?;
        let mut response = conn
            .send_initial_request(&url, &cookies, &first_range)
            .await?;
        size.update_from_headers(&response);
        cookies.get_from(&response);

        let mut numb_redirect = 0;
        let mut prev_url = url.clone();

        while response.status() == StatusCode::FOUND {
            if numb_redirect > 10 {
                return Err(Error::TooManyRedirects);
            }
            url = redirect_url(response)?;
            if url.host() != prev_url.host() {
                prev_url = url.clone();
                conn = Connection::new(&url, &restriction).await?;
            }
            response = conn
                .send_initial_request(&url, &cookies, &first_range)
                .await?;
            size.update_from_headers(&response);
            cookies.get_from(&response);

            debug!("redirecting to: {url}");
            numb_redirect += 1
        }

        let host = url.host().unwrap().parse().unwrap();
        let client = Client {
            host,
            restriction,
            url,
            conn,
            cookies,
            size,
        };
        match response.status() {
            StatusCode::OK => Ok(StreamingClient::RangeRefused(RangeRefused {
                stream: response.into_body(),
                client,
            })),
            StatusCode::PARTIAL_CONTENT => Ok(StreamingClient::RangeSupported(RangeSupported {
                stream: response.into_body(),
                client,
            })),
            StatusCode::RANGE_NOT_SATISFIABLE => {
                tracing::info!("{response:?}");
                todo!("redo without range")
            }
            _ => Err(Error::status_not_ok(response).await),
        }
    }
}

impl StreamingClient {
    #[tracing::instrument(level = "debug", ret)]
    pub(crate) async fn new(
        url: hyper::Uri,
        restriction: Option<Network>,
        size: Size,
        target: StreamTarget,
    ) -> Result<Self, Error> {
        ClientBuilder {
            restriction,
            url,
            cookies: Cookies::new(),
            size,
        }
        .connect(target)
        .await
    }

    pub(crate) fn stream_size(&self) -> Size {
        match self {
            StreamingClient::RangeSupported(client) => client.stream_size(),
            StreamingClient::RangeRefused(client) => client.stream_size(),
        }
    }
}

impl Client {
    #[tracing::instrument(level = "debug", err, ret)]
    pub(crate) async fn try_get_range(
        mut self,
        Range { start, end }: Range<u64>,
    ) -> Result<StreamingClient, Error> {
        assert!(Some(start) < self.stream_size().known());

        let range = format!("bytes={start}-{end}");
        let response = self.send_range_request(&range).await?;

        self.size.update_from_headers(&response);
        match response.status() {
            StatusCode::OK => Ok(StreamingClient::RangeRefused(RangeRefused {
                stream: response.into_body(),
                client: self,
            })),
            StatusCode::PARTIAL_CONTENT => Ok(StreamingClient::RangeSupported(RangeSupported {
                stream: response.into_body(),
                client: self,
            })),
            StatusCode::RANGE_NOT_SATISFIABLE => return Err(Error::InvalidRange),
            _ => Err(Error::status_not_ok(response).await),
        }
    }

    pub(crate) fn stream_size(&self) -> Size {
        self.size.clone()
    }
}

fn redirect_url<T>(redirect: hyper::Response<T>) -> Result<hyper::Uri, Error> {
    redirect
        .headers()
        .get(header::LOCATION)
        .ok_or(Error::MissingRedirectLocation)?
        .to_str()
        .map_err(Error::BrokenRedirectLocation)?
        .parse()
        .map_err(Error::InvalidUriRedirectLocation)
}
