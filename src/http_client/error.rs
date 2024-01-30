use http;
use http::header;
use http::header::InvalidHeaderValue;
use http::uri::InvalidUri;
use http::StatusCode;

use super::response;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Problem with the response send by the server
    #[error("Problem with the response send by the server: {0}")]
    Response(#[from] response::Error),
    /// Error setting up the stream request
    #[error("Error setting up the stream request, {0}")]
    Http(#[from] http::Error),
    /// Error creating socket
    #[error("Error creating socket, {0}")]
    SocketCreation(std::io::Error),
    /// Could not restrict traffic to one network interface
    #[error("Could not restrict traffic to one network interface, {0}")]
    Restricting(std::io::Error),
    /// Could not connect to server
    #[error("Could not connect to server, {0}")]
    Connecting(std::io::Error),
    /// Could not configure socket
    #[error("Could not configure socket, {0}")]
    SocketConfig(std::io::Error),
    /// Could not resolve dns, resolve error
    #[error("Could not resolve dns, resolve error, {0}")]
    DnsResolve(#[from] hickory_resolver::error::ResolveError),
    /// Could not resolve dns, no ip addresses for server
    #[error("Could not resolve dns, no ip addresses for server")]
    DnsEmpty,
    /// Url had no server part
    #[error("Url had no server part")]
    UrlWithoutHost,
    /// server returned error,\n\tcode: {code}\n\tbody: {body:?}
    #[error("server returned error,\n\tcode: {code}\n\tbody: {body:?}")]
    StatusNotOk {
        code: StatusCode,
        body: Option<String>,
    },
    /// Request host part contains invalid characters, it could be that
    /// the server send a corrupt forward address.
    #[error("Request host part contains invalid characters, {0}")]
    InvalidHost(InvalidHeaderValue),
    /// server redirected us however did not send location
    #[error("server redirected us however did not send location")]
    MissingRedirectLocation,
    /// The redirect location contained invalid characters
    #[error("The redirect location contained invalid characters, {0}")]
    BrokenRedirectLocation(header::ToStrError),
    /// The redirect location is not a url
    #[error("The redirect location is not a url, {0}")]
    InvalidUriRedirectLocation(InvalidUri),
    /// server redirected us more then 10 times
    #[error("server redirected us more then 10 times")]
    TooManyRedirects,
    /// Server did not send any data
    #[error("Server did not send any data")]
    MissingFrame,
    /// Could not send request to server
    #[error("Could not send request to server: {0}")]
    SendingRequest(#[from] SendingRequest),
    /// Could not set up connection to server
    #[error("Could not set up connection to server: {0}")]
    Handshake(#[from] Handshake),
    /// Could not read response body
    #[error("Could not read response body: {0}")]
    ReadingBody(#[from] ReadingBody),
    /// Could now write the received data to storage
    #[error("Could now write the received data to storage: {0}")]
    WritingData(std::io::Error),
}

impl Error {
    /// Does the error represent something outside the client device
    /// taking too long?
    pub fn is_external_timeout(&self) -> bool {
        match self {
            Error::Response(_) => false,
            Error::Http(_) => false,
            Error::SocketCreation(_) => false,
            Error::Restricting(_) => false,
            Error::Connecting(_) => todo!(),
            Error::SocketConfig(_) => false,
            Error::DnsResolve(e) => {
                matches!(e.kind(), hickory_resolver::error::ResolveErrorKind::Timeout)
            }
            Error::DnsEmpty => false,
            Error::UrlWithoutHost => false,
            Error::StatusNotOk { .. } => false,
            Error::InvalidHost(_) => false,
            Error::MissingRedirectLocation => false,
            Error::BrokenRedirectLocation(_) => false,
            Error::InvalidUriRedirectLocation(_) => false,
            Error::TooManyRedirects => false,
            Error::MissingFrame => false,
            Error::SendingRequest(e) => matches!(e, SendingRequest::TimedOut),
            Error::Handshake(e) => matches!(e, Handshake::TimedOut),
            Error::ReadingBody(e) => matches!(e, ReadingBody::TimedOut),
            Error::WritingData(_) => false,
        }
    }
}

macro_rules! timed_out {
    ($struct_name:ident ) => {
        #[derive(Debug, thiserror::Error)]
        pub enum $struct_name {
            #[error("Timed out")]
            TimedOut,
            #[error(transparent)]
            Other(hyper::Error),
        }

        impl $struct_name {
            pub(crate) fn timed_out(_: tokio::time::error::Elapsed) -> Self {
                Self::TimedOut
            }
        }
    };
}

timed_out!(SendingRequest);
timed_out!(Handshake);
timed_out!(ReadingBody);

impl PartialEq for Error {
    /// Warning: **slow**
    /// if the content of a variant can not be compared returns true.
    /// Uses to_string on generic Errors where many variants
    /// exist.
    fn eq(&self, other: &Self) -> bool {
        use Error as E;
        match (self, other) {
            (E::Response(e1), E::Response(e2)) => e1 == e2,
            (E::Http(e1), E::Http(e2)) => e1.to_string() == e2.to_string(),
            (E::SocketCreation(_), E::SocketCreation(_)) => true,
            (E::Restricting(_), E::Restricting(_)) => true,
            (E::Connecting(_), E::Connecting(_)) => true,
            (E::SocketConfig(_), E::SocketConfig(_)) => true,
            (E::DnsResolve(e1), E::DnsResolve(e2)) => {
                e1.kind().to_string() == e2.kind().to_string()
            }
            (E::DnsEmpty, E::DnsEmpty) => true,
            (E::UrlWithoutHost, E::UrlWithoutHost) => true,
            (
                E::StatusNotOk { code, body },
                E::StatusNotOk {
                    code: code2,
                    body: body2,
                },
            ) => code == code2 && body == body2,
            (E::InvalidHost(_), E::InvalidHost(_)) => true,
            (E::MissingRedirectLocation, E::MissingRedirectLocation) => true,
            (E::BrokenRedirectLocation(_), E::BrokenRedirectLocation(_)) => true,
            (E::InvalidUriRedirectLocation(_), E::InvalidUriRedirectLocation(_)) => true,
            (E::TooManyRedirects, E::TooManyRedirects) => true,
            (E::MissingFrame, E::MissingFrame) => true,
            (E::SendingRequest(_), E::SendingRequest(_)) => true,
            (E::Handshake(_), E::Handshake(_)) => true,
            (E::ReadingBody(_), E::ReadingBody(_)) => true,
            (E::WritingData(e1), E::WritingData(e2)) => e1.kind() == e2.kind(),
            _ => false,
        }
    }
}
