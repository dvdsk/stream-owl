use std::ops::Range;

use http::StatusCode;
use hyper::body::Incoming;

use crate::http_client::headers;

use super::headers::content_length;

#[derive(Debug)]
pub(crate) enum ValidResponse {
    Ok {
        stream: Incoming,
        /// servers are allowed to leave out Content-Length :(
        /// a livestream could leave it out for example
        size: Option<u64>,
    },
    PartialContent {
        stream: Incoming,
        size: Option<u64>,
        range: Range<u64>,
    },
    RangeNotSatisfiable {
        size: Option<u64>,
    },
}

#[derive(Debug, Clone, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("Response with status PARTIAL_CONTENT had an incorrect header: {0}")]
    /// Response with status PARTIAL_CONTENT had an incorrect header"
    InvalidPartialContentHeader(headers::Error),
    #[error("Response with status RANGE_NOT_SATISFIABLE had an incorrect header: {0}")]
    /// Response with status RANGE_NOT_SATISFIABLE had an incorrect header"
    InvalidRangeNotSatHeader(headers::Error),
    #[error("Response with status OK had an incorrect header: {0}")]
    /// Response with status OK had an incorrect header"
    InvalidOkHeader(headers::Error),
    #[error("Server reports a problem with our request, statuscode: {0}")]
    /// Server reports a problem with our request"
    IncorrectStatus(StatusCode),
    #[error("The http spec does not allow range: * (indicating unsatisfied) with a PARTIAL_CONTENT status")]
    /// The http spec does not allow range"
    UnsatisfiedRangeInPartialContent,
    /// Server did not send requested range
    #[error("Server did not send right range, requested: {requested:?}, got: {got:?}")]
    NotRequestedRange {
        requested: Range<u64>,
        got: Range<u64>,
    },
}

impl ValidResponse {
    pub(crate) fn from_hyper_and_range(
        response: hyper::Response<Incoming>,
        request_range: Range<u64>,
    ) -> Result<Self, Error> {
        match response.status() {
            StatusCode::OK => Self::check_ok(response),
            StatusCode::PARTIAL_CONTENT => Self::check_partial_content(response, request_range),
            StatusCode::RANGE_NOT_SATISFIABLE => Self::check_range_not_sat(response),
            code => Err(Error::IncorrectStatus(code)),
        }
    }
}

impl ValidResponse {
    pub(crate) fn stream_size(&self) -> Option<u64> {
        match self {
            ValidResponse::Ok { size, .. } => *size,
            ValidResponse::PartialContent { size, .. } => *size,
            ValidResponse::RangeNotSatisfiable { size } => *size,
        }
    }

    fn check_ok(response: hyper::Response<Incoming>) -> Result<ValidResponse, Error> {
        let size = content_length(&response).map_err(Error::InvalidOkHeader)?;
        Ok(ValidResponse::Ok {
            stream: response.into_body(),
            size,
        })
    }

    fn check_partial_content(
        response: hyper::Response<Incoming>,
        requested_range: Range<u64>,
    ) -> Result<ValidResponse, Error> {
        let size =
            headers::range_content_length(&response).map_err(Error::InvalidPartialContentHeader)?;
        let range = headers::range(&response)
            .map_err(Error::InvalidPartialContentHeader)?
            .ok_or(Error::UnsatisfiedRangeInPartialContent)?;
        if range.start != requested_range.start {
            return Err(Error::NotRequestedRange {
                requested: requested_range,
                got: range,
            });
        }

        Ok(ValidResponse::PartialContent {
            size,
            range,
            stream: response.into_body(),
        })
    }

    fn check_range_not_sat(response: http::Response<Incoming>) -> Result<ValidResponse, Error> {
        let size =
            headers::range_content_length(&response).map_err(Error::InvalidRangeNotSatHeader)?;

        Ok(ValidResponse::RangeNotSatisfiable { size })
    }
}
