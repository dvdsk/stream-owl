use std::fmt::Display;
use std::num::ParseIntError;
use std::ops::Range;
use std::sync::Arc;

use http::{header, Response};

/// wrapper around [`http::header::ToStrError`] providing a slow [`PartialEq`]
/// implementation
#[derive(Debug, Clone)]
pub struct NotUtf8(Arc<http::header::ToStrError>);

impl NotUtf8 {
    fn wrap(error: http::header::ToStrError) -> Error {
        Error::NotUtf8(Self(Arc::new(error)))
    }
}

impl PartialEq for NotUtf8 {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

impl Display for NotUtf8 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("Header is not a utf8 string: {0}")]
    NotUtf8(NotUtf8),
    #[error("Could not parse content length as a number: {0}")]
    ContentLengthNotNumber(ParseIntError),
    #[error("Response with statuscode PartialContent should have a content range")]
    MissingContentRange,
    #[error("A content range or length should be in byte")]
    RangeNotBytes,
    #[error("Range header should always contain a `/` symbol")]
    InvalidRangeHeader,
    #[error("Content Range must have a `-` separating range start and stop")]
    MissingRangeDelimiter,
    #[error("Could not parse range start as a number: {0}")]
    RangeStartNotNumber(ParseIntError),
    #[error("Could not parse range stop as a number: {0}")]
    RangeStopNotNumber(ParseIntError),
    #[error("The range start must be smaller then the end")]
    RangeStartSmallerThenEnd,
}

pub fn content_length<T>(response: &Response<T>) -> Result<Option<u64>, Error> {
    let headers = response.headers();
    headers
        .get(header::CONTENT_LENGTH)
        .map(|header| {
            header
                .to_str()
                .map_err(NotUtf8::wrap)?
                .parse()
                .map_err(Error::ContentLengthNotNumber)
        })
        .transpose()
}

fn range_and_total<T>(response: &Response<T>) -> Result<(&str, &str), Error> {
    let headers = response.headers();
    headers
        .get(header::CONTENT_RANGE)
        .ok_or(Error::MissingContentRange)?
        .to_str()
        .map_err(NotUtf8::wrap)?
        .strip_prefix("bytes ")
        .ok_or(Error::RangeNotBytes)?
        .rsplit_once("/")
        .ok_or(Error::InvalidRangeHeader)
}

pub fn range_content_length<T>(response: &Response<T>) -> Result<Option<u64>, Error> {
    let content_length = range_and_total(response)?.1;
    if content_length == "*" {
        Ok(None)
    } else {
        content_length
            .parse()
            .map(Some)
            .map_err(Error::ContentLengthNotNumber)
    }
}

pub fn range<T>(response: &Response<T>) -> Result<Option<Range<u64>>, Error> {
    let range = range_and_total(response)?.0;
    if range == "*" {
        return Ok(None);
    }

    let (start, stop) = range.split_once("-").ok_or(Error::MissingRangeDelimiter)?;
    let start = start.parse().map_err(Error::RangeStartNotNumber)?;
    let stop = stop.parse().map_err(Error::RangeStartNotNumber)?;
    if stop < start {
        Err(Error::RangeStartSmallerThenEnd)
    } else {
        Ok(Some(start..stop))
    }
}

#[cfg(test)]
mod tests {
    use http::StatusCode;

    use super::*;

    #[test]
    fn test_range() {
        fn test_response<'a>(key: &'static str, val: &'a str) -> hyper::Response<&'static str> {
            let mut input = hyper::Response::new("");
            *input.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
            let headers = input.headers_mut();
            headers.insert(key, val.try_into().unwrap());
            input
        }

        for (testcase, expected) in [("content-range", "bytes */10000", None)]
            .map(|(key, val, expected)| (test_response(key, val), expected))
        {
            let expected: Option<Range<u64>> = expected;
            assert_eq!(range(&testcase).unwrap(), expected)
        }
    }
}
