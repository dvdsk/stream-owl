# HAProxy config

> **Stream or download HTTP resources while using them as local files**

[![Crates.io](https://img.shields.io/crates/v/stream-owl?style=flat-square)](https://crates.io/crates/stream-owl)
[![Crates.io](https://img.shields.io/crates/d/stream-owl?style=flat-square)](https://crates.io/crates/stream-owl)
[![Docs.rs](https://img.shields.io/docsrs/stream-owl?style=flat-square)](https://docs.rs/stream-owl)
[![License](https://img.shields.io/badge/license-GPLv3-blue?style=flat-square)](LICENSE-GPLv3)

See also:
 - [API documentation](https://docs.rs/stream-owl)
 - [Changelog](CHANGELOG.md)

A library to use resources behind an HTTP API as if they were local. Stream every byte eagerly to disk (a download) or lazily get only what you are reading. Downloads prioritise the part of the stream you are currently reading. They keep their progress on disk you recover from crashes/poweroffs or just finish a download later. Switch between downloading or streaming without requesting bytes you already had. Throttle a streams bandwidth and prioritize one stream above another.

Features:
 - Exposes a Read + Seek interface
 - Keeps a small memory cache or download the whole resource to disk
 - Switch between the in memory cache and saving to disk without losing progress or the reader noticing
 - Recovers download progress
 - Limit the bandwidth used
 - Prioritize one stream over another

## License options
This work is available under the Gnu Public License (version 3). For other licenses please contact me.

## Contributing
To remain flexible in licensing I need to retain sole ownership of the source code. Unfortunately that means I can not accept code contributions. 

I would love to get your bug reports and hear about features you would like!
