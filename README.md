# cross-fs

This project provides cross-platform extensions for `std::fs::File`, avoiding the hassle of conditional compilation when writing code for different operating systems. We also provide support for Direct I/O.

## Features

- **Direct I/O**: Implements Direct I/O on supported platforms. You can create aligned buffers using `cross-fs::avec!(n)`.
- **Positioned I/O**: Defines a unified interface through the `PositionedExt` trait.
- **Vectored I/O**: Defined through the `VectoredExt` trait, with implementation for Windows code when Direct I/O is enabled (since Vectored I/O is only available on Windows when Direct I/O is enabled, the standard library doesn't implement it by default, and this project fills that gap).

## Supported Operating Systems

|  | Direct I/O | Positioned I/O | Vectored I/O |
| --- | --- | --- | --- |
| Linux | Full | Full | Full |
| Windows | Full | Full | Must enable Direct I/O |

> Since I develop on WSL, I can only design code for Linux and Windows. If you're willing to help add support for other platforms, PRs are very welcome!

## Feature Flags

- align-512: Changes memory alignment size to 512 (default is 4096). In most cases, you shouldn't need to enable this feature.

## Contributing

Suggestions for improvements or Pull Requests are welcome. If you think the crate should add new features or if there are any bugs that need fixing, please let me know through an issue, and I'll do my best to fix and implement them.

## License

This project is licensed under either of:

- [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) ([LICENSE-APACHE](./LICENSE-APACHE))
- [MIT License](https://opensource.org/licenses/MIT) ([LICENSE-MIT](./LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
