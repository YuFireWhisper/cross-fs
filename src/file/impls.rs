use std::io;

#[cfg(feature = "direct-io")]
use crate::{ALIGN, File, avec};

#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(target_os = "windows")]
pub mod windows;

const LENGTH_NON_ALIGNED_ERROR: io::Error = io::const_error!(
    io::ErrorKind::InvalidInput,
    "Buffer length must be a multiple of ALIGN"
);

fn read_helper<F, T>(file: &File, buf: &mut [u8], other: T, f: F) -> io::Result<usize>
where
    F: Fn(&std::fs::File, &mut [u8], T) -> io::Result<usize>,
{
    #[cfg(feature = "direct-io")]
    {
        let buf_ptr = buf.as_ptr() as usize;
        let buf_len = buf.len();
        let aligned_len = buf_len.next_multiple_of(ALIGN);

        // If direct I/O is disabled or the buffer is already aligned,
        // use it directly.
        if file.direct_io_buffer_size == 0
            || (buf_ptr.is_multiple_of(ALIGN) && buf_len == aligned_len)
        {
            return f(&file.inner, buf, other);
        }

        if aligned_len > file.direct_io_buffer_size {
            let mut dbuf = avec!(aligned_len);
            let n = f(&file.inner, &mut dbuf, other)?.min(buf_len);
            buf[..n].copy_from_slice(&dbuf[..n]);
            Ok(n)
        } else {
            let mut dbuf = file.direct_io_buffer.write();
            let n = f(&file.inner, &mut dbuf[..aligned_len], other)?.min(buf_len);
            buf[..n].copy_from_slice(&dbuf[..n]);
            Ok(n)
        }
    }

    #[cfg(not(feature = "direct-io"))]
    {
        f(&file.inner, buf, other)
    }
}

fn write_helper<F, T, R>(file: &File, buf: &[u8], other: T, f: F) -> io::Result<R>
where
    F: Fn(&std::fs::File, &[u8], T) -> io::Result<R>,
{
    #[cfg(feature = "direct-io")]
    {
        if file.direct_io_buffer_size == 0 {
            return f(&file.inner, buf, other);
        }

        let buf_ptr = buf.as_ptr() as usize;
        let buf_len = buf.len();

        if !buf_len.is_multiple_of(ALIGN) {
            return Err(LENGTH_NON_ALIGNED_ERROR);
        }

        // Already aligned
        if buf_ptr.is_multiple_of(ALIGN) {
            return f(&file.inner, buf, other);
        }

        if buf_len > file.direct_io_buffer_size {
            let mut dbuf = avec!(buf_len);
            dbuf[..buf_len].copy_from_slice(buf);
            f(&file.inner, &dbuf[..buf_len], other)
        } else {
            let mut dbuf = file.direct_io_buffer.write();
            dbuf[..buf_len].copy_from_slice(buf);
            f(&file.inner, &dbuf[..buf_len], other)
        }
    }

    #[cfg(not(feature = "direct-io"))]
    {
        f(&file.inner, buf, other)
    }
}
