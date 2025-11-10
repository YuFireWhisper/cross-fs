#[cfg(feature = "direct-io")]
use std::io;

#[cfg(feature = "direct-io")]
use crate::{ALIGN, File, avec};

#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(target_os = "windows")]
pub mod windows;

#[cfg(feature = "direct-io")]
fn read_helper<F, T>(file: &File, buf: &mut [u8], other: T, f: F) -> io::Result<usize>
where
    F: Fn(&std::fs::File, &mut [u8], T) -> io::Result<usize>,
{
    if (buf.as_ptr() as usize).is_multiple_of(ALIGN) {
        return f(&file.inner, buf, other);
    }

    let aligned_len = buf.len().next_multiple_of(ALIGN);

    if aligned_len > file.direct_io_buffer_size {
        let mut dbuf = avec!(aligned_len);
        let n = f(&file.inner, &mut dbuf, other)?.min(buf.len());
        buf[..n].copy_from_slice(&dbuf[..n]);
        Ok(n)
    } else {
        let mut dbuf = file.direct_io_buffer.write();
        let n = f(&file.inner, &mut dbuf[..aligned_len], other)?.min(buf.len());
        buf[..n].copy_from_slice(&dbuf[..n]);
        Ok(n)
    }
}

#[cfg(feature = "direct-io")]
fn write_helper<F, T, R>(file: &File, buf: &[u8], other: T, f: F) -> io::Result<R>
where
    F: Fn(&std::fs::File, &[u8], T) -> io::Result<R>,
{
    let buf_len = buf.len();

    if !buf_len.is_multiple_of(ALIGN) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Buffer length must be a multiple of {}, got {}",
                ALIGN, buf_len
            ),
        ));
    }

    if (buf.as_ptr() as usize).is_multiple_of(ALIGN) {
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
