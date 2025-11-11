#[cfg(feature = "direct-io")]
use std::os::{fd::RawFd, unix::io::AsRawFd};
use std::{
    io::{self, Read, Write},
    os::unix::{self, fs::FileExt as _},
};

#[cfg(feature = "direct-io")]
use crate::{ALIGN, LENGTH_NON_ALIGNED_ERROR, avec};
use crate::{File, PositionedExt, VectoredExt};

#[cfg(feature = "direct-io")]
fn read_vectored_handler<F>(
    file: &File,
    bufs: &mut [io::IoSliceMut<'_>],
    offset: u64,
    read_fn: F,
) -> io::Result<usize>
where
    F: Fn(RawFd, *const libc::iovec, libc::c_int, libc::off_t) -> libc::ssize_t,
{
    let bufs_len = bufs.len();
    let mut iovs = Vec::with_capacity(bufs_len);
    let mut tmp_bufs = Vec::with_capacity(bufs_len);

    for buf in bufs.iter_mut() {
        let buf_addr = buf.as_mut_ptr() as usize;
        let buf_len = buf.len();
        let aligned_len = buf_len.next_multiple_of(ALIGN);

        if buf_addr.is_multiple_of(ALIGN) && buf_len == aligned_len {
            iovs.push(libc::iovec {
                iov_base: buf.as_mut_ptr() as *mut _,
                iov_len: aligned_len,
            });
            tmp_bufs.push(None);
        } else {
            let mut tmp_buf = avec!(aligned_len);
            iovs.push(libc::iovec {
                iov_base: tmp_buf.as_mut_ptr() as *mut _,
                iov_len: aligned_len,
            });
            tmp_bufs.push(Some(tmp_buf));
        }
    }

    match read_fn(
        file.inner.as_raw_fd(),
        iovs.as_ptr(),
        bufs_len as _,
        offset as _,
    ) {
        n if n < 0 => Err(io::Error::last_os_error()),
        0 => Ok(0),
        n => {
            let n = n as usize;
            let mut remaining = n;
            for (buf, tmp_buf) in bufs.iter_mut().zip(tmp_bufs) {
                let len = buf.len();
                let read = remaining.min(len);

                if let Some(tmp_buf) = tmp_buf {
                    buf.copy_from_slice(&tmp_buf[..read]);
                }

                remaining -= read;

                if remaining == 0 {
                    break;
                }
            }
            Ok(n)
        }
    }
}

#[cfg(feature = "direct-io")]
fn write_vectored_handler<F>(
    file: &File,
    bufs: &[io::IoSlice<'_>],
    offset: u64,
    write_fn: F,
) -> io::Result<usize>
where
    F: Fn(RawFd, *const libc::iovec, libc::c_int, libc::off_t) -> libc::ssize_t,
{
    let bufs_len = bufs.len();
    let mut iovs = Vec::with_capacity(bufs_len);
    let mut tmp_bufs = Vec::with_capacity(bufs_len);

    for buf in bufs.iter() {
        let buf_addr = buf.as_ptr() as usize;
        let buf_len = buf.len();

        if !buf_len.is_multiple_of(ALIGN) {
            return Err(LENGTH_NON_ALIGNED_ERROR);
        }

        if buf_addr.is_multiple_of(ALIGN) {
            iovs.push(libc::iovec {
                iov_base: buf.as_ptr() as *mut _,
                iov_len: buf_len,
            });
            tmp_bufs.push(None);
        } else {
            let mut tmp_buf = avec!(buf_len);
            tmp_buf[..buf_len].copy_from_slice(buf);
            iovs.push(libc::iovec {
                iov_base: tmp_buf.as_mut_ptr() as *mut _,
                iov_len: buf_len,
            });
            tmp_bufs.push(Some(tmp_buf));
        }
    }

    match write_fn(
        file.inner.as_raw_fd(),
        iovs.as_ptr(),
        bufs_len as _,
        offset as _,
    ) {
        n if n < 0 => Err(io::Error::last_os_error()),
        n => Ok(n as usize),
    }
}

impl Read for &File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_helper(buf, 0, |mut f, b, _| f.read(b))
    }

    #[cfg(feature = "direct-io")]
    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        read_vectored_handler(self, bufs, 0, |fd, ptr, len, _| unsafe {
            libc::readv(fd, ptr, len)
        })
    }

    #[cfg(not(feature = "direct-io"))]
    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        (&self.inner).read_vectored(bufs)
    }

    #[cfg(feature = "direct-io")]
    #[inline]
    fn is_read_vectored(&self) -> bool {
        true
    }

    #[cfg(not(feature = "direct-io"))]
    fn is_read_vectored(&self) -> bool {
        (&self.inner).is_read_vectored()
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self).read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        (&*self).read_vectored(bufs)
    }

    fn is_read_vectored(&self) -> bool {
        (&self).is_read_vectored()
    }
}

impl Write for &File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_helper(buf, 0, |mut f, b, _| f.write(b))
    }

    #[cfg(feature = "direct-io")]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        write_vectored_handler(self, bufs, 0, |fd, ptr, len, _| unsafe {
            libc::writev(fd, ptr, len)
        })
    }

    #[cfg(not(feature = "direct-io"))]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        (&self.inner).write_vectored(bufs)
    }

    #[cfg(feature = "direct-io")]
    #[inline]
    fn is_write_vectored(&self) -> bool {
        true
    }

    #[cfg(not(feature = "direct-io"))]
    fn is_write_vectored(&self) -> bool {
        (&self.inner).is_write_vectored()
    }

    fn flush(&mut self) -> io::Result<()> {
        (&self.inner).flush()
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self).write(buf)
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        (&*self).write_vectored(bufs)
    }

    fn is_write_vectored(&self) -> bool {
        (&self).is_write_vectored()
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self).flush()
    }
}

impl PositionedExt for File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.read_helper(buf, offset, |f, b, o| f.read_at(b, o))
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        self.write_helper(buf, offset, |f, b, o| f.write_at(b, o))
    }
}

impl VectoredExt for File {
    #[cfg(feature = "direct-io")]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        read_vectored_handler(self, bufs, offset, |fd, ptr, len, off| unsafe {
            libc::preadv(fd, ptr, len, off)
        })
    }

    #[cfg(not(feature = "direct-io"))]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        (&self.inner).read_vectored_at(bufs, offset)
    }

    #[cfg(feature = "direct-io")]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        write_vectored_handler(self, bufs, offset, |fd, ptr, len, off| unsafe {
            libc::pwritev(fd, ptr, len, off)
        })
    }

    #[cfg(not(feature = "direct-io"))]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        (&self.inner).write_vectored_at(bufs, offset)
    }
}

impl unix::fs::FileExt for File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        PositionedExt::read_at(self, buf, offset)
    }

    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        VectoredExt::read_vectored_at(self, bufs, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        PositionedExt::write_at(self, buf, offset)
    }

    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        VectoredExt::write_vectored_at(self, bufs, offset)
    }
}
