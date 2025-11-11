#[cfg(feature = "direct-io")]
use std::os::unix::io::AsRawFd;
use std::{
    io::{self, Read, Write},
    os::unix,
};

#[cfg(feature = "direct-io")]
use crate::{
    ALIGN, avec,
    file::impls::{read_helper, write_helper},
};
use crate::{
    File,
    file::{PositionedExt, VectoredExt},
};

fn tmp_bufs_into_bufs(n: usize, bufs: &mut [io::IoSliceMut<'_>], tmp_bufs: Vec<Option<Vec<u8>>>) {
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
}

impl Read for &File {
    #[cfg(feature = "direct-io")]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        read_helper(self, buf, (), |mut f, b, _| f.read(b))
    }

    #[cfg(not(feature = "direct-io"))]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&self.inner).read(buf)
    }

    #[cfg(feature = "direct-io")]
    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
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

        let n = unsafe {
            libc::readv(
                self.inner.as_raw_fd(),
                iovs.as_ptr(),
                bufs_len as libc::c_int,
            )
        };

        match n {
            n if n < 0 => Err(io::Error::last_os_error()),
            0 => Ok(0),
            n => {
                let n = n as usize;
                tmp_bufs_into_bufs(n, bufs, tmp_bufs);
                Ok(n)
            }
        }
    }

    #[cfg(not(feature = "direct-io"))]
    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        (&self.inner).read_vectored(bufs)
    }

    fn is_read_vectored(&self) -> bool {
        true
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
    #[cfg(feature = "direct-io")]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        write_helper(self, buf, (), |mut f, b, _| f.write(b))
    }

    #[cfg(not(feature = "direct-io"))]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&self.inner).write(buf)
    }

    #[cfg(feature = "direct-io")]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let bufs_len = bufs.len();
        let mut iovs = Vec::with_capacity(bufs_len);
        let mut tmp_bufs = Vec::with_capacity(bufs_len);

        for buf in bufs.iter() {
            let buf_addr = buf.as_ptr() as usize;
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

        let n = unsafe {
            libc::writev(
                self.inner.as_raw_fd(),
                iovs.as_ptr(),
                bufs_len as libc::c_int,
            )
        };

        if n < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(n as usize)
    }

    #[cfg(not(feature = "direct-io"))]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        (&self.inner).write_vectored(bufs)
    }

    fn is_write_vectored(&self) -> bool {
        true
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
    #[cfg(feature = "direct-io")]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        read_helper(self, buf, offset, |f, b, o| {
            unix::fs::FileExt::read_at(f, b, o)
        })
    }

    #[cfg(not(feature = "direct-io"))]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        unix::fs::FileExt::read_at(&self.inner, buf, offset)
    }

    #[cfg(feature = "direct-io")]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        write_helper(self, buf, offset, |f, b, o| {
            unix::fs::FileExt::write_at(f, b, o)
        })
    }

    #[cfg(not(feature = "direct-io"))]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        unix::fs::FileExt::write_at(&self.inner, buf, offset)
    }
}

impl VectoredExt for File {
    #[cfg(feature = "direct-io")]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
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

        let n = unsafe {
            libc::preadv(
                self.inner.as_raw_fd(),
                iovs.as_ptr(),
                bufs_len as libc::c_int,
                offset as libc::off_t,
            )
        };

        match n {
            n if n < 0 => Err(io::Error::last_os_error()),
            0 => Ok(0),
            n => {
                let n = n as usize;
                tmp_bufs_into_bufs(n, bufs, tmp_bufs);
                Ok(n)
            }
        }
    }

    #[cfg(not(feature = "direct-io"))]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        unix::fs::FileExt::read_vectored_at(&self.inner, bufs, offset)
    }

    #[cfg(feature = "direct-io")]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        let bufs_len = bufs.len();
        let mut iovs = Vec::with_capacity(bufs_len);
        let mut tmp_bufs = Vec::with_capacity(bufs_len);

        for buf in bufs.iter() {
            let buf_addr = buf.as_ptr() as usize;
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

        let n = unsafe {
            libc::pwritev(
                self.inner.as_raw_fd(),
                iovs.as_ptr(),
                bufs_len as libc::c_int,
                offset as libc::off_t,
            )
        };

        if n < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(n as usize)
    }

    #[cfg(not(feature = "direct-io"))]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        unix::fs::FileExt::write_vectored_at(&self.inner, bufs, offset)
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
