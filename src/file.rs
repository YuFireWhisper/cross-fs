use std::{
    fmt::Debug,
    fs::FileTimes,
    io::{self, Seek},
    path::Path,
    process::Stdio,
    time::SystemTime,
};

#[cfg(feature = "direct-io")]
use parking_lot::RwLock;

use crate::open_options::OpenOptions;
#[cfg(feature = "direct-io")]
use crate::{ALIGN, LENGTH_NON_ALIGNED_ERROR, avec};

pub mod impls;

pub trait PositionedExt {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize>;
    fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match self.write_at(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

pub trait VectoredExt {
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize>;
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize>;
}

pub struct File {
    pub(crate) inner: std::fs::File,

    #[cfg(feature = "direct-io")]
    pub(crate) direct_io_buffer: RwLock<Vec<u8>>,
    #[cfg(feature = "direct-io")]
    pub(crate) direct_io_buffer_size: usize,
}

impl File {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path)
    }

    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        OpenOptions::new().write(true).create(true).open(path)
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        OpenOptions::new().write(true).create_new(true).open(path)
    }

    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    pub fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()
    }

    pub fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }

    pub fn lock(&self) -> io::Result<()> {
        self.inner.lock()
    }

    pub fn lock_shared(&self) -> io::Result<()> {
        self.inner.lock_shared()
    }

    pub fn try_lock(&self) -> Result<(), std::fs::TryLockError> {
        self.inner.try_lock()
    }

    pub fn try_lock_shared(&self) -> Result<(), std::fs::TryLockError> {
        self.inner.try_lock_shared()
    }

    pub fn unlock(&self) -> io::Result<()> {
        self.inner.unlock()
    }

    pub fn set_len(&self, size: u64) -> io::Result<()> {
        self.inner.set_len(size)
    }

    pub fn metadata(&self) -> io::Result<std::fs::Metadata> {
        self.inner.metadata()
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            inner: self.inner.try_clone()?,
            #[cfg(feature = "direct-io")]
            direct_io_buffer: RwLock::new(vec![0; self.direct_io_buffer.read().len()]),
            #[cfg(feature = "direct-io")]
            direct_io_buffer_size: self.direct_io_buffer_size,
        })
    }

    pub fn set_permissions(&self, perm: std::fs::Permissions) -> io::Result<()> {
        self.inner.set_permissions(perm)
    }

    pub fn set_times(&self, times: FileTimes) -> io::Result<()> {
        self.inner.set_times(times)
    }

    pub fn set_modified(&self, modified: SystemTime) -> io::Result<()> {
        self.inner.set_modified(modified)
    }

    pub fn as_std(&self) -> &std::fs::File {
        &self.inner
    }

    #[cfg(feature = "direct-io")]
    pub(crate) fn read_helper<F>(&self, buf: &mut [u8], offset: u64, f: F) -> io::Result<usize>
    where
        F: Fn(&std::fs::File, &mut [u8], u64) -> io::Result<usize>,
    {
        let buf_ptr = buf.as_ptr() as usize;
        let buf_len = buf.len();
        let aligned_len = buf_len.next_multiple_of(ALIGN);

        // If direct I/O is disabled or the buffer is already aligned,
        // use it directly.
        if self.direct_io_buffer_size == 0
            || (buf_ptr.is_multiple_of(ALIGN) && buf_len == aligned_len)
        {
            return f(&self.inner, buf, offset);
        }

        if aligned_len > self.direct_io_buffer_size {
            let mut dbuf = avec!(aligned_len);
            let n = f(&self.inner, &mut dbuf, offset)?.min(buf_len);
            buf[..n].copy_from_slice(&dbuf[..n]);
            Ok(n)
        } else {
            let mut dbuf = self.direct_io_buffer.write();
            let n = f(&self.inner, &mut dbuf[..aligned_len], offset)?.min(buf_len);
            buf[..n].copy_from_slice(&dbuf[..n]);
            Ok(n)
        }
    }

    #[cfg(not(feature = "direct-io"))]
    fn read_helper<F>(&self, buf: &mut [u8], offset: u64, f: F) -> io::Result<usize>
    where
        F: Fn(&std::fs::File, &mut [u8], u64) -> io::Result<usize>,
    {
        f(&self.inner, buf, offset)
    }

    #[cfg(feature = "direct-io")]
    fn write_helper<F, R>(&self, buf: &[u8], offset: u64, f: F) -> io::Result<R>
    where
        F: Fn(&std::fs::File, &[u8], u64) -> io::Result<R>,
    {
        if self.direct_io_buffer_size == 0 {
            return f(&self.inner, buf, offset);
        }

        let buf_ptr = buf.as_ptr() as usize;
        let buf_len = buf.len();

        if !buf_len.is_multiple_of(ALIGN) {
            return Err(LENGTH_NON_ALIGNED_ERROR);
        }

        // Already aligned
        if buf_ptr.is_multiple_of(ALIGN) {
            return f(&self.inner, buf, offset);
        }

        if buf_len > self.direct_io_buffer_size {
            let mut dbuf = avec!(buf_len);
            dbuf[..buf_len].copy_from_slice(buf);
            f(&self.inner, &dbuf[..buf_len], offset)
        } else {
            let mut dbuf = self.direct_io_buffer.write();
            dbuf[..buf_len].copy_from_slice(buf);
            f(&self.inner, &dbuf[..buf_len], offset)
        }
    }

    #[cfg(not(feature = "direct-io"))]
    fn write_helper<F, R>(&self, buf: &[u8], offset: u64, f: F) -> io::Result<R>
    where
        F: Fn(&std::fs::File, &[u8], u64) -> io::Result<R>,
    {
        f(&self.inner, buf, offset)
    }
}

impl Debug for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl Seek for &File {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        (&self.inner).seek(pos)
    }
}

impl Seek for File {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl From<File> for std::fs::File {
    fn from(file: File) -> Self {
        file.inner
    }
}

impl From<File> for Stdio {
    fn from(file: File) -> Self {
        file.inner.into()
    }
}
