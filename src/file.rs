use std::{
    fmt,
    fs::FileTimes,
    io::{self, Read, Seek, Write},
    os::{
        fd::{AsFd, AsRawFd, IntoRawFd, OwnedFd},
        unix::fs::FileExt as UnixFileExt,
        windows::fs::FileExt as WindowsFileExt,
    },
    path::Path,
    process::Stdio,
    time::SystemTime,
};

use parking_lot::RwLock;

use crate::{alloc_aligend_buffer, open_options::OpenOptions};

pub struct File {
    pub(crate) inner: std::fs::File,
    pub(crate) direct_io_buffer: RwLock<Vec<u8>>,
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
            direct_io_buffer: RwLock::new(vec![0; self.direct_io_buffer.read().len()]),
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
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl Read for &File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let dbuf = self.direct_io_buffer.read();

        if dbuf.is_empty() {
            return (&self.inner).read(buf);
        }

        if buf.len() > dbuf.len() {
            let mut direct_io_buffer = alloc_aligend_buffer(buf.len());
            let n = (&self.inner).read(&mut direct_io_buffer[..buf.len()])?;
            buf[..n].copy_from_slice(&direct_io_buffer[..n]);
            return Ok(n);
        }

        let mut dbuf = self.direct_io_buffer.write();

        let n = (&self.inner).read(&mut dbuf[..buf.len()])?;
        buf[..n].copy_from_slice(&dbuf[..n]);

        Ok(n)
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self).read(buf)
    }
}

impl Write for &File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let dbuf = self.direct_io_buffer.read();

        if dbuf.is_empty() {
            return (&self.inner).write(buf);
        }

        if buf.len() > dbuf.len() {
            let mut direct_io_buffer = alloc_aligend_buffer(buf.len());
            direct_io_buffer[..buf.len()].copy_from_slice(buf);
            return (&self.inner).write(&direct_io_buffer[..buf.len()]);
        }

        let mut dbuf = self.direct_io_buffer.write();

        dbuf[..buf.len()].copy_from_slice(buf);
        (&self.inner).write(&dbuf[..buf.len()])
    }

    fn flush(&mut self) -> io::Result<()> {
        (&self.inner).flush()
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self).flush()
    }
}

impl Seek for &File {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        (&self.inner).seek(pos)
    }
}

impl Seek for File {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        (&*self).seek(pos)
    }
}

impl AsFd for File {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl AsRawFd for File {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.inner.as_raw_fd()
    }
}

impl From<File> for std::fs::File {
    fn from(file: File) -> Self {
        file.inner
    }
}

impl From<File> for OwnedFd {
    fn from(file: File) -> Self {
        file.inner.into()
    }
}

impl From<File> for Stdio {
    fn from(file: File) -> Self {
        file.inner.into()
    }
}

impl IntoRawFd for File {
    fn into_raw_fd(self) -> std::os::fd::RawFd {
        self.inner.into_raw_fd()
    }
}

#[cfg(target_os = "linux")]
impl UnixFileExt for File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let dbuf = self.direct_io_buffer.read();

        if dbuf.is_empty() {
            return self.inner.read_at(buf, offset);
        }

        if buf.len() > dbuf.len() {
            let mut direct_io_buffer = alloc_aligend_buffer(buf.len());
            let n = self
                .inner
                .read_at(&mut direct_io_buffer[..buf.len()], offset)?;
            buf[..n].copy_from_slice(&direct_io_buffer[..n]);
            return Ok(n);
        }

        let mut dbuf = self.direct_io_buffer.write();
        let n = self.inner.read_at(&mut dbuf[..buf.len()], offset)?;
        buf[..n].copy_from_slice(&dbuf[..n]);

        Ok(n)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        let dbuf = self.direct_io_buffer.read();

        if dbuf.is_empty() {
            return self.inner.write_at(buf, offset);
        }

        if buf.len() > dbuf.len() {
            let mut direct_io_buffer = alloc_aligend_buffer(buf.len());
            direct_io_buffer[..buf.len()].copy_from_slice(buf);
            return self.inner.write_at(&direct_io_buffer[..buf.len()], offset);
        }

        let mut dbuf = self.direct_io_buffer.write();
        dbuf[..buf.len()].copy_from_slice(buf);
        self.inner.write_at(&dbuf[..buf.len()], offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let dbuf = self.direct_io_buffer.read();

        if dbuf.is_empty() {
            return self.inner.write_all_at(buf, offset);
        }

        if buf.len() > dbuf.len() {
            let mut direct_io_buffer = alloc_aligend_buffer(buf.len());
            direct_io_buffer[..buf.len()].copy_from_slice(buf);
            return self
                .inner
                .write_all_at(&direct_io_buffer[..buf.len()], offset);
        }

        let mut dbuf = self.direct_io_buffer.write();
        dbuf[..buf.len()].copy_from_slice(buf);
        self.inner.write_all_at(&dbuf[..buf.len()], offset)
    }
}

#[cfg(windows)]
impl WindowsFileExt for File {
    fn seek_read(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let dbuf = self.direct_io_buffer.read();

        if dbuf.is_empty() {
            return self.inner.seek_read(buf, offset);
        }

        if buf.len() > dbuf.len() {
            let mut direct_io_buffer = alloc_aligend_buffer(buf.len());
            let n = self
                .inner
                .seek_read(&mut direct_io_buffer[..buf.len()], offset)?;
            buf[..n].copy_from_slice(&direct_io_buffer[..n]);
            return Ok(n);
        }

        let mut dbuf = self.direct_io_buffer.write();
        let n = self.inner.seek_read(&mut dbuf[..buf.len()], offset)?;
        buf[..n].copy_from_slice(&dbuf[..n]);

        Ok(n)
    }

    fn seek_write(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        let dbuf = self.direct_io_buffer.read();

        if dbuf.is_empty() {
            return self.inner.seek_write(buf, offset);
        }

        if buf.len() > dbuf.len() {
            let mut direct_io_buffer = alloc_aligend_buffer(buf.len());
            direct_io_buffer[..buf.len()].copy_from_slice(buf);
            return self
                .inner
                .seek_write(&direct_io_buffer[..buf.len()], offset);
        }

        let mut dbuf = self.direct_io_buffer.write();
        dbuf[..buf.len()].copy_from_slice(buf);
        self.inner.seek_write(&dbuf[..buf.len()], offset)
    }
}
