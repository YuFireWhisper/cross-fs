use std::{
    fmt,
    fs::FileTimes,
    io::{self, Read},
    path::Path,
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
