use std::{fmt, fs::FileTimes, io, path::Path, time::SystemTime};

use crate::open_options::OpenOptions;

pub struct File {
    pub(crate) inner: std::fs::File,
    pub(crate) direct_io: bool,
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
            direct_io: self.direct_io,
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
