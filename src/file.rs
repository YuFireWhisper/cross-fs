use std::{
    fmt,
    fs::FileTimes,
    io::{self, Read, Seek, Write},
    path::Path,
    process::Stdio,
    time::SystemTime,
};

use parking_lot::RwLock;

use crate::{ALIGN, avec, open_options::OpenOptions};

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
}

fn read<F, T>(file: &File, buf: &mut [u8], other: T, f: F) -> io::Result<usize>
where
    F: Fn(&std::fs::File, &mut [u8], T) -> io::Result<usize>,
{
    #[cfg(feature = "direct-io")]
    {
        if (buf.as_ptr() as usize).is_multiple_of(ALIGN) {
            return f(&file.inner, buf, other);
        }

        let len = if buf.len().is_multiple_of(ALIGN) {
            buf.len()
        } else {
            buf.len().next_multiple_of(ALIGN)
        };

        if len > file.direct_io_buffer_size {
            let mut dbuf = avec!(len);
            let n = f(&file.inner, &mut dbuf, other)?.min(buf.len());
            buf[..n].copy_from_slice(&dbuf[..n]);
            Ok(n)
        } else {
            let mut dbuf = file.direct_io_buffer.write();
            let n = f(&file.inner, &mut dbuf[..len], other)?.min(buf.len());
            buf[..n].copy_from_slice(&dbuf[..n]);
            Ok(n)
        }
    }

    #[cfg(not(feature = "direct-io"))]
    {
        f(&file.inner, buf, other)
    }
}

fn write<F, T, R>(file: &File, buf: &[u8], other: T, f: F) -> io::Result<R>
where
    F: Fn(&std::fs::File, &[u8], T) -> io::Result<R>,
{
    #[cfg(feature = "direct-io")]
    {
        assert!(
            buf.len().is_multiple_of(ALIGN),
            "Buffer length must be a multiple of ALIGN"
        );

        if (buf.as_ptr() as usize).is_multiple_of(ALIGN) {
            return f(&file.inner, buf, other);
        }

        if buf.len() > file.direct_io_buffer_size {
            let mut dbuf = avec!(buf.len());
            dbuf[..buf.len()].copy_from_slice(buf);
            f(&file.inner, &dbuf[..buf.len()], other)
        } else {
            let mut dbuf = file.direct_io_buffer.write();
            dbuf[..buf.len()].copy_from_slice(buf);
            f(&file.inner, &dbuf[..buf.len()], other)
        }
    }

    #[cfg(not(feature = "direct-io"))]
    {
        f(&file.inner, buf, other)
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl Read for &File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        read(self, buf, (), |mut file, buf, _| file.read(buf))
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self).read(buf)
    }
}

impl Write for &File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        write(self, buf, (), |mut file, buf, _| file.write(buf))
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

#[cfg(unix)]
pub mod impl_unix {
    use std::{
        io,
        os::{
            fd::{AsFd, AsRawFd, BorrowedFd, IntoRawFd, OwnedFd, RawFd},
            unix::fs::FileExt,
        },
    };

    use crate::file::{File, read, write};

    impl AsFd for File {
        fn as_fd(&self) -> BorrowedFd<'_> {
            self.inner.as_fd()
        }
    }

    impl AsRawFd for File {
        fn as_raw_fd(&self) -> RawFd {
            self.inner.as_raw_fd()
        }
    }

    impl IntoRawFd for File {
        fn into_raw_fd(self) -> RawFd {
            self.inner.into_raw_fd()
        }
    }

    impl From<File> for OwnedFd {
        fn from(file: File) -> Self {
            file.inner.into()
        }
    }

    impl FileExt for File {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            read(self, buf, offset, |file, buf, offset| {
                file.read_at(buf, offset)
            })
        }

        fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
            write(self, buf, offset, |file, buf, offset| {
                file.write_at(buf, offset)
            })
        }

        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            write(self, buf, offset, |file, buf, offset| {
                file.write_all_at(buf, offset)
            })
        }
    }

    impl crate::FileExt for File {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            read(self, buf, offset, |file, buf, offset| {
                file.read_at(buf, offset)
            })
        }

        fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
            write(self, buf, offset, |file, buf, offset| {
                file.write_at(buf, offset)
            })
        }

        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            write(self, buf, offset, |file, buf, offset| {
                file.write_all_at(buf, offset)
            })
        }
    }
}

#[cfg(windows)]
pub mod impl_windows {
    use std::{
        io,
        os::windows::{
            fs::FileExt,
            io::{AsHandle, AsRawHandle, IntoRawHandle, OwnedHandle},
        },
    };

    use crate::file::{File, read, write};

    impl AsHandle for File {
        fn as_handle(&self) -> std::os::windows::io::BorrowedHandle<'_> {
            self.inner.as_handle()
        }
    }

    impl AsRawHandle for File {
        fn as_raw_handle(&self) -> std::os::windows::io::RawHandle {
            self.inner.as_raw_handle()
        }
    }

    impl IntoRawHandle for File {
        fn into_raw_handle(self) -> std::os::windows::io::RawHandle {
            self.inner.into_raw_handle()
        }
    }

    impl From<File> for OwnedHandle {
        fn from(file: File) -> Self {
            file.inner.into()
        }
    }

    impl FileExt for File {
        fn seek_read(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            read(self, buf, offset, |file, buf, offset| {
                file.seek_read(buf, offset)
            })
        }

        fn seek_write(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
            write(self, buf, offset, |file, buf, offset| {
                file.seek_write(buf, offset)
            })
        }
    }

    impl crate::FileExt for File {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            read(self, buf, offset, |file, buf, offset| {
                file.seek_read(buf, offset)
            })
        }

        fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
            write(self, buf, offset, |file, buf, offset| {
                file.seek_write(buf, offset)
            })
        }

        fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
            while !buf.is_empty() {
                match self.write_at(buf, offset)? {
                    0 => {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write whole buffer",
                        ));
                    }
                    n => {
                        buf = &buf[n..];
                        offset += n as u64;
                    }
                }
            }
            Ok(())
        }
    }
}
