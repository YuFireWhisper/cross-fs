#[cfg(feature = "direct-io")]
use std::os::windows::{self, fs::FileExt as _};
use std::{
    io::{self, Read, Write},
    os::windows::{
        io::{BorrowedHandle, IntoRawHandle, OwnedHandle},
        prelude::{AsHandle, AsRawHandle},
        raw,
    },
};

#[cfg(feature = "direct-io")]
use windows_sys::Win32::{
    Foundation::HANDLE,
    Storage::FileSystem::{FILE_SEGMENT_ELEMENT, ReadFileScatter, WriteFileGather},
    System::IO::{GetOverlappedResult, OVERLAPPED},
};

#[cfg(feature = "direct-io")]
use crate::{ALIGN, LENGTH_NON_ALIGNED_ERROR, avec, file::VectoredExt};
use crate::{File, file::PositionedExt};

#[cfg(feature = "direct-io")]
impl File {
    fn read_vectored_handler(
        &self,
        bufs: &mut [io::IoSliceMut<'_>],
        offset: Option<u64>,
    ) -> io::Result<usize> {
        if self.direct_io_buffer_size == 0 {
            return (&self.inner).read_vectored(bufs);
        }

        let bufs_len = bufs.len();
        let mut segments = Vec::with_capacity(bufs_len + 1);
        let mut tmp_bufs = Vec::with_capacity(bufs_len);
        let mut total_len = 0;

        for buf in bufs.iter_mut() {
            let buf_addr = buf.as_mut_ptr() as usize;
            let buf_len = buf.len();
            let aligned_len = buf_len.next_multiple_of(ALIGN);

            if buf_addr.is_multiple_of(ALIGN) && buf_len == aligned_len {
                segments.push(FILE_SEGMENT_ELEMENT {
                    Buffer: buf.as_mut_ptr() as *mut _,
                });
                tmp_bufs.push(None);
            } else {
                let mut tmp_buf = avec!(aligned_len);
                segments.push(FILE_SEGMENT_ELEMENT {
                    Buffer: tmp_buf.as_mut_ptr() as *mut _,
                });
                tmp_bufs.push(Some(tmp_buf));
            }

            total_len += aligned_len;
        }

        segments.push(unsafe { std::mem::zeroed() });

        let raw_handle: HANDLE = self.inner.as_raw_handle() as _;
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        if let Some(off) = offset {
            overlapped.Anonymous.Anonymous.Offset = (off & 0xFFFFFFFF) as u32;
            overlapped.Anonymous.Anonymous.OffsetHigh = (off >> 32) as u32;
        }

        if unsafe {
            ReadFileScatter(
                raw_handle,
                segments.as_ptr(),
                total_len as u32,
                std::ptr::null_mut(),
                &mut overlapped,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }

        let mut bytes_read: u32 = 0;

        if unsafe { GetOverlappedResult(raw_handle, &overlapped, &mut bytes_read, 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        if bytes_read == 0 {
            return Ok(0);
        }

        let bytes_read = bytes_read as usize;
        let mut remaining = bytes_read;

        for (buf, tmp_buf) in bufs.iter_mut().zip(tmp_bufs) {
            let buf_len = buf.len();
            let wrote = remaining.min(buf_len);

            if let Some(tmp_buf) = tmp_buf {
                buf[..wrote].copy_from_slice(&tmp_buf[..wrote]);
            }

            remaining -= wrote;

            if remaining == 0 {
                break;
            }
        }

        Ok(bytes_read)
    }

    fn write_vectored_handler(
        &self,
        bufs: &[io::IoSlice<'_>],
        offset: Option<u64>,
    ) -> io::Result<usize> {
        if self.direct_io_buffer_size == 0 {
            return (&self.inner).write_vectored(bufs);
        }

        let bufs_len = bufs.len();
        let mut segments: Vec<FILE_SEGMENT_ELEMENT> = Vec::with_capacity(bufs_len + 1);
        let mut tmp_bufs = Vec::with_capacity(bufs_len);
        let mut total_len = 0;

        for buf in bufs {
            let buf_addr = buf.as_ptr() as usize;
            let buf_len = buf.len();

            if !buf_len.is_multiple_of(ALIGN) {
                return Err(LENGTH_NON_ALIGNED_ERROR);
            }

            if buf_addr.is_multiple_of(ALIGN) {
                segments.push(FILE_SEGMENT_ELEMENT {
                    Buffer: buf.as_ptr() as *mut _,
                });
                tmp_bufs.push(None);
            } else {
                let mut tmp_buf = avec!(buf_len);
                tmp_buf[..buf_len].copy_from_slice(buf);
                segments.push(FILE_SEGMENT_ELEMENT {
                    Buffer: tmp_buf.as_mut_ptr() as *mut _,
                });
                tmp_bufs.push(Some(tmp_buf));
            }

            total_len += buf_len;
        }

        segments.push(unsafe { std::mem::zeroed() });

        let raw_handle: HANDLE = self.inner.as_raw_handle() as _;
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        if let Some(off) = offset {
            overlapped.Anonymous.Anonymous.Offset = (off & 0xFFFFFFFF) as u32;
            overlapped.Anonymous.Anonymous.OffsetHigh = (off >> 32) as u32;
        }

        if unsafe {
            WriteFileGather(
                raw_handle,
                segments.as_ptr(),
                total_len as u32,
                std::ptr::null_mut(),
                &mut overlapped,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }

        let mut bytes_written: u32 = 0;

        if unsafe { GetOverlappedResult(raw_handle, &overlapped, &mut bytes_written, 1) } == 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(bytes_written as usize)
    }
}

impl AsHandle for File {
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.inner.as_handle()
    }
}

impl AsRawHandle for File {
    fn as_raw_handle(&self) -> raw::HANDLE {
        self.inner.as_raw_handle()
    }
}

impl From<File> for OwnedHandle {
    fn from(file: File) -> Self {
        file.inner.into()
    }
}

impl IntoRawHandle for File {
    fn into_raw_handle(self) -> raw::HANDLE {
        self.inner.into_raw_handle()
    }
}

impl Read for &File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_helper(buf, 0, |mut f, b, _| f.read(b))
    }

    #[cfg(feature = "direct-io")]
    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.read_vectored_handler(bufs, None)
    }

    #[cfg(not(feature = "direct-io"))]
    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> io::Result<usize> {
        (&self.inner).read_vectored(bufs)
    }

    #[cfg(feature = "direct-io")]
    #[inline]
    fn is_read_vectored(&self) -> bool {
        if self.direct_io_buffer_size == 0 {
            return self.inner.is_read_vectored();
        }
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

    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> io::Result<usize> {
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
    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> io::Result<usize> {
        self.write_vectored_handler(bufs, None)
    }

    #[cfg(not(feature = "direct-io"))]
    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> io::Result<usize> {
        (&self.inner).write_vectored(bufs)
    }

    #[cfg(feature = "direct-io")]
    #[inline]
    fn is_write_vectored(&self) -> bool {
        if self.direct_io_buffer_size == 0 {
            return self.inner.is_write_vectored();
        }
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

    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> io::Result<usize> {
        (&*self).write_vectored(bufs)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self).flush()
    }
}

impl PositionedExt for File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.read_helper(buf, offset, |f, b, o| f.seek_read(b, o))
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        self.write_helper(buf, offset, |f, b, o| f.seek_write(b, o))
    }
}

// When performing Vectored I/O on Windows, Direct I/O must be enabled.
// Therefore, we only implement this trait when the direct-io feature is enabled.
// Windows doesn't have a built-in vectored I/O API (like Linux's readv/writev).
#[cfg(feature = "direct-io")]
impl VectoredExt for File {
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        if self.direct_io_buffer_size == 0 {
            return Err(io::Error::other(
                "Vectored I/O requires Direct I/O to be enabled",
            ));
        }
        self.read_vectored_handler(bufs, Some(offset))
    }

    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        if self.direct_io_buffer_size == 0 {
            return Err(io::Error::other(
                "Vectored I/O requires Direct I/O to be enabled",
            ));
        }
        self.write_vectored_handler(bufs, Some(offset))
    }
}

impl windows::fs::FileExt for File {
    fn seek_read(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        PositionedExt::read_at(self, buf, offset)
    }

    fn seek_write(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        PositionedExt::write_at(self, buf, offset)
    }
}
