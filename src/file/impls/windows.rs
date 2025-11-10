use std::io;
use std::io::{Read, Write};
use std::os::windows;
#[cfg(feature = "direct-io")]
use std::os::windows::prelude::AsRawHandle;

#[cfg(feature = "direct-io")]
use windows_sys::Win32::{
    Foundation::HANDLE,
    Storage::FileSystem::{FILE_SEGMENT_ELEMENT, ReadFileScatter, WriteFileGather},
    System::IO::{GetOverlappedResult, OVERLAPPED},
};

use crate::{File, file::PositionedExt};
#[cfg(feature = "direct-io")]
use crate::{
    avec,
    file::{
        VectoredExt,
        impls::{ALIGN, read_helper, write_helper},
    },
};

impl Read for &File {
    #[cfg(feature = "direct-io")]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        read_helper(self, buf, (), |mut f, b, _| f.read(&mut b[..]))
    }

    #[cfg(not(feature = "direct-io"))]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&self.inner).read(buf)
    }

    #[cfg(feature = "direct-io")]
    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> io::Result<usize> {
        let bufs_len = bufs.len();
        let mut segments: Vec<FILE_SEGMENT_ELEMENT> = Vec::with_capacity(bufs_len + 1);
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

        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        let raw_handle: HANDLE = self.inner.as_raw_handle() as _;

        let result = unsafe {
            ReadFileScatter(
                raw_handle,
                segments.as_ptr(),
                total_len as u32,
                std::ptr::null_mut(),
                &mut overlapped,
            )
        };

        if result == 0 {
            return Err(io::Error::last_os_error());
        }

        let mut bytes_read: u32 = 0;
        let overlapped_result =
            unsafe { GetOverlappedResult(raw_handle, &overlapped, &mut bytes_read, 1) };

        if overlapped_result == 0 {
            return Err(io::Error::last_os_error());
        }

        if bytes_read == 0 {
            return Ok(0);
        }

        let mut remaining = bytes_read as usize;

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

        Ok(bytes_read as usize)
    }

    #[cfg(not(feature = "direct-io"))]
    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> io::Result<usize> {
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

    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> io::Result<usize> {
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
    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> io::Result<usize> {
        let bufs_len = bufs.len();
        let mut segments: Vec<FILE_SEGMENT_ELEMENT> = Vec::with_capacity(bufs_len + 1);
        let mut tmp_bufs = Vec::with_capacity(bufs_len);
        let mut total_len = 0;

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

        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        let raw_handle: HANDLE = self.inner.as_raw_handle() as _;

        let result = unsafe {
            WriteFileGather(
                raw_handle,
                segments.as_ptr(),
                total_len as u32,
                std::ptr::null_mut(),
                &mut overlapped,
            )
        };

        if result == 0 {
            return Err(io::Error::last_os_error());
        }

        let mut bytes_written: u32 = 0;
        let overlapped_result =
            unsafe { GetOverlappedResult(raw_handle, &overlapped, &mut bytes_written, 1) };

        if overlapped_result == 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(bytes_written as usize)
    }

    #[cfg(not(feature = "direct-io"))]
    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> io::Result<usize> {
        (&self.inner).write_vectored(bufs)
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
    #[cfg(feature = "direct-io")]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        read_helper(self, buf, offset, |f, b, o| {
            windows::fs::FileExt::seek_read(f, b, o)
        })
    }

    #[cfg(not(feature = "direct-io"))]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        windows::fs::FileExt::seek_read(&self.inner, buf, offset)
    }

    #[cfg(feature = "direct-io")]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        write_helper(self, buf, offset, |f, b, o| {
            windows::fs::FileExt::seek_write(f, b, o)
        })
    }

    #[cfg(not(feature = "direct-io"))]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        windows::fs::FileExt::seek_write(&self.inner, buf, offset)
    }
}

// When performing Vectored I/O on Windows, Direct I/O must be enabled.
// Therefore, we only implement this trait when the direct-io feature is enabled.
// Windows doesn't have a built-in vectored I/O API (like Linux's readv/writev).
#[cfg(feature = "direct-io")]
impl VectoredExt for File {
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        let bufs_len = bufs.len();
        let mut segments: Vec<FILE_SEGMENT_ELEMENT> = Vec::with_capacity(bufs_len + 1);
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

        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        overlapped.Anonymous.Anonymous.Offset = (offset & 0xFFFFFFFF) as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
        let raw_handle: HANDLE = self.inner.as_raw_handle() as _;

        let result = unsafe {
            ReadFileScatter(
                raw_handle,
                segments.as_ptr(),
                total_len as u32,
                std::ptr::null_mut(),
                &mut overlapped,
            )
        };

        if result == 0 {
            return Err(io::Error::last_os_error());
        }

        let mut bytes_read: u32 = 0;
        let overlapped_result =
            unsafe { GetOverlappedResult(raw_handle, &overlapped, &mut bytes_read, 1) };

        if overlapped_result == 0 {
            return Err(io::Error::last_os_error());
        }

        if bytes_read == 0 {
            return Ok(0);
        }

        let mut remaining = bytes_read as usize;

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

        Ok(bytes_read as usize)
    }

    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        let bufs_len = bufs.len();
        let mut segments: Vec<FILE_SEGMENT_ELEMENT> = Vec::with_capacity(bufs_len + 1);
        let mut tmp_bufs = Vec::with_capacity(bufs_len);
        let mut total_len = 0;

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

        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        overlapped.Anonymous.Anonymous.Offset = (offset & 0xFFFFFFFF) as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
        let raw_handle: HANDLE = self.inner.as_raw_handle() as _;

        let result = unsafe {
            WriteFileGather(
                raw_handle,
                segments.as_ptr(),
                total_len as u32,
                std::ptr::null_mut(),
                &mut overlapped,
            )
        };

        if result == 0 {
            return Err(io::Error::last_os_error());
        }

        let mut bytes_written: u32 = 0;
        let overlapped_result =
            unsafe { GetOverlappedResult(raw_handle, &overlapped, &mut bytes_written, 1) };

        if overlapped_result == 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(bytes_written as usize)
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
