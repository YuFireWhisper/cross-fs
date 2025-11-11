#![feature(can_vector, io_const_error)]
#![cfg_attr(target_family = "unix", feature(unix_file_vectored_at))]

use std::io;

mod open_options;

pub use open_options::OpenOptions;

#[cfg(feature = "align-512")]
pub const ALIGN: usize = 512;

#[cfg(not(feature = "align-512"))]
pub const ALIGN: usize = 4096;

#[macro_export]
macro_rules! avec {
    ($cap:expr) => {{
        let layout = ::std::alloc::Layout::from_size_align($cap, $crate::ALIGN).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        unsafe { Vec::<u8>::from_raw_parts(ptr as *mut u8, $cap, $cap) }
    }};
}

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
    fn read_vectored(&self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize>;
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize>;
    fn write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize>;
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize>;
}

#[cfg(unix)]
impl PositionedExt for std::fs::File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(self, buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::write_at(self, buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        std::os::unix::fs::FileExt::write_all_at(self, buf, offset)
    }
}

#[cfg(unix)]
impl VectoredExt for std::fs::File {
    fn read_vectored(&self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.read_vectored(bufs)
    }

    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::read_vectored_at(self, bufs, offset)
    }

    fn write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.write_vectored(bufs)
    }

    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::write_vectored_at(self, bufs, offset)
    }
}

#[cfg(target_os = "windows")]
impl PositionedExt for std::fs::File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_read(self, buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_write(self, buf, offset)
    }
}

#[cfg(target_os = "windows")]
impl VectoredExt for std::fs::File {
    fn read_vectored(&self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        windows_read_vectored_handler(self, bufs, None)
    }

    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        windows_read_vectored_handler(self, bufs, Some(offset))
    }

    fn write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        windows_write_vectored_handler(self, bufs, None)
    }

    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        windows_write_vectored_handler(self, bufs, Some(offset))
    }
}

#[cfg(target_os = "windows")]
fn windows_read_vectored_handler(
    file: &std::fs::File,
    bufs: &mut [io::IoSliceMut<'_>],
    offset: Option<u64>,
) -> io::Result<usize> {
    use windows_sys::Win32::Storage::FileSystem::FILE_SEGMENT_ELEMENT;

    let mut segments = Vec::with_capacity(bufs.len() + 1);
    let mut total_len = 0;

    for buf in bufs.iter_mut() {
        segments.push(FILE_SEGMENT_ELEMENT {
            Buffer: buf.as_mut_ptr() as *mut _,
        });
        total_len += buf.len();
    }
    segments.push(unsafe { std::mem::zeroed() });

    windows_vectored_handler_inner(file, &segments, total_len, offset, true)
}

#[cfg(target_os = "windows")]
fn windows_vectored_handler_inner(
    file: &std::fs::File,
    segments: &[windows_sys::Win32::Storage::FileSystem::FILE_SEGMENT_ELEMENT],
    total_len: usize,
    offset: Option<u64>,
    is_read: bool,
) -> io::Result<usize> {
    use std::os::windows::{io::AsRawHandle, raw::HANDLE};

    use windows_sys::Win32::{
        Storage::FileSystem::{ReadFileScatter, WriteFileGather},
        System::IO::{GetOverlappedResult, OVERLAPPED},
    };

    let raw_handle: HANDLE = file.as_raw_handle() as _;
    let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
    if let Some(off) = offset {
        overlapped.Anonymous.Anonymous.Offset = (off & 0xFFFFFFFF) as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (off >> 32) as u32;
    }

    let result = if is_read {
        unsafe {
            ReadFileScatter(
                raw_handle,
                segments.as_ptr(),
                total_len as u32,
                std::ptr::null_mut(),
                &mut overlapped,
            )
        }
    } else {
        unsafe {
            WriteFileGather(
                raw_handle,
                segments.as_ptr(),
                total_len as u32,
                std::ptr::null_mut(),
                &mut overlapped,
            )
        }
    };

    if result == 0 {
        return Err(io::Error::last_os_error());
    }

    let mut n: u32 = 0;

    if unsafe { GetOverlappedResult(raw_handle, &overlapped, &mut n, 1) } == 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(n as usize)
}

#[cfg(target_os = "windows")]
fn windows_write_vectored_handler(
    file: &std::fs::File,
    bufs: &[io::IoSlice<'_>],
    offset: Option<u64>,
) -> io::Result<usize> {
    use windows_sys::Win32::Storage::FileSystem::FILE_SEGMENT_ELEMENT;

    let mut segments = Vec::with_capacity(bufs.len() + 1);
    let mut total_len = 0;

    for buf in bufs.iter() {
        segments.push(FILE_SEGMENT_ELEMENT {
            Buffer: buf.as_ptr() as *mut _,
        });
        total_len += buf.len();
    }
    segments.push(unsafe { std::mem::zeroed() });

    windows_vectored_handler_inner(file, &segments, total_len, offset, false)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    const FILE_NAME: &str = "testfile";
    const TEST_DIRECT_IO_DATA: [u8; ALIGN * 2] = [5; ALIGN * 2];

    #[test]
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    fn direct_io() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join(FILE_NAME);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .direct_io(true)
            .open(&file_path)
            .unwrap();

        let mut buf = avec!(ALIGN * 2);
        buf.copy_from_slice(&TEST_DIRECT_IO_DATA);

        let n = file.write(&buf).unwrap();
        assert_eq!(n, ALIGN * 2);

        let mut read_buf = avec!(ALIGN * 2);
        let n = file.read_at(&mut read_buf, 0).unwrap();
        assert_eq!(n, ALIGN * 2);
        assert_eq!(read_buf, buf);
    }
}
