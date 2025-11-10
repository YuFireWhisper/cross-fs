#![feature(can_vector, unix_file_vectored_at)]

mod file;
mod file_ext;
mod open_options;

pub use file::File;
pub use file_ext::FileExt;
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

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};

    use tempfile::tempdir;

    use super::*;

    const FILE_NAME: &str = "testfile";
    const FILE_SIZE: usize = ALIGN * 2;

    #[cfg(feature = "direct-io")]
    const DIO_BUFFER_SIZE: usize = ALIGN;

    #[test]
    fn basic_read_write() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join(FILE_NAME);

        let mut file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .expect("Failed to create test file");

        let data = vec![1u8; FILE_SIZE];

        file.write_all(&data).expect("Failed to write to test file");
        file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to start of test file");

        let mut buf = vec![0u8; FILE_SIZE];
        let n = file.read(&mut buf).expect("Failed to read from test file");

        assert_eq!(n, FILE_SIZE);
        assert_eq!(buf, data);
    }

    #[test]
    #[cfg(feature = "direct-io")]
    fn direct_io_read_write() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join(FILE_NAME);

        let mut file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .direct_io(DIO_BUFFER_SIZE)
            .open(&file_path)
            .expect("Failed to create test file with direct I/O");

        let data = vec![1u8; FILE_SIZE];

        file.write_all(&data)
            .expect("Failed to write to test file with direct I/O");
        file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to start of test file with direct I/O");

        let mut buf = vec![0u8; FILE_SIZE];
        let n = file
            .read(&mut buf)
            .expect("Failed to read from test file with direct I/O");

        assert_eq!(n, FILE_SIZE);
        assert_eq!(buf, data);
    }

    #[test]
    #[cfg(feature = "direct-io")]
    fn read_lager_than_buffer_data() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join(FILE_NAME);

        let mut file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .direct_io(DIO_BUFFER_SIZE / 2)
            .open(&file_path)
            .expect("Failed to create test file with direct I/O");

        let data = vec![1u8; DIO_BUFFER_SIZE];

        file.write_all(&data)
            .expect("Failed to write to test file with direct I/O");
        file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to start of test file with direct I/O");

        let mut buf = vec![0u8; DIO_BUFFER_SIZE];
        let n = file
            .read(&mut buf)
            .expect("Failed to read from test file with direct I/O");

        assert_eq!(n, DIO_BUFFER_SIZE);
        assert_eq!(buf, data);
    }

    #[test]
    #[cfg(all(target_os = "linux", not(feature = "direct-io")))]
    fn read_vectored() {
        use std::io::IoSliceMut;

        let dir = tempdir().unwrap();
        let file_path = dir.path().join(FILE_NAME);

        let mut file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .expect("Failed to create test file");

        let mut data = vec![1u8; FILE_SIZE];
        data[FILE_SIZE / 4] = 2;
        data[FILE_SIZE / 2 + 3] = 3;
        data[FILE_SIZE - 1] = 4;

        file.write_all(&data).expect("Failed to write to test file");
        file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to start of test file");

        let mut buf1 = vec![0u8; FILE_SIZE / 4];
        let mut buf2 = vec![0u8; FILE_SIZE / 4];
        let mut buf3 = vec![0u8; FILE_SIZE / 2];

        let mut bufs = [
            IoSliceMut::new(&mut buf1),
            IoSliceMut::new(&mut buf2),
            IoSliceMut::new(&mut buf3),
        ];

        let n = file
            .read_vectored(&mut bufs)
            .expect("Failed to read from test file");

        assert_eq!(n, FILE_SIZE);
        assert_eq!(&buf1, &data[..FILE_SIZE / 4]);
        assert_eq!(&buf2, &data[FILE_SIZE / 4..FILE_SIZE / 2]);
        assert_eq!(&buf3, &data[FILE_SIZE / 2..]);
    }

    #[test]
    #[cfg(all(target_os = "linux", feature = "direct-io"))]
    fn direct_io_read_vectored() {
        use std::io::IoSliceMut;

        let dir = tempdir().unwrap();
        let file_path = dir.path().join(FILE_NAME);

        let mut file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .direct_io(DIO_BUFFER_SIZE)
            .open(&file_path)
            .expect("Failed to create test file with direct I/O");

        let mut data = vec![1u8; FILE_SIZE];
        data[1] = 2;
        data[FILE_SIZE - 1] = 3;

        file.write_all(&data)
            .expect("Failed to write to test file with direct I/O");
        file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to start of test file with direct I/O");

        let mut unalign_buf = loop {
            let buf = vec![0u8; FILE_SIZE / 2];
            if !(buf.as_ptr() as usize).is_multiple_of(ALIGN) {
                break buf;
            }
        };
        let mut align_buf = avec!(FILE_SIZE / 2);
        let mut bufs = [
            IoSliceMut::new(&mut unalign_buf),
            IoSliceMut::new(&mut align_buf),
        ];

        {
            if cfg!(target_os = "linux") {
                println!("Running on Linux");
            }

            if cfg!(feature = "direct-io") {
                println!("Direct I/O feature is enabled");
            }
        }

        let n = file
            .read_vectored(&mut bufs)
            .expect("Failed to read from test file with direct I/O");

        dbg!(n);

        file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to start of test file with direct I/O");
        let mut tmp = avec!(FILE_SIZE);
        file.read_exact(&mut tmp)
            .expect("Failed to read from test file with direct I/O");
        assert_eq!(&tmp, &data);

        assert_eq!(n, FILE_SIZE);
        assert_eq!(&unalign_buf, &data[..FILE_SIZE / 2]);
        assert_eq!(&align_buf, &data[FILE_SIZE / 2..]);
    }
}
