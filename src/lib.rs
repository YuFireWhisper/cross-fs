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

    use super::*;

    struct FileWrapper(File);

    impl Drop for FileWrapper {
        fn drop(&mut self) {
            let _ = std::fs::remove_file("testfile.dat");
        }
    }

    const TEST_FILE_PATH: &str = "testfile.dat";
    const TEST_FILE: [u8; 4096] = [2u8; 4096];

    #[test]
    fn basic_read_write() {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(TEST_FILE_PATH)
            .expect("Failed to create test file");
        let wrapper = FileWrapper(file);
        let mut file = &wrapper.0;

        file.write_all(&TEST_FILE)
            .expect("Failed to write to test file");

        file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to start of test file");

        let mut buf = vec![0u8; 4096];
        let n = file.read(&mut buf).expect("Failed to read from test file");

        assert_eq!(n, 4096);
        assert_eq!(buf, TEST_FILE);
    }

    #[test]
    #[cfg(feature = "direct-io")]
    fn direct_io_read_write() {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .direct_io(4096)
            .open(TEST_FILE_PATH)
            .expect("Failed to create test file with direct I/O");
        let wrapper = FileWrapper(file);
        let mut file = &wrapper.0;

        file.write_all(&TEST_FILE)
            .expect("Failed to write to test file with direct I/O");

        file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to start of test file with direct I/O");

        let mut buf = vec![0u8; 4096];
        let n = file
            .read(&mut buf)
            .expect("Failed to read from test file with direct I/O");

        assert_eq!(n, 4096);
        assert_eq!(buf, TEST_FILE);
    }
}
