use std::sync::OnceLock;

mod file;
mod open_options;

pub use file::File;
pub use open_options::OpenOptions;

static SECTOR_SIZE: OnceLock<usize> = OnceLock::new();

fn alloc_aligend_buffer(size: usize) -> Vec<u8> {
    let sector_size = get_sector_size();

    let layout = std::alloc::Layout::from_size_align(size, sector_size)
        .expect("Failed to create layout for aligned buffer");

    unsafe {
        let ptr = std::alloc::alloc_zeroed(layout);
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        Vec::from_raw_parts(ptr, size, size)
    }
}

fn get_sector_size() -> usize {
    *SECTOR_SIZE.get_or_init(|| {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;

            let file = std::fs::File::open("/dev/null").expect("Failed to open /dev/null");
            let fd = file.as_raw_fd();
            let mut sector_size: u32 = 0;

            unsafe {
                assert_ne!(
                    libc::ioctl(fd, libc::BLKSSZGET, &mut sector_size),
                    -1,
                    "Failed to get sector size"
                );
            }

            sector_size as usize
        }
    })
}
