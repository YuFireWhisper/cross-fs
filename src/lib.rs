use std::sync::OnceLock;

mod file;
mod open_options;

pub use file::File;
pub use open_options::OpenOptions;

static SECTOR_SIZE: OnceLock<usize> = OnceLock::new();

#[cfg(windows)]
const DERIVE_PATH: &str = "\\\\.\\PhysicalDrive0";

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

#[cfg(target_os = "linux")]
fn get_sector_size() -> usize {
    *SECTOR_SIZE.get_or_init(|| {
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
    })
}

#[cfg(windows)]
fn get_sector_size() -> usize {
    *SECTOR_SIZE.get_or_init(|| {
        use std::{
            ffi::{OsStr, c_void},
            os::windows::ffi::OsStrExt,
        };

        use windows_sys::Win32::{
            Foundation::{HANDLE, INVALID_HANDLE_VALUE},
            Storage::FileSystem::{
                CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_READ, FILE_SHARE_WRITE,
                OPEN_EXISTING,
            },
            System::{
                IO::DeviceIoControl,
                Ioctl::{
                    IOCTL_STORAGE_QUERY_PROPERTY, PropertyStandardQuery,
                    STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR, STORAGE_PROPERTY_QUERY,
                    StorageAccessAlignmentProperty,
                },
            },
        };

        let path = OsStr::new(DERIVE_PATH)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<u16>>();

        let handle: HANDLE = unsafe {
            CreateFileW(
                path.as_ptr(),
                0,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                std::ptr::null_mut(),
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                std::ptr::null_mut(),
            )
        };

        assert_ne!(
            handle,
            INVALID_HANDLE_VALUE,
            "{}",
            std::io::Error::last_os_error()
        );

        let query = STORAGE_PROPERTY_QUERY {
            PropertyId: StorageAccessAlignmentProperty,
            QueryType: PropertyStandardQuery,
            AdditionalParameters: [0],
        };

        let mut desc: STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR = unsafe { std::mem::zeroed() };
        let mut bytes_returned: u32 = 0;

        let success = unsafe {
            DeviceIoControl(
                handle,
                IOCTL_STORAGE_QUERY_PROPERTY,
                &query as *const _ as *mut c_void,
                std::mem::size_of::<STORAGE_PROPERTY_QUERY>() as u32,
                &mut desc as *mut _ as *mut c_void,
                std::mem::size_of::<STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR>() as u32,
                &mut bytes_returned,
                std::ptr::null_mut(),
            )
        };

        assert_ne!(success, 0, "{}", std::io::Error::last_os_error());

        desc.BytesPerLogicalSector as usize
    })
}
