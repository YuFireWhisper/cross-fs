use std::{io, path::Path, sync::OnceLock};

use crate::file::File;

static SECTOR_SIZE: OnceLock<usize> = OnceLock::new();

#[derive(Clone)]
#[derive(Debug)]
#[derive(Default)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,

    // special
    direct_io: bool,
    direct_io_buffer_size: usize,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn read(&mut self, read: bool) -> &mut Self {
        self.read = read;
        self
    }

    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    pub fn append(&mut self, append: bool) -> &mut Self {
        self.append = append;
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        self
    }

    pub fn direct_io(&mut self, direct_io: bool, buffer_size: usize) -> &mut Self {
        self.direct_io = direct_io;
        self.direct_io_buffer_size = buffer_size;
        self
    }

    pub fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<File> {
        let mut opts = std::fs::OpenOptions::new();
        opts.read(self.read);
        opts.write(self.write);
        opts.append(self.append);
        opts.truncate(self.truncate);
        opts.create(self.create);
        opts.create_new(self.create_new);

        let mut direct_io_buffer = None;

        if self.direct_io {
            #[cfg(target_os = "linux")]
            {
                use std::{
                    alloc::{Layout, alloc},
                    os::unix::fs::OpenOptionsExt,
                };

                opts.custom_flags(libc::O_DIRECT);

                let layout = Layout::from_size_align(self.direct_io_buffer_size, get_sector_size())
                    .expect("Invalid layout for direct I/O buffer");

                unsafe {
                    let ptr = alloc(layout);
                    assert!(!ptr.is_null(), "Failed to allocate direct I/O buffer");

                    direct_io_buffer = Some(Vec::from_raw_parts(
                        ptr,
                        self.direct_io_buffer_size,
                        self.direct_io_buffer_size,
                    ));
                }
            }
        }

        let base = opts.open(path)?;

        Ok(File {
            inner: base,
            direct_io_buffer: direct_io_buffer.unwrap_or_default(),
        })
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
