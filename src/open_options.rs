use std::{io, path::Path};

use crate::file::File;

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
    direct_io_buffer_size: usize, // 0 means disabled
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

    /// Set the buffer size for direct I/O operations.
    ///
    /// # Arguments
    ///
    /// * `buffer_size` - The size of the buffer to be used for direct I/O. A value of 0 disables direct I/O.
    pub fn direct_io(&mut self, buffer_size: usize) -> &mut Self {
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

        if self.direct_io_buffer_size > 0 {
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::fs::OpenOptionsExt;

                use crate::alloc_aligend_buffer;

                opts.custom_flags(libc::O_DIRECT);
                direct_io_buffer = Some(alloc_aligend_buffer(self.direct_io_buffer_size));
            }
        }

        let base = opts.open(path)?;

        Ok(File {
            inner: base,
            direct_io_buffer: direct_io_buffer.unwrap_or_default(),
        })
    }
}
