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

        if self.direct_io {
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.custom_flags(libc::O_DIRECT);
            }
        }

        let base = opts.open(path)?;

        Ok(File {
            inner: base,
            direct_io: self.direct_io,
            direct_io_buffer_size: self.direct_io_buffer_size,
        })
    }
}
