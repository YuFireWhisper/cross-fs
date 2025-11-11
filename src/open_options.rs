use std::{fs::File, io, path::Path};

#[derive(Clone)]
#[derive(Debug)]
#[derive(Default)]
pub struct OpenOptions {
    pub(crate) read: bool,
    pub(crate) write: bool,
    pub(crate) append: bool,
    pub(crate) truncate: bool,
    pub(crate) create: bool,
    pub(crate) create_new: bool,
    pub(crate) direct_io: bool,
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

    pub fn direct_io(&mut self, enable: bool) -> &mut Self {
        self.direct_io = enable;
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

            #[cfg(target_os = "windows")]
            {
                use std::os::windows::fs::OpenOptionsExt;
                opts.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_NO_BUFFERING);
            }
        }

        opts.open(path)
    }
}
