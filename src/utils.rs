use std::alloc::Layout;

#[cfg(feature = "align-512")]
const ALIGN: usize = 512;

#[cfg(not(feature = "align-512"))]
const ALIGN: usize = 4096;

pub fn alloc_aligend_buffer(size: usize) -> Vec<u8> {
    let layout =
        Layout::from_size_align(size, ALIGN).expect("Failed to create layout for aligned buffer");

    unsafe {
        let ptr = std::alloc::alloc_zeroed(layout);
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        Vec::from_raw_parts(ptr, size, size)
    }
}
