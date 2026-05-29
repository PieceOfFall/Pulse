#[cfg(target_os = "macos")]
mod platform {
    use std::ffi::c_void;

    unsafe extern "C" {
        fn malloc_default_zone() -> *mut c_void;
        fn malloc_zone_pressure_relief(zone: *mut c_void, goal: usize) -> usize;
    }

    pub(super) fn release_allocator_pressure() {
        unsafe {
            let zone = malloc_default_zone();
            if !zone.is_null() {
                let _ = malloc_zone_pressure_relief(zone, usize::MAX);
            }
        }
    }
}

#[cfg(not(target_os = "macos"))]
mod platform {
    pub(super) fn release_allocator_pressure() {}
}

pub(in crate::broker) fn release_allocator_pressure() {
    platform::release_allocator_pressure();
}
