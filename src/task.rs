use core::ffi::c_void;
use core::ptr::NonNull;

use crate::ffi;

pub struct SwiftTask(NonNull<c_void>);

unsafe impl Send for SwiftTask {}
unsafe impl Sync for SwiftTask {}

impl SwiftTask {
    pub fn from_raw(ptr: *mut c_void) -> Option<Self> {
        NonNull::new(ptr).map(Self)
    }

    /// Request cancellation without releasing the handle, so a surviving
    /// observer can still await the task's settlement.
    #[cfg(feature = "async")]
    pub(crate) fn cancel(&self) {
        unsafe { ffi::fm_task_cancel(self.0.as_ptr()) };
    }

    /// The raw handle, for registering a settlement callback.
    #[cfg(feature = "async")]
    pub(crate) fn as_raw(&self) -> *mut c_void {
        self.0.as_ptr()
    }
}

impl Drop for SwiftTask {
    fn drop(&mut self) {
        unsafe {
            ffi::fm_task_cancel(self.0.as_ptr());
            ffi::fm_object_release(self.0.as_ptr());
        }
    }
}
