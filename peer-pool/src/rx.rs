//! RandomX (rx/0) through tevador's librandomx in light mode (256 MiB cache per key), with the node's verification flags.
//! One Cache per epoch key, shared read-only; each verifier thread re-points its own VM at the job's cache.

use std::ffi::c_void;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

#[allow(non_camel_case_types)]
type randomx_flags = u32;
#[repr(C)]
struct RxCache {
    _p: [u8; 0],
}
#[repr(C)]
struct RxVm {
    _p: [u8; 0],
}

unsafe extern "C" {
    fn randomx_get_flags() -> randomx_flags;
    fn randomx_alloc_cache(flags: randomx_flags) -> *mut RxCache;
    fn randomx_init_cache(cache: *mut RxCache, key: *const c_void, key_size: usize);
    fn randomx_release_cache(cache: *mut RxCache);
    fn randomx_create_vm(flags: randomx_flags, cache: *mut RxCache, dataset: *mut c_void) -> *mut RxVm;
    fn randomx_vm_set_cache(vm: *mut RxVm, cache: *mut RxCache);
    fn randomx_destroy_vm(vm: *mut RxVm);
    fn randomx_calculate_hash(vm: *mut RxVm, input: *const c_void, input_size: usize, output: *mut c_void);
}

/// The RandomX cache for one key. Read-only after init, so it may be shared across threads.
pub struct Cache {
    ptr: *mut RxCache,
    pub key: Vec<u8>,
}
unsafe impl Send for Cache {}
unsafe impl Sync for Cache {}

impl Cache {
    pub fn new(key: &[u8]) -> Cache {
        assert!(!key.is_empty());
        unsafe {
            let ptr = randomx_alloc_cache(randomx_get_flags());
            assert!(!ptr.is_null(), "randomx: cannot allocate the cache");
            randomx_init_cache(ptr, key.as_ptr() as *const c_void, key.len());
            Cache { ptr, key: key.to_vec() }
        }
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        unsafe { randomx_release_cache(self.ptr) }
    }
}

/// A light-mode VM. Holds an Arc to its current cache so the cache outlives it.
pub struct Vm {
    ptr: *mut RxVm,
    cache: Arc<Cache>,
}
unsafe impl Send for Vm {}

impl Vm {
    pub fn new(cache: Arc<Cache>) -> Vm {
        unsafe {
            let ptr = randomx_create_vm(randomx_get_flags(), cache.ptr, std::ptr::null_mut());
            assert!(!ptr.is_null(), "randomx: cannot create a VM");
            Vm { ptr, cache }
        }
    }

    pub fn hash(&mut self, cache: &Arc<Cache>, input: &[u8]) -> [u8; 32] {
        if !Arc::ptr_eq(&self.cache, cache) {
            unsafe { randomx_vm_set_cache(self.ptr, cache.ptr) };
            self.cache = cache.clone();
        }
        let mut out = [0u8; 32];
        unsafe {
            randomx_calculate_hash(self.ptr, input.as_ptr() as *const c_void, input.len(), out.as_mut_ptr() as *mut c_void)
        };
        out
    }
}

impl Drop for Vm {
    fn drop(&mut self) {
        unsafe { randomx_destroy_vm(self.ptr) }
    }
}

type Task = (Arc<Cache>, Vec<u8>, mpsc::Sender<[u8; 32]>);

/// A fixed set of verifier threads, so share checks never use more than `threads` cores however
/// many miners are connected.
pub struct Verifier {
    tx: Mutex<mpsc::Sender<Task>>,
}

impl Verifier {
    pub fn new(threads: usize) -> Verifier {
        let (tx, rx) = mpsc::channel::<Task>();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..threads.max(1) {
            let rx = rx.clone();
            thread::spawn(move || {
                let mut vm: Option<Vm> = None;
                loop {
                    let task = rx.lock().unwrap().recv();
                    let Ok((cache, input, reply)) = task else { return };
                    let v = vm.get_or_insert_with(|| Vm::new(cache.clone()));
                    let _ = reply.send(v.hash(&cache, &input));
                }
            });
        }
        Verifier { tx: Mutex::new(tx) }
    }

    pub fn hash(&self, cache: &Arc<Cache>, input: &[u8]) -> [u8; 32] {
        let (rtx, rrx) = mpsc::channel();
        self.tx.lock().unwrap().send((cache.clone(), input.to_vec(), rtx)).expect("verifier threads alive");
        rrx.recv().expect("verifier answered")
    }
}
