// Links the static librandomx (tevador RandomX, BSD-3-Clause) built by scripts/build-node.sh.
// RANDOMX_LIB_DIR points at the directory holding librandomx.a (default /out, the peercash-bin volume).
fn main() {
    let dir = std::env::var("RANDOMX_LIB_DIR").unwrap_or_else(|_| "/out".into());
    println!("cargo:rerun-if-env-changed=RANDOMX_LIB_DIR");
    println!("cargo:rustc-link-search=native={dir}");
    println!("cargo:rustc-link-lib=static=randomx");
    println!("cargo:rustc-link-lib=dylib=stdc++");
}
