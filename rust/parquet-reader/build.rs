use std::{env, path::PathBuf};

fn main() {
    let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let out = crate_dir.join("include").join("parquet_reader.h");
    std::fs::create_dir_all(out.parent().unwrap()).unwrap();
    cbindgen::generate(&crate_dir)
        .expect("cbindgen failed")
        .write_to_file(out);
    println!("cargo:rerun-if-changed=src");
}
