//! Compile the .proto file into Rust at build time.
//!
//! We tell `tonic-build` to emit both server and client stubs so this
//! crate's tests (and any in-process callers) can use the generated
//! client without a separate crate.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/nebula.proto"], &["proto"])?;
    Ok(())
}
