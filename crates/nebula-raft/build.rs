//! Compile the Raft transport `.proto` into Rust at build time.
//!
//! Generated code is included via `tonic::include_proto!("nebula.raft.v1")`
//! in `network.rs`. Both server and client stubs are emitted so a
//! single nebula-server binary can both serve incoming RPCs and act
//! as a client to its peers — which is what every Raft node does.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/raft.proto"], &["proto"])?;
    Ok(())
}
