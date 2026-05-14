//! gRPC transport for openraft.
//!
//! # Wire shape
//!
//! See `proto/raft.proto`. Every RPC body is a single `bytes` field
//! carrying a `bincode`-encoded openraft request or response. This is
//! a deliberate shortcut — openraft's request/response types already
//! have `serde` derives, so we avoid maintaining a parallel protobuf
//! schema that would drift across openraft version bumps. The
//! within-region constraint (peers always run the same nebuladb
//! binary) makes the version-coupling cost acceptable.
//!
//! # Three pieces
//!
//! - [`RaftRpcServer`] — implements the gRPC service, owned by the
//!   incoming-server side. Hands deserialized requests off to
//!   `Raft::append_entries` / `Raft::vote` / `Raft::install_snapshot`.
//! - [`GrpcRaftNetworkFactory`] — implements `RaftNetworkFactory`.
//!   openraft asks it for a per-target `Network`; the factory dials
//!   the target's gRPC URL and returns a `GrpcRaftNetwork`.
//! - [`GrpcRaftNetwork`] — implements `RaftNetwork`. Bincode-encodes
//!   the openraft request, sends it via tonic, decodes the response.
//!
//! # What this does NOT do (yet)
//!
//! - Streaming snapshot transfer. `install_snapshot` follows the
//!   chunked default openraft provides: many small RPCs back-to-back.
//!   For a 1 GiB index that's still bounded memory — the cursor we
//!   build from in `snapshot.rs` is allocated once and openraft
//!   slices it. Phase 2.5 may switch to a streaming RPC if the
//!   per-chunk overhead bites.
//! - Mutual TLS. Plain h2 only. Adding mTLS is a separate config
//!   knob in 2.5; the gRPC transport doesn't inherently block it.
//! - Backoff tuning. We use openraft's default 500ms constant; tune
//!   in soak (Phase 2.6) if peer flap causes unnecessary churn.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use parking_lot::Mutex;
use tonic::transport::{Channel, Endpoint};

use crate::types::{NebulaNode, NebulaTypeConfig, NodeId};

// Generated tonic stubs. The build script compiles `proto/raft.proto`
// into this module path.
pub mod proto {
    tonic::include_proto!("nebula.raft.v1");
}

use proto::nebula_raft_client::NebulaRaftClient;
pub use proto::nebula_raft_server::{NebulaRaft, NebulaRaftServer};
use proto::RaftRpc;

// ---------------------------------------------------------------
// Server side: NebulaRaft service impl
// ---------------------------------------------------------------

/// gRPC service implementation. One per node; carries a handle to the
/// local `openraft::Raft` so incoming RPCs can be dispatched.
///
/// Phase 2.5 will wire this in `nebula-server`'s boot path, binding
/// the service on `NEBULA_RAFT_BIND` (default `0.0.0.0:50052`). The
/// handle is `Arc`-shared so the same `Raft` instance can be both
/// driven by local clients (the storage layer in 2.3) AND react to
/// peer RPCs (here).
#[derive(Clone)]
pub struct RaftRpcServer {
    raft: Arc<openraft::Raft<NebulaTypeConfig>>,
}

impl RaftRpcServer {
    pub fn new(raft: Arc<openraft::Raft<NebulaTypeConfig>>) -> Self {
        Self { raft }
    }
}

// `tonic::Status` is large (~176 bytes) so clippy's `result_large_err`
// fires here. We can't shrink the error type — it's the trait return
// value tonic mandates. Suppressed at the helper level rather than at
// every call site.
#[allow(clippy::result_large_err)]
fn bincode_decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, tonic::Status> {
    bincode::deserialize::<T>(bytes)
        .map_err(|e| tonic::Status::invalid_argument(format!("decode: {e}")))
}

#[allow(clippy::result_large_err)]
fn bincode_encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, tonic::Status> {
    bincode::serialize(value).map_err(|e| tonic::Status::internal(format!("encode: {e}")))
}

#[tonic::async_trait]
impl NebulaRaft for RaftRpcServer {
    async fn append_entries(
        &self,
        request: tonic::Request<RaftRpc>,
    ) -> Result<tonic::Response<RaftRpc>, tonic::Status> {
        let req: AppendEntriesRequest<NebulaTypeConfig> =
            bincode_decode(&request.into_inner().payload)?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| tonic::Status::internal(format!("append_entries: {e}")))?;
        let payload = bincode_encode(&resp)?;
        Ok(tonic::Response::new(RaftRpc { payload }))
    }

    async fn vote(
        &self,
        request: tonic::Request<RaftRpc>,
    ) -> Result<tonic::Response<RaftRpc>, tonic::Status> {
        let req: VoteRequest<NodeId> = bincode_decode(&request.into_inner().payload)?;
        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| tonic::Status::internal(format!("vote: {e}")))?;
        let payload = bincode_encode(&resp)?;
        Ok(tonic::Response::new(RaftRpc { payload }))
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftRpc>,
    ) -> Result<tonic::Response<RaftRpc>, tonic::Status> {
        let req: InstallSnapshotRequest<NebulaTypeConfig> =
            bincode_decode(&request.into_inner().payload)?;
        let resp = self
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| tonic::Status::internal(format!("install_snapshot: {e}")))?;
        let payload = bincode_encode(&resp)?;
        Ok(tonic::Response::new(RaftRpc { payload }))
    }
}

// ---------------------------------------------------------------
// Client side: RaftNetwork + RaftNetworkFactory impls
// ---------------------------------------------------------------

/// Per-target gRPC client. Wraps a tonic `NebulaRaftClient` and
/// implements openraft's `RaftNetwork`.
///
/// We hold the channel by value (cheap clone) so each `&mut self`
/// RPC method can dial without taking a long-lived borrow.
#[derive(Clone)]
pub struct GrpcRaftNetwork {
    target: NodeId,
    addr: String,
    client: NebulaRaftClient<Channel>,
}

impl GrpcRaftNetwork {
    fn unreachable(
        &self,
        e: impl std::fmt::Display,
    ) -> RPCError<NodeId, NebulaNode, RaftError<NodeId>> {
        RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
            "{} (target={}, addr={})",
            e, self.target, self.addr
        ))))
    }

    fn unreachable_install(
        &self,
        e: impl std::fmt::Display,
    ) -> RPCError<NodeId, NebulaNode, RaftError<NodeId, InstallSnapshotError>> {
        RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
            "{} (target={}, addr={})",
            e, self.target, self.addr
        ))))
    }
}

impl RaftNetwork<NebulaTypeConfig> for GrpcRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<NebulaTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NebulaNode, RaftError<NodeId>>>
    {
        let payload = bincode::serialize(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "encode append_entries: {e}"
            ))))
        })?;
        let resp = self
            .client
            .append_entries(tonic::Request::new(RaftRpc { payload }))
            .await
            .map_err(|e| self.unreachable(e))?;
        let bytes = resp.into_inner().payload;
        // openraft remote-error vs local — peer-side internal panic
        // becomes a tonic::Status here, so anything that decodes
        // cleanly as an AppendEntriesResponse is "RPC succeeded".
        bincode::deserialize::<AppendEntriesResponse<NodeId>>(&bytes).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "decode append_entries: {e}"
            ))))
        })
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<NebulaTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, NebulaNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let payload = bincode::serialize(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "encode install_snapshot: {e}"
            ))))
        })?;
        let resp = self
            .client
            .install_snapshot(tonic::Request::new(RaftRpc { payload }))
            .await
            .map_err(|e| self.unreachable_install(e))?;
        let bytes = resp.into_inner().payload;
        bincode::deserialize::<InstallSnapshotResponse<NodeId>>(&bytes).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "decode install_snapshot: {e}"
            ))))
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NebulaNode, RaftError<NodeId>>> {
        let payload = bincode::serialize(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "encode vote: {e}"
            ))))
        })?;
        let resp = self
            .client
            .vote(tonic::Request::new(RaftRpc { payload }))
            .await
            .map_err(|e| self.unreachable(e))?;
        let bytes = resp.into_inner().payload;
        bincode::deserialize::<VoteResponse<NodeId>>(&bytes).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "decode vote: {e}"
            ))))
        })
    }

    // openraft 0.9's full_snapshot default impl chunks via repeated
    // install_snapshot calls. We don't override; the chunked default
    // is correct and matches the buffered Cursor<Vec<u8>> snapshot
    // model from Phase 2.3. A streaming override is a Phase 2.6
    // soak-driven optimization if the per-chunk overhead bites.
}

/// Channel pool keyed by target node-id. Channels are cheap to clone
/// (refcount under the hood) so every `RaftNetwork` instance gets its
/// own clone, but we cache the dial cost per node.
///
/// Connect failure does NOT bubble out of `new_client` — openraft
/// requires the factory to always hand back a network instance, even
/// if the peer is presently unreachable. The tonic channel is lazy:
/// dialing happens on first RPC, not at construction time.
#[derive(Clone, Default)]
pub struct GrpcRaftNetworkFactory {
    inner: Arc<Mutex<HashMap<NodeId, Channel>>>,
}

impl GrpcRaftNetworkFactory {
    pub fn new() -> Self {
        Self::default()
    }

    fn channel_for(&self, target: NodeId, node: &NebulaNode) -> Channel {
        let mut cache = self.inner.lock();
        if let Some(c) = cache.get(&target) {
            return c.clone();
        }
        // tonic Endpoint dial is lazy when constructed via `from_shared`;
        // we still set a connect timeout so a half-open peer doesn't
        // wedge an RPC for the default 30s.
        let endpoint = Endpoint::from_shared(format!("http://{}", node.addr))
            .unwrap_or_else(|_| Endpoint::from_static("http://127.0.0.1:0"))
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10));
        let channel = endpoint.connect_lazy();
        cache.insert(target, channel.clone());
        channel
    }
}

impl RaftNetworkFactory<NebulaTypeConfig> for GrpcRaftNetworkFactory {
    type Network = GrpcRaftNetwork;

    async fn new_client(&mut self, target: NodeId, node: &NebulaNode) -> Self::Network {
        let channel = self.channel_for(target, node);
        let client = NebulaRaftClient::new(channel);
        GrpcRaftNetwork {
            target,
            addr: node.addr.clone(),
            client,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn factory_constructs_without_error_for_unreachable_peer() {
        // openraft's contract: factory must return a network instance
        // even if the peer is dead. Connect happens on first RPC.
        let mut factory = GrpcRaftNetworkFactory::new();
        let node = NebulaNode::new("127.0.0.1:1");
        let _ = factory.new_client(99, &node).await;
    }

    #[tokio::test]
    async fn factory_caches_channels_per_target() {
        let factory = GrpcRaftNetworkFactory::new();
        let node = NebulaNode::new("127.0.0.1:1");
        factory.channel_for(1, &node);
        factory.channel_for(1, &node);
        factory.channel_for(2, &NebulaNode::new("127.0.0.1:2"));
        assert_eq!(factory.inner.lock().len(), 2);
    }

    #[tokio::test]
    async fn client_against_dead_peer_returns_unreachable() {
        // Aim at a port no one is listening on. The tonic channel
        // is lazy so construction succeeds; the first RPC fails.
        let mut factory = GrpcRaftNetworkFactory::new();
        let node = NebulaNode::new("127.0.0.1:1");
        let mut client = factory.new_client(99, &node).await;

        // We can't easily build an AppendEntriesRequest by hand
        // (openraft's request types are not Default), so use vote —
        // its request only needs a Vote which is easy to construct.
        let rpc = VoteRequest::new(openraft::Vote::new(1, 99), None);
        let err = client
            .vote(rpc, RPCOption::new(Duration::from_secs(1)))
            .await
            .unwrap_err();
        // Either Unreachable or Network — both indicate "peer unreachable".
        match err {
            RPCError::Unreachable(_) | RPCError::Network(_) => {}
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    /// Stand-in service that decodes the same way the real one does
    /// but answers from a fixed handler, so we can prove the gRPC
    /// wire path round-trips without needing a real openraft::Raft
    /// (which requires a full storage stack to construct).
    #[derive(Clone, Default)]
    struct EchoVoteRaft {
        granted: bool,
    }

    #[tonic::async_trait]
    impl NebulaRaft for EchoVoteRaft {
        async fn append_entries(
            &self,
            _: tonic::Request<RaftRpc>,
        ) -> Result<tonic::Response<RaftRpc>, tonic::Status> {
            Err(tonic::Status::unimplemented("test stub"))
        }

        async fn vote(
            &self,
            request: tonic::Request<RaftRpc>,
        ) -> Result<tonic::Response<RaftRpc>, tonic::Status> {
            // Decode the incoming VoteRequest exactly as the real
            // server would, then reply with a fixed VoteResponse.
            let req: VoteRequest<NodeId> = bincode_decode(&request.into_inner().payload)?;
            let resp = VoteResponse::<NodeId> {
                vote: req.vote,
                vote_granted: self.granted,
                last_log_id: None,
            };
            let payload = bincode_encode(&resp)?;
            Ok(tonic::Response::new(RaftRpc { payload }))
        }

        async fn install_snapshot(
            &self,
            _: tonic::Request<RaftRpc>,
        ) -> Result<tonic::Response<RaftRpc>, tonic::Status> {
            Err(tonic::Status::unimplemented("test stub"))
        }
    }

    #[tokio::test]
    async fn vote_round_trips_through_real_grpc_transport() {
        // Bind on an ephemeral port so concurrent test runs don't fight.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        // Spawn the server.
        let svc = EchoVoteRaft { granted: true };
        let server = tonic::transport::Server::builder()
            .add_service(NebulaRaftServer::new(svc))
            .serve_with_incoming(incoming);
        let server_handle = tokio::spawn(server);

        // Build a client through the same factory the openraft side uses.
        let mut factory = GrpcRaftNetworkFactory::new();
        let node = NebulaNode::new(addr.to_string());
        let mut client = factory.new_client(7, &node).await;

        let req = VoteRequest::new(openraft::Vote::new(5, 7), None);
        let resp = client
            .vote(req, RPCOption::new(Duration::from_secs(2)))
            .await
            .expect("vote RPC should succeed");
        assert!(resp.vote_granted, "echo server replied granted=true");
        assert_eq!(resp.vote.leader_id.term, 5);

        server_handle.abort();
    }
}
