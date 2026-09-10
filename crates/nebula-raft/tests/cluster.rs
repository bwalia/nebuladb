//! Multi-node Raft integration test (design 0011 §6).
//!
//! This is the harness that did not previously exist: the in-crate unit
//! tests only bootstrap a *single* node and round-trip the gRPC vote
//! path against a stub. Here we bring up three real `openraft::Raft`
//! instances over the real gRPC transport on ephemeral ports, form a
//! cluster, replicate a mutation, then exercise the dynamic-membership
//! surface added in design 0011: add a learner, promote it to voter,
//! and remove an original voter — asserting no committed data is lost
//! and writes keep committing across the reconfiguration.
//!
//! It is deliberately a single end-to-end test rather than many small
//! ones: standing up a cluster is expensive, and the interesting
//! assertions are sequential stages of one cluster's lifecycle.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use nebula_embed::{Embedder, MockEmbedder};
use nebula_index::TextIndex;
use nebula_raft::{bootstrap, LogPayload, NebulaNode, NodeId, RaftConfig, RaftHandle};
use nebula_vector::{HnswConfig, Metric};
use nebula_wal::WalRecord;

/// A bootstrapped node plus the TCP listener its gRPC transport serves
/// on. We pre-bind the listener so we know the port *before* building
/// the peer map, then hand the bound listener to tonic — this avoids
/// the bind-race of "pick a port, hope it's free at serve time."
struct Node {
    id: NodeId,
    handle: Arc<RaftHandle>,
    index: Arc<TextIndex>,
    // Kept so the temp data dir outlives the node.
    _dir: tempfile::TempDir,
}

fn fresh_index() -> Arc<TextIndex> {
    let emb: Arc<dyn Embedder> = Arc::new(MockEmbedder::default());
    Arc::new(TextIndex::new(emb, Metric::Cosine, HnswConfig::default()).unwrap())
}

/// Build a deterministic normalized vector for `text`, matching the
/// shape the state-machine unit tests use so applied state is realistic.
fn vector_for(text: &str) -> Vec<f32> {
    let dim = MockEmbedder::default().dim();
    let mut v = vec![0.0_f32; dim];
    for (i, b) in text.bytes().enumerate() {
        v[i % dim] += b as f32 / 255.0;
    }
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in &mut v {
        *x /= norm;
    }
    v
}

fn upsert_payload(id: &str, text: &str) -> LogPayload {
    LogPayload::Mutation(WalRecord::UpsertText {
        bucket: "b".into(),
        external_id: id.into(),
        text: text.into(),
        vector: vector_for(text),
        metadata_json: "{}".into(),
    })
}

/// Bind a listener on an ephemeral loopback port and return
/// (listener, "127.0.0.1:PORT").
async fn bind_ephemeral() -> (tokio::net::TcpListener, String) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    (l, addr)
}

/// Bootstrap a node with the given id + peer map, then spawn its gRPC
/// transport on the pre-bound listener.
async fn start_node(
    id: NodeId,
    listener: tokio::net::TcpListener,
    addr: String,
    peers: BTreeMap<NodeId, NebulaNode>,
) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let index = fresh_index();
    let cfg = RaftConfig {
        node_id: id,
        peers,
        data_dir: dir.path().to_path_buf(),
        bind: addr,
        cluster_name: "itest".into(),
    };
    let handle = Arc::new(bootstrap(cfg, Arc::clone(&index)).await.unwrap());

    let svc = nebula_raft::RaftRpcServer::new(Arc::clone(handle.raft()));
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(nebula_raft::NebulaRaftServer::new(svc))
            .serve_with_incoming(incoming),
    );

    Node {
        id,
        handle,
        index,
        _dir: dir,
    }
}

/// Poll until some node reports a leader, or panic after `timeout`.
/// Returns the leader's node id.
async fn wait_for_leader(nodes: &[Node], timeout: Duration) -> NodeId {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        for n in nodes {
            if let Some(leader) = n.handle.raft().metrics().borrow().current_leader {
                return leader;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("no leader elected within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn node_by_id(nodes: &[Node], id: NodeId) -> &Node {
    nodes.iter().find(|n| n.id == id).expect("node id present")
}

/// Wait until `pred(index_len)` holds on the given node, or panic.
async fn wait_for_doc_count(node: &Node, want: usize, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if node.index.len() == want {
            return;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "node {} never reached {} docs (saw {})",
                node.id,
                want,
                node.index.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_forms_replicates_and_reconfigures() {
    // Set RUST_LOG=openraft=debug to trace the consensus internals when
    // diagnosing a failure; the subscriber is best-effort so parallel
    // tests that also init one don't conflict.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();

    // ---- stage 0: bring up three nodes, form the cluster ----
    let (l1, a1) = bind_ephemeral().await;
    let (l2, a2) = bind_ephemeral().await;
    let (l3, a3) = bind_ephemeral().await;
    // A fourth listener reserved for the replacement/learner node. It is
    // NOT in the initial membership.
    let (l4, a4) = bind_ephemeral().await;

    let mut peers = BTreeMap::new();
    peers.insert(1u64, NebulaNode::new(a1.clone()));
    peers.insert(2u64, NebulaNode::new(a2.clone()));
    peers.insert(3u64, NebulaNode::new(a3.clone()));

    let mut nodes = vec![
        start_node(1, l1, a1, peers.clone()).await,
        start_node(2, l2, a2, peers.clone()).await,
        start_node(3, l3, a3, peers.clone()).await,
    ];

    // Give the transports a beat to accept connections before we form.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Formation is a one-time call on a single node with the full set.
    node_by_id(&nodes, 1)
        .handle
        .initialize_cluster()
        .await
        .expect("initialize should succeed on a fresh cluster");

    // Re-initializing is idempotent — must not error the second time.
    match node_by_id(&nodes, 1).handle.initialize_cluster().await {
        Err(nebula_raft::MembershipError::AlreadyInitialized) => {}
        other => panic!("expected AlreadyInitialized, got {other:?}"),
    }

    let leader_id = wait_for_leader(&nodes, Duration::from_secs(10)).await;
    assert!((1..=3).contains(&leader_id), "leader must be one of 1..=3");

    // ---- stage 1: replicate a mutation, assert convergence ----
    let leader = node_by_id(&nodes, leader_id);
    leader
        .handle
        .submit_mutation(upsert_payload("d1", "alpha"))
        .await
        .expect("write to leader should commit");

    // Every voter should converge to one doc.
    for n in &nodes {
        wait_for_doc_count(n, 1, Duration::from_secs(10)).await;
    }

    // A write to a follower must be redirected, never silently dropped.
    let follower = nodes.iter().find(|n| n.id != leader_id).unwrap();
    match follower
        .handle
        .submit_mutation(upsert_payload("d-nope", "beta"))
        .await
    {
        Err(nebula_raft::SubmitError::NotLeader { .. }) => {}
        other => panic!("follower write should be NotLeader, got {other:?}"),
    }

    // ---- stage 2: add a 4th node as a learner, then promote it ----
    let node4 = start_node(4, l4, a4.clone(), peers.clone()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let leader = node_by_id(&nodes, leader_id);
    leader
        .handle
        .add_learner(4, a4.clone())
        .await
        .expect("add_learner should catch the new node up");

    // Blocking add_learner means node 4 has the committed doc.
    wait_for_doc_count(&node4, 1, Duration::from_secs(10)).await;

    let view = leader.handle.membership();
    assert!(view.learners.contains(&4), "node 4 should be a learner");
    assert!(!view.voters.contains(&4), "node 4 not yet a voter");

    leader
        .handle
        .promote_to_voter(4)
        .await
        .expect("promote learner->voter should commit");

    let view = leader.handle.membership();
    assert!(view.voters.contains(&4), "node 4 is now a voter");

    // ---- stage 3: a write during/after reconfiguration still commits ----
    leader
        .handle
        .submit_mutation(upsert_payload("d2", "gamma"))
        .await
        .expect("write after growing the cluster should commit");

    // Add node 4 to the tracked set so we can assert on it.
    nodes.push(node4);
    for n in &nodes {
        wait_for_doc_count(n, 2, Duration::from_secs(10)).await;
    }

    // ---- stage 4: remove an original voter, assert no data loss ----
    // Remove a node that is NOT the current leader (removing the leader
    // is legal but forces a re-election we don't need to exercise here).
    let victim = nodes
        .iter()
        .find(|n| n.id != leader_id && n.id != 4)
        .expect("some non-leader original voter exists")
        .id;

    let leader = node_by_id(&nodes, leader_id);
    leader
        .handle
        .remove_node(victim)
        .await
        .expect("removing a voter should commit");

    let view = leader.handle.membership();
    assert!(
        !view.voters.contains(&victim),
        "victim {victim} should be gone from voters"
    );

    // The surviving cluster still commits writes and keeps all data.
    leader
        .handle
        .submit_mutation(upsert_payload("d3", "delta"))
        .await
        .expect("write after shrinking the cluster should commit");

    for n in nodes.iter().filter(|n| n.id != victim) {
        wait_for_doc_count(n, 3, Duration::from_secs(10)).await;
    }
}
