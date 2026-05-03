//! Cluster membership + role declaration.
//!
//! This module describes **what this process is** and **who its
//! peers are** — nothing more. It is deliberately not a membership
//! protocol: no gossip, no leader election, no failure detection
//! beyond "probe each declared peer's /healthz when asked."
//!
//! # Why declarative
//!
//! NebulaDB's replication today is leader→follower(s) with a
//! single writer. That topology is a config artifact — operators
//! decide which process plays which role when they start it. A
//! dynamic membership layer buys nothing until we have more than
//! one way the topology can change at runtime, and that's a later
//! session.
//!
//! # Env contract
//!
//! ```text
//! NEBULA_NODE_ID=node-1                             # opaque string, self-chosen
//! NEBULA_NODE_ROLE=leader|follower|standalone       # defaults to standalone
//! NEBULA_PEERS=node-2=http://n2:8080,node-3=http://n3:8080  # id=base-url pairs
//! ```
//!
//! `standalone` is the single-process default and keeps every pre-
//! replication deployment behaving identically (no new env
//! required). `follower` also triggers `NEBULA_FOLLOW_LEADER`
//! resolution in `main.rs` — the role is descriptive, the leader
//! URL is the operational contract.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// One-process deployment with no replication. Default when
    /// nothing is configured — preserves legacy behavior.
    #[default]
    Standalone,
    /// Accepts writes, serves its WAL to followers via
    /// [`nebula_grpc::ReplicationService`].
    Leader,
    /// Mirrors a leader's WAL. Refuses writes at the router layer
    /// to prevent local divergence.
    Follower,
}

impl FromStr for NodeRole {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "standalone" => Ok(Self::Standalone),
            "leader" => Ok(Self::Leader),
            "follower" => Ok(Self::Follower),
            other => Err(format!("unknown NEBULA_NODE_ROLE={other}")),
        }
    }
}

/// One entry in the node registry. Peers carry just enough info to
/// be reachable — authoritative details (role, durability state,
/// replication lag) come from probing the peer itself.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerInfo {
    pub id: String,
    /// Base URL including scheme, e.g. `http://nebula-follower:8080`.
    /// The probe endpoint `/healthz` is appended at call time.
    pub base_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ClusterConfig {
    pub node_id: Option<String>,
    pub role: NodeRole,
    /// Leader's gRPC URL when `role == Follower`. Mirrored from
    /// `NEBULA_FOLLOW_LEADER` so the cluster view can show which
    /// leader a follower is tailing without the operator having to
    /// correlate two env vars.
    pub leader_url: Option<String>,
    pub peers: Vec<PeerInfo>,
}

impl ClusterConfig {
    /// Parse a single peers env value: `id=url,id2=url2`. Blank
    /// entries are silently skipped; malformed entries are an
    /// error (better to fail at boot than silently drop a peer
    /// the operator thought they'd configured).
    pub fn parse_peers(s: &str) -> Result<Vec<PeerInfo>, String> {
        let mut out = Vec::new();
        for raw in s.split(',') {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            let (id, url) = raw
                .split_once('=')
                .ok_or_else(|| format!("peer entry missing '=' in: {raw}"))?;
            let id = id.trim();
            let url = url.trim();
            if id.is_empty() || url.is_empty() {
                return Err(format!("peer entry has empty id or url: {raw}"));
            }
            out.push(PeerInfo {
                id: id.to_string(),
                base_url: url.to_string(),
            });
        }
        Ok(out)
    }

    /// Assemble from env. Any parsing error surfaces directly —
    /// we want boot to fail loudly on a typo rather than quietly
    /// drop this node off the cluster view.
    pub fn from_env() -> Result<Self, String> {
        let node_id = std::env::var("NEBULA_NODE_ID")
            .ok()
            .filter(|s| !s.is_empty());
        let role = std::env::var("NEBULA_NODE_ROLE")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<NodeRole>())
            .transpose()?
            .unwrap_or_default();
        let leader_url = std::env::var("NEBULA_FOLLOW_LEADER")
            .ok()
            .filter(|s| !s.is_empty());
        let peers = std::env::var("NEBULA_PEERS")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| Self::parse_peers(&s))
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            node_id,
            role,
            leader_url,
            peers,
        })
    }

    pub fn is_follower(&self) -> bool {
        matches!(self.role, NodeRole::Follower)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_role_forms() {
        assert_eq!("leader".parse::<NodeRole>().unwrap(), NodeRole::Leader);
        assert_eq!("FOLLOWER".parse::<NodeRole>().unwrap(), NodeRole::Follower);
        assert_eq!("".parse::<NodeRole>().unwrap(), NodeRole::Standalone);
        assert!("potato".parse::<NodeRole>().is_err());
    }

    #[test]
    fn parse_peers_ok() {
        let p =
            ClusterConfig::parse_peers("a=http://x:1,b=http://y:2, c=http://z:3").unwrap();
        assert_eq!(p.len(), 3);
        assert_eq!(p[0].id, "a");
        assert_eq!(p[2].base_url, "http://z:3");
    }

    #[test]
    fn parse_peers_empty_and_malformed() {
        assert!(ClusterConfig::parse_peers("").unwrap().is_empty());
        // Empty segments ignored.
        let p = ClusterConfig::parse_peers(",a=http://x,,b=http://y,").unwrap();
        assert_eq!(p.len(), 2);
        // Missing '=' is a hard error.
        assert!(ClusterConfig::parse_peers("a=http://x,b").is_err());
        // Empty url rejected.
        assert!(ClusterConfig::parse_peers("a=").is_err());
    }
}
