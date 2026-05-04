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

use serde::{Deserialize, Serialize};

// NodeRole moved to `nebula-core` so nebula-grpc and nebula-pgwire can
// refuse writes on followers without depending on this crate.
pub use nebula_core::NodeRole;

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

/// One cross-region peer the local process should tail WAL from.
///
/// Distinct from `PeerInfo` because cross-region uses gRPC (not REST)
/// and carries a region name — the operator wires one entry per remote
/// region, and the consumer task uses `region` to decide whether an
/// incoming record is "for a bucket I own" (reject) or "from a remote
/// home" (apply locally).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossRegionPeer {
    /// Remote region's canonical name, e.g. `us-west-2`.
    pub region: String,
    /// gRPC URL of the remote region's leader, e.g.
    /// `http://nebula-prod-us-west-2-leader:50051`.
    pub grpc_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ClusterConfig {
    pub node_id: Option<String>,
    pub role: NodeRole,
    /// Canonical region name for this process. `None` means the node
    /// is not participating in multi-region routing — writes land in
    /// whatever region the client hits. Set via `NEBULA_REGION`.
    pub region: Option<String>,
    /// Leader's gRPC URL when `role == Follower`. Mirrored from
    /// `NEBULA_FOLLOW_LEADER` so the cluster view can show which
    /// leader a follower is tailing without the operator having to
    /// correlate two env vars.
    pub leader_url: Option<String>,
    pub peers: Vec<PeerInfo>,
    /// Cross-region peers this node should tail. Only populated on
    /// leader pods — followers always tail their local leader, not
    /// remote regions.
    pub cross_region_peers: Vec<CrossRegionPeer>,
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

    /// Parse a cross-region peers env value: `region=grpc_url,region2=url2`.
    /// Shares the `id=value` form with `parse_peers` but validates a
    /// gRPC-looking scheme so typos surface at boot.
    pub fn parse_cross_region(s: &str) -> Result<Vec<CrossRegionPeer>, String> {
        let mut out = Vec::new();
        for raw in s.split(',') {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            let (region, url) = raw
                .split_once('=')
                .ok_or_else(|| format!("cross-region entry missing '=' in: {raw}"))?;
            let region = region.trim();
            let url = url.trim();
            if region.is_empty() || url.is_empty() {
                return Err(format!(
                    "cross-region entry has empty region or url: {raw}"
                ));
            }
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(format!(
                    "cross-region url must start with http:// or https:// — got {url}"
                ));
            }
            out.push(CrossRegionPeer {
                region: region.to_string(),
                grpc_url: url.to_string(),
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
        let region = std::env::var("NEBULA_REGION")
            .ok()
            .filter(|s| !s.is_empty());
        let leader_url = std::env::var("NEBULA_FOLLOW_LEADER")
            .ok()
            .filter(|s| !s.is_empty());
        let peers = std::env::var("NEBULA_PEERS")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| Self::parse_peers(&s))
            .transpose()?
            .unwrap_or_default();
        let cross_region_peers = std::env::var("NEBULA_CROSS_REGION_PEERS")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| Self::parse_cross_region(&s))
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            node_id,
            role,
            region,
            leader_url,
            peers,
            cross_region_peers,
        })
    }

    pub fn is_follower(&self) -> bool {
        matches!(self.role, NodeRole::Follower)
    }

    /// This node's canonical region, or `"default"` if multi-region
    /// is not configured. The fallback exists so single-region
    /// deployments don't need any new env vars.
    pub fn region_or_default(&self) -> &str {
        self.region.as_deref().unwrap_or("default")
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

    #[test]
    fn parse_cross_region_happy_path() {
        let p = ClusterConfig::parse_cross_region(
            "us-west-2=http://nebula-us-west-2:50051,eu-west-1=http://nebula-eu-west-1:50051",
        )
        .unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!(p[0].region, "us-west-2");
        assert_eq!(p[1].grpc_url, "http://nebula-eu-west-1:50051");
    }

    #[test]
    fn parse_cross_region_rejects_non_http_scheme() {
        // https:// is fine, ftp:// or a bare host is not.
        assert!(ClusterConfig::parse_cross_region("us-east-1=https://a:50051").is_ok());
        assert!(ClusterConfig::parse_cross_region("us-east-1=ftp://a").is_err());
        assert!(ClusterConfig::parse_cross_region("us-east-1=nebula-a:50051").is_err());
    }

    #[test]
    fn region_or_default_falls_back() {
        let mut c = ClusterConfig::default();
        assert_eq!(c.region_or_default(), "default");
        c.region = Some("us-east-1".into());
        assert_eq!(c.region_or_default(), "us-east-1");
    }
}
