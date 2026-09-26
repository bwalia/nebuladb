//! Compatibility registry loader (YAML → typed catalog).

use serde::{Deserialize, Serialize};

/// How a capability is provided relative to the named product.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatStatus {
    Native,
    Compatible,
    Translated,
    External,
    Partial,
    Unsupported,
    AdapterBased,
    MigrationBased,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatCapability {
    pub id: String,
    pub name: String,
    pub status: CompatStatus,
    #[serde(default)]
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatProduct {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub capabilities: Vec<CompatCapability>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatRegistry {
    pub version: String,
    pub products: Vec<CompatProduct>,
}

impl CompatRegistry {
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        serde_yaml::from_str(yaml).map_err(|e| e.to_string())
    }

    pub fn product(&self, id: &str) -> Option<&CompatProduct> {
        self.products.iter().find(|p| p.id == id)
    }
}

/// Embedded default registry shipped with the crate. The server can
/// also load an override from `docs/compat/registry.yaml` at boot.
pub fn embedded_registry() -> CompatRegistry {
    CompatRegistry::from_yaml(include_str!("../registry.embedded.yaml"))
        .expect("embedded compat registry must parse")
}
