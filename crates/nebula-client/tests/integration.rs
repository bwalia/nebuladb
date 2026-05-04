//! Integration tests using axum-based mock servers to exercise the
//! client's home-region routing.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use hyper::StatusCode;
use nebula_client::Client;
use serde_json::json;

#[derive(Clone, Default)]
struct Shared {
    upserts: Arc<Mutex<Vec<(String, String)>>>,
}

/// A mock NebulaDB endpoint for one region. Returns a canned
/// home-region response (configurable per region) and accepts
/// writes only when the bucket's home == this region.
fn mock_app(my_region: String, homes: Arc<Mutex<Vec<(String, String)>>>, shared: Shared) -> Router {
    let homes_for_get = Arc::clone(&homes);
    let app = Router::new()
        .route(
            "/api/v1/admin/bucket/:bucket/home-region",
            get({
                let homes = homes_for_get;
                let my_region = my_region.clone();
                move |Path(bucket): Path<String>| {
                    let homes = Arc::clone(&homes);
                    let my_region = my_region.clone();
                    async move {
                        let g = homes.lock().unwrap();
                        let home = g.iter().find(|(b, _)| b == &bucket).map(|(_, r)| r.clone());
                        let has_home = home.is_some();
                        Json(json!({
                            "bucket": bucket,
                            "home_region": home,
                            "home_epoch": 1,
                            "replicated_to": [],
                            "node_region": my_region,
                            "has_home": has_home,
                        }))
                    }
                }
            }),
        )
        .route(
            "/api/v1/bucket/:bucket/doc",
            post({
                let homes = Arc::clone(&homes);
                let my_region = my_region.clone();
                move |Path(bucket): Path<String>, State(s): State<Shared>, Json(body): Json<serde_json::Value>| {
                    let homes = Arc::clone(&homes);
                    let my_region = my_region.clone();
                    async move {
                        let g = homes.lock().unwrap();
                        let home = g.iter().find(|(b, _)| b == &bucket).map(|(_, r)| r.clone());
                        drop(g);
                        if let Some(h) = home {
                            if h != my_region {
                                return (
                                    StatusCode::MISDIRECTED_REQUEST,
                                    Json(json!({
                                        "code": "wrong_home_region",
                                        "home_region": h,
                                        "home_epoch": 1,
                                        "node_region": my_region,
                                    })),
                                );
                            }
                        }
                        s.upserts.lock().unwrap().push((bucket.clone(), body["id"].as_str().unwrap_or("").to_string()));
                        (StatusCode::OK, Json(json!({"ok": true})))
                    }
                }
            }),
        )
        .with_state(shared);
    app
}

async fn spawn_mock(region: &str, homes: Arc<Mutex<Vec<(String, String)>>>, shared: Shared) -> String {
    let app = mock_app(region.to_string(), homes, shared);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    format!("http://{addr}")
}

/// Client hitting the preferred region for a bucket homed elsewhere
/// gets a 421, updates its cache, and retries transparently against
/// the correct region. After the retry the write lands on the home.
#[tokio::test]
async fn write_follows_server_supplied_home() {
    let homes = Arc::new(Mutex::new(vec![("catalog".to_string(), "us-east-1".to_string())]));
    let east_shared = Shared::default();
    let west_shared = Shared::default();
    let east_url = spawn_mock("us-east-1", Arc::clone(&homes), east_shared.clone()).await;
    let west_url = spawn_mock("us-west-2", Arc::clone(&homes), west_shared.clone()).await;

    let client = Client::builder()
        .region("us-east-1", east_url)
        .region("us-west-2", west_url)
        .prefer("us-west-2") // Prefer the WRONG region on first write.
        .build();

    client
        .upsert_doc("catalog", "sku-1", "hello", None)
        .await
        .expect("upsert should succeed after retry");

    // The east mock recorded the upsert; the west mock did not.
    let east_writes = east_shared.upserts.lock().unwrap().clone();
    let west_writes = west_shared.upserts.lock().unwrap().clone();
    assert_eq!(east_writes, vec![("catalog".to_string(), "sku-1".to_string())]);
    assert!(west_writes.is_empty(), "west should not have accepted");
}

/// Second write hits the cache and goes straight to the home region.
#[tokio::test]
async fn second_write_uses_cache() {
    let homes = Arc::new(Mutex::new(vec![("catalog".to_string(), "us-east-1".to_string())]));
    let east_shared = Shared::default();
    let west_shared = Shared::default();
    let east_url = spawn_mock("us-east-1", Arc::clone(&homes), east_shared.clone()).await;
    let west_url = spawn_mock("us-west-2", Arc::clone(&homes), west_shared.clone()).await;

    let client = Client::builder()
        .region("us-east-1", east_url)
        .region("us-west-2", west_url)
        .prefer("us-west-2")
        .build();
    // Warm the cache.
    client.upsert_doc("catalog", "sku-1", "one", None).await.unwrap();
    // This write should skip the west endpoint entirely.
    client.upsert_doc("catalog", "sku-2", "two", None).await.unwrap();

    let east_writes = east_shared.upserts.lock().unwrap().clone();
    let west_writes = west_shared.upserts.lock().unwrap().clone();
    assert_eq!(east_writes.len(), 2);
    assert!(west_writes.is_empty());
}

/// No-home bucket: the client goes to the preferred region without
/// any redirect dance.
#[tokio::test]
async fn no_home_means_preferred_region_serves_writes() {
    // Empty homes map.
    let homes = Arc::new(Mutex::new(vec![]));
    let east_shared = Shared::default();
    let east_url = spawn_mock("us-east-1", Arc::clone(&homes), east_shared.clone()).await;

    let client = Client::builder()
        .region("us-east-1", east_url)
        .build();
    client.upsert_doc("catalog", "sku-1", "ok", None).await.unwrap();
    assert_eq!(east_shared.upserts.lock().unwrap().len(), 1);
}
