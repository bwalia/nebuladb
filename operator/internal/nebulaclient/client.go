// Package nebulaclient is a thin typed wrapper around NebulaDB's HTTP admin
// surface. Every method maps 1:1 to an endpoint documented in
// crates/nebula-server/src/router.rs. We intentionally keep this small and
// dependency-free — controller-runtime brings enough surface area already.
package nebulaclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultTimeout = 5 * time.Second
)

// Client talks to one NebulaDB pod's REST interface.
type Client struct {
	baseURL    string
	httpClient *http.Client
	bearer     string
}

// New constructs a Client. base should be the full scheme+host+port, e.g.
// "http://nebula-0.nebula-headless:8080". bearer may be empty to disable auth.
func New(base, bearer string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(base, "/"),
		httpClient: &http.Client{Timeout: defaultTimeout},
		bearer:     bearer,
	}
}

// WithTimeout returns a copy using a different per-request timeout.
func (c *Client) WithTimeout(d time.Duration) *Client {
	cp := *c
	cp.httpClient = &http.Client{Timeout: d}
	return &cp
}

// Health is the /healthz response body.
type Health struct {
	Status string `json:"status"`
	Docs   int64  `json:"docs"`
	Dim    int    `json:"dim"`
	Model  string `json:"model"`
}

// Healthz probes GET /healthz.
func (c *Client) Healthz(ctx context.Context) (*Health, error) {
	var out Health
	if err := c.do(ctx, http.MethodGet, "/healthz", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ReplicationStatus mirrors GET /api/v1/admin/replication.
type ReplicationStatus struct {
	Role                string `json:"role"`
	LocalNewest         *int64 `json:"local_newest,omitempty"`
	FollowerApplied     *int64 `json:"follower_applied,omitempty"`
	LeaderNewestProbed  *int64 `json:"leader_newest_probed,omitempty"`
	LagBytes            *int64 `json:"lag_bytes,omitempty"`
	Behind              bool   `json:"behind,omitempty"`
}

func (c *Client) Replication(ctx context.Context) (*ReplicationStatus, error) {
	var out ReplicationStatus
	if err := c.do(ctx, http.MethodGet, "/api/v1/admin/replication", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ClusterNode is one peer entry from GET /api/v1/admin/cluster/nodes.
type ClusterNode struct {
	ID      string `json:"id"`
	Role    string `json:"role"`
	URL     string `json:"url,omitempty"`
	Healthy bool   `json:"healthy"`
	Docs    int64  `json:"docs,omitempty"`
	ProbeMs int64  `json:"probe_ms,omitempty"`
}

type ClusterNodesResponse struct {
	Nodes []ClusterNode `json:"nodes"`
}

func (c *Client) ClusterNodes(ctx context.Context) (*ClusterNodesResponse, error) {
	var out ClusterNodesResponse
	if err := c.do(ctx, http.MethodGet, "/api/v1/admin/cluster/nodes", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SnapshotResponse is returned by POST /api/v1/admin/snapshot.
type SnapshotResponse struct {
	Path            string `json:"path"`
	WalSeqCaptured  int64  `json:"wal_seq_captured"`
}

func (c *Client) Snapshot(ctx context.Context) (*SnapshotResponse, error) {
	var out SnapshotResponse
	if err := c.do(ctx, http.MethodPost, "/api/v1/admin/snapshot", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CompactWAL calls POST /api/v1/admin/wal/compact. Response body is ignored.
func (c *Client) CompactWAL(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/api/v1/admin/wal/compact", nil, nil)
}

// EmptyBucket calls POST /api/v1/admin/bucket/:bucket/empty.
func (c *Client) EmptyBucket(ctx context.Context, bucket string) error {
	if bucket == "" {
		return errors.New("bucket required")
	}
	path := fmt.Sprintf("/api/v1/admin/bucket/%s/empty", url.PathEscape(bucket))
	return c.do(ctx, http.MethodPost, path, nil, nil)
}

// BucketStats is one entry from GET /api/v1/admin/buckets.
type BucketStats struct {
	Name        string   `json:"name"`
	Docs        int64    `json:"docs"`
	ParentDocs  int64    `json:"parent_docs"`
	TopKeys     []string `json:"top_keys,omitempty"`
}

type BucketsResponse struct {
	Buckets []BucketStats `json:"buckets"`
}

func (c *Client) Buckets(ctx context.Context, topKeys int) (*BucketsResponse, error) {
	q := ""
	if topKeys > 0 {
		q = fmt.Sprintf("?top_keys=%d", topKeys)
	}
	var out BucketsResponse
	if err := c.do(ctx, http.MethodGet, "/api/v1/admin/buckets"+q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// UpsertDoc is used by the bucket controller to seed a marker document.
// We deliberately keep a minimal shape — the controller only ever upserts
// a single known seed document.
type UpsertDocRequest struct {
	ID       string                 `json:"id"`
	Text     string                 `json:"text"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

func (c *Client) UpsertDoc(ctx context.Context, bucket string, req UpsertDocRequest) error {
	if bucket == "" {
		return errors.New("bucket required")
	}
	path := fmt.Sprintf("/api/v1/bucket/%s/doc", url.PathEscape(bucket))
	return c.do(ctx, http.MethodPost, path, req, nil)
}

// Durability is GET /api/v1/admin/durability — used to detect whether the
// leader is running with a persistent data directory before trying to snapshot.
type Durability struct {
	Persistent bool   `json:"persistent"`
	DataDir    string `json:"data_dir,omitempty"`
	WalNewest  *int64 `json:"wal_newest,omitempty"`
	WalOldest  *int64 `json:"wal_oldest,omitempty"`
}

func (c *Client) Durability(ctx context.Context) (*Durability, error) {
	var out Durability
	if err := c.do(ctx, http.MethodGet, "/api/v1/admin/durability", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// --- internal plumbing ---

func (c *Client) do(ctx context.Context, method, path string, body, out interface{}) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearer)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &HTTPError{Status: resp.StatusCode, Method: method, Path: path, Body: string(raw)}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// HTTPError surfaces a non-2xx response body verbatim for easier controller
// diagnostics. Useful since NebulaDB returns JSON { "error": "..." } bodies.
type HTTPError struct {
	Status int
	Method string
	Path   string
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("nebuladb %s %s: HTTP %d: %s", e.Method, e.Path, e.Status, e.Body)
}

// IsReadOnlyFollower returns true when NebulaDB's follower write guard rejects
// the request. Matches the 409 response from the guard middleware.
func IsReadOnlyFollower(err error) bool {
	var he *HTTPError
	if !errors.As(err, &he) {
		return false
	}
	return he.Status == http.StatusConflict && strings.Contains(he.Body, "read_only_follower")
}
