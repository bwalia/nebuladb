// check_website_live.go — find leads with a non-empty website, probe the
// first N URLs, and set metadata is_website_live=true when the site responds.
//
// Stdlib only. Usage:
//
//	export NEBULA_URL=https://showcase.nebuladb.net
//	export NEBULA_TOKEN=<api-key>
//	go run scripts/check_website_live.go
//
// Flags:
//
//	-limit 10          how many live sites to update (default 10)
//	-candidates 200    how many search hits to scan for a website field
//	-dry-run           probe only; do not upsert
//	-bucket leads      bucket name
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func main() {
	baseURL := envOr("NEBULA_URL", "https://showcase.nebuladb.net")
	token := os.Getenv("NEBULA_TOKEN")
	if token == "" {
		fatal("NEBULA_TOKEN is required (Bearer API key)")
	}

	limit := flag.Int("limit", 10, "max leads to mark is_website_live=true")
	candidates := flag.Int("candidates", 200, "search hits to scan for non-empty website")
	bucket := flag.String("bucket", "leads", "bucket name")
	dryRun := flag.Bool("dry-run", false, "probe only; skip upsert")
	flag.Parse()

	client := &http.Client{Timeout: 30 * time.Second}
	api := &nebulaAPI{base: strings.TrimRight(baseURL, "/"), token: token, http: client}

	fmt.Printf("scanning up to %d candidates in %q for non-empty website…\n", *candidates, *bucket)
	withSite, err := api.findLeadsWithWebsite(*bucket, *candidates)
	if err != nil {
		fatal(err.Error())
	}
	fmt.Printf("found %d lead(s) with website set\n", len(withSite))
	if len(withSite) == 0 {
		fmt.Println("nothing to do (corpus may not store a website metadata field yet)")
		return
	}

	probe := &http.Client{
		Timeout: 8 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	updated := 0
	for _, lead := range withSite {
		if updated >= *limit {
			break
		}
		live := websiteLive(probe, lead.Website)
		fmt.Printf("  %s  %s  live=%v\n", lead.ID, lead.Website, live)
		if !live {
			continue
		}
		if *dryRun {
			fmt.Printf("    dry-run: would set is_website_live=true\n")
			updated++
			continue
		}
		if err := api.setWebsiteLive(*bucket, lead.ID); err != nil {
			fmt.Printf("    upsert failed: %v\n", err)
			continue
		}
		fmt.Printf("    updated is_website_live=true\n")
		updated++
	}
	fmt.Printf("done: marked %d / %d live\n", updated, *limit)
}

type leadHit struct {
	ID       string
	Website  string
	Metadata map[string]any
}

type nebulaAPI struct {
	base  string
	token string
	http  *http.Client
}

func (a *nebulaAPI) findLeadsWithWebsite(bucket string, want int) ([]leadHit, error) {
	// SQL residual filters only support = / IN, so we pull a semantic
	// candidate set and filter non-empty website client-side.
	sql := fmt.Sprintf(
		`SELECT id, website, company_name FROM %s WHERE semantic_match(content, 'company business website') LIMIT %d`,
		bucket, want,
	)
	body, err := a.postJSON("/api/v1/query", map[string]any{"sql": sql})
	if err != nil {
		return nil, err
	}
	var resp struct {
		Rows []struct {
			ID     string         `json:"id"`
			Fields map[string]any `json:"fields"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode query: %w", err)
	}

	out := make([]leadHit, 0)
	seen := map[string]bool{}
	for _, row := range resp.Rows {
		site := stringField(row.Fields, "website")
		if site == "" || seen[row.ID] {
			continue
		}
		seen[row.ID] = true
		out = append(out, leadHit{ID: row.ID, Website: site, Metadata: row.Fields})
	}

	// Fallback: AI search (returns full metadata) in case SQL projection
	// omits sparse keys the same way.
	if len(out) == 0 {
		aiBody, err := a.postJSON("/api/v1/ai/search", map[string]any{
			"bucket": bucket,
			"query":  "company with website",
			"k":      want,
		})
		if err != nil {
			return nil, err
		}
		var ai struct {
			Hits []struct {
				ID       string         `json:"id"`
				Metadata map[string]any `json:"metadata"`
			} `json:"hits"`
		}
		if err := json.Unmarshal(aiBody, &ai); err != nil {
			return nil, fmt.Errorf("decode ai/search: %w", err)
		}
		for _, h := range ai.Hits {
			site := stringField(h.Metadata, "website")
			if site == "" || seen[h.ID] {
				continue
			}
			seen[h.ID] = true
			out = append(out, leadHit{ID: h.ID, Website: site, Metadata: h.Metadata})
		}
	}
	return out, nil
}

func (a *nebulaAPI) setWebsiteLive(bucket, id string) error {
	doc, err := a.getDoc(bucket, id)
	if err != nil {
		return err
	}
	meta := map[string]any{}
	if m, ok := doc["metadata"].(map[string]any); ok && m != nil {
		for k, v := range m {
			meta[k] = v
		}
	}
	meta["is_website_live"] = true

	text, _ := doc["text"].(string)
	_, err = a.postJSON("/api/v1/bucket/"+url.PathEscape(bucket)+"/doc", map[string]any{
		"id":       id,
		"text":     text,
		"metadata": meta,
	})
	return err
}

func (a *nebulaAPI) getDoc(bucket, id string) (map[string]any, error) {
	req, err := http.NewRequest(http.MethodGet,
		a.base+"/api/v1/bucket/"+url.PathEscape(bucket)+"/doc/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	res, err := a.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("GET doc %s: HTTP %d: %s", id, res.StatusCode, truncate(string(body), 200))
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (a *nebulaAPI) postJSON(path string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, a.base+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Content-Type", "application/json")
	res, err := a.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: HTTP %d: %s", path, res.StatusCode, truncate(string(body), 200))
	}
	return body, nil
}

func websiteLive(c *http.Client, raw string) bool {
	u := normalizeURL(raw)
	if u == "" {
		return false
	}
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		req, err := http.NewRequest(method, u, nil)
		if err != nil {
			return false
		}
		req.Header.Set("User-Agent", "nebuladb-website-check/1.0")
		res, err := c.Do(req)
		if err != nil {
			continue
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode > 0 && res.StatusCode < 500 {
			return true
		}
	}
	return false
}

func normalizeURL(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" || strings.EqualFold(s, "null") || s == "-" {
		return ""
	}
	if !strings.Contains(s, "://") {
		s = "https://" + s
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return ""
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return ""
	}
	return u.String()
}

func stringField(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		return strings.TrimSpace(fmt.Sprint(t))
	case json.Number:
		return strings.TrimSpace(t.String())
	default:
		s := strings.TrimSpace(fmt.Sprint(t))
		if s == "<nil>" || s == "null" {
			return ""
		}
		return s
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
