// reingest_bucket.go — high-throughput reingest of NebulaDB documents into a
// new bucket so they are re-embedded with the currently configured embedder
// (e.g. after switching mock-384 → Ollama nomic-embed-text @ 768-d).
//
// Why Go: stdlib-only, cheap goroutines, HTTP connection pooling — best fit
// for multi-million doc copy over the REST API without extra deps.
//
// IMPORTANT: do NOT use /admin/bucket/:b/import for model changes — that
// path reuses stored vectors. This tool GETs text+metadata and POSTs
// /docs/bulk so the server embeds afresh.
//
// Full admin export of ~3M docs will OOM the server (builds one giant JSON).
// Default source path scans zero-padded numeric ids (matches prod leads).
//
// Usage:
//
//	export NEBULA_URL=https://showcase.nebuladb.net
//	export NEBULA_TOKEN=<api-key>
//
//	# 1) Dump leads → local JSONL (resumable, reusable)
//	go run scripts/reingest_bucket.go dump \
//	  -from-bucket leads -out /data/leads.jsonl \
//	  -id-min 1 -id-max 40000000 -id-width 8 -workers 64
//
//	# 2) Reingest JSONL → uk_companies (re-embeds)
//	go run scripts/reingest_bucket.go load \
//	  -to-bucket uk_companies -in /data/leads.jsonl \
//	  -workers 8 -batch 64
//
//	# Or one-shot mirror (scan + upsert, optional JSONL tee):
//	go run scripts/reingest_bucket.go mirror \
//	  -from-bucket leads -to-bucket uk_companies \
//	  -id-min 1 -id-max 40000000 -id-width 8 \
//	  -workers 32 -batch 64 -tee /data/leads.jsonl
//
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const bulkMax = 1000

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	switch cmd {
	case "dump":
		cmdDump(args)
	case "load":
		cmdLoad(args)
	case "mirror":
		cmdMirror(args)
	case "-h", "help", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage:
  go run scripts/reingest_bucket.go dump   [flags]   # leads → JSONL via id scan
  go run scripts/reingest_bucket.go load   [flags]   # JSONL → bucket (re-embed)
  go run scripts/reingest_bucket.go mirror [flags]   # leads → uk_companies (+ optional tee)

env: NEBULA_URL (default https://showcase.nebuladb.net), NEBULA_TOKEN (required)
`)
}

// ---------------------------------------------------------------------------
// shared client
// ---------------------------------------------------------------------------

type nebula struct {
	base  string
	token string
	http  *http.Client
}

func newNebula() *nebula {
	token := os.Getenv("NEBULA_TOKEN")
	if token == "" {
		fatal("NEBULA_TOKEN is required")
	}
	base := envOr("NEBULA_URL", "https://showcase.nebuladb.net")
	tr := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 128,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	return &nebula{
		base:  strings.TrimRight(base, "/"),
		token: token,
		http:  &http.Client{Timeout: 120 * time.Second, Transport: tr},
	}
}

type docOut struct {
	ID       string          `json:"id"`
	Text     string          `json:"text"`
	Metadata json.RawMessage `json:"metadata"`
}

func (n *nebula) getDoc(bucket, id string) (*docOut, error) {
	req, err := http.NewRequest(http.MethodGet,
		n.base+"/api/v1/bucket/"+url.PathEscape(bucket)+"/doc/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+n.token)
	res, err := n.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if res.StatusCode == http.StatusNotFound {
		return nil, errNotFound
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s/%s: HTTP %d: %s", bucket, id, res.StatusCode, truncate(string(body), 160))
	}
	var raw struct {
		ID       string          `json:"id"`
		Text     string          `json:"text"`
		Metadata json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	if strings.TrimSpace(raw.Text) == "" {
		return nil, errEmptyText
	}
	meta := raw.Metadata
	if len(meta) == 0 {
		meta = json.RawMessage(`{}`)
	}
	return &docOut{ID: raw.ID, Text: raw.Text, Metadata: meta}, nil
}

func (n *nebula) upsertBulk(bucket string, items []docOut) (inserted int, err error) {
	if len(items) == 0 {
		return 0, nil
	}
	if len(items) > bulkMax {
		return 0, fmt.Errorf("batch %d exceeds max %d", len(items), bulkMax)
	}
	payload := map[string]any{"items": make([]map[string]any, 0, len(items))}
	arr := payload["items"].([]map[string]any)
	for _, it := range items {
		var meta any = map[string]any{}
		if len(it.Metadata) > 0 {
			_ = json.Unmarshal(it.Metadata, &meta)
		}
		arr = append(arr, map[string]any{
			"id":       it.ID,
			"text":     it.Text,
			"metadata": meta,
		})
	}
	payload["items"] = arr
	raw, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost,
		n.base+"/api/v1/bucket/"+url.PathEscape(bucket)+"/docs/bulk", bytes.NewReader(raw))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+n.token)
	req.Header.Set("Content-Type", "application/json")
	res, err := n.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode >= 300 {
		return 0, fmt.Errorf("bulk %s: HTTP %d: %s", bucket, res.StatusCode, truncate(string(body), 200))
	}
	var out struct {
		Inserted int `json:"inserted"`
	}
	_ = json.Unmarshal(body, &out)
	return out.Inserted, nil
}

var (
	errNotFound  = fmt.Errorf("not found")
	errEmptyText = fmt.Errorf("empty text")
)

// ---------------------------------------------------------------------------
// dump — scan ids → JSONL
// ---------------------------------------------------------------------------

func cmdDump(args []string) {
	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	from := fs.String("from-bucket", "leads", "source bucket")
	outPath := fs.String("out", "leads.jsonl", "output JSONL path")
	idMin := fs.Int64("id-min", 1, "numeric id scan start")
	idMax := fs.Int64("id-max", 40_000_000, "numeric id scan end (inclusive)")
	idWidth := fs.Int("id-width", 8, "zero-pad width for numeric ids")
	workers := fs.Int("workers", 64, "concurrent GET workers")
	extra := fs.String("extra-ids", "demo-web-001,demo-web-002,demo-web-003,demo-web-004", "comma-separated extra ids")
	checkpoint := fs.String("checkpoint", "", "resume file (stores last completed numeric id)")
	_ = fs.Parse(args)

	n := newNebula()
	start := *idMin
	if *checkpoint != "" {
		if b, err := os.ReadFile(*checkpoint); err == nil {
			if v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil && v >= start {
				start = v + 1
				fmt.Printf("resuming numeric scan at %d\n", start)
			}
		}
	}

	f, err := os.OpenFile(*outPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fatal(err.Error())
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 1<<20)
	defer bw.Flush()
	var writeMu sync.Mutex

	ids := make(chan string, *workers*4)
	var found, miss, errors atomic.Int64
	var wg sync.WaitGroup
	t0 := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range ids {
				doc, err := n.getDoc(*from, id)
				if err == errNotFound || err == errEmptyText {
					miss.Add(1)
					continue
				}
				if err != nil {
					errors.Add(1)
					fmt.Fprintf(os.Stderr, "get %s: %v\n", id, err)
					continue
				}
				line, _ := json.Marshal(doc)
				writeMu.Lock()
				bw.Write(line)
				bw.WriteByte('\n')
				writeMu.Unlock()
				found.Add(1)
			}
		}()
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			fnd, m, e := found.Load(), miss.Load(), errors.Load()
			elapsed := time.Since(t0).Seconds()
			rate := float64(fnd+m) / elapsed
			fmt.Printf("progress found=%d miss=%d err=%d rate=%.0f ids/s elapsed=%s\n",
				fnd, m, e, rate, time.Since(t0).Truncate(time.Second))
			_ = bw.Flush()
		}
	}()

	for _, id := range splitCSV(*extra) {
		ids <- id
	}
	for i := start; i <= *idMax; i++ {
		ids <- fmt.Sprintf("%0*d", *idWidth, i)
		if *checkpoint != "" && i%10000 == 0 {
			_ = os.WriteFile(*checkpoint, []byte(strconv.FormatInt(i, 10)), 0o644)
		}
	}
	close(ids)
	wg.Wait()
	_ = bw.Flush()
	if *checkpoint != "" {
		_ = os.WriteFile(*checkpoint, []byte(strconv.FormatInt(*idMax, 10)), 0o644)
	}
	fmt.Printf("dump done found=%d miss=%d err=%d out=%s took=%s\n",
		found.Load(), miss.Load(), errors.Load(), *outPath, time.Since(t0).Truncate(time.Second))
}

// ---------------------------------------------------------------------------
// load — JSONL → bucket (re-embed via bulk upsert)
// ---------------------------------------------------------------------------

func cmdLoad(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	to := fs.String("to-bucket", "uk_companies", "destination bucket")
	inPath := fs.String("in", "leads.jsonl", "input JSONL")
	workers := fs.Int("workers", 8, "concurrent bulk upsert workers")
	batch := fs.Int("batch", 64, "docs per bulk request (max 1000; keep modest for Ollama)")
	skipExisting := fs.Bool("skip-existing", false, "HEAD/GET dest first and skip if present")
	_ = fs.Parse(args)
	if *batch < 1 {
		*batch = 1
	}
	if *batch > bulkMax {
		*batch = bulkMax
	}

	n := newNebula()
	f, err := os.Open(*inPath)
	if err != nil {
		fatal(err.Error())
	}
	defer f.Close()

	jobs := make(chan []docOut, *workers*2)
	var inserted, batches, errors atomic.Int64
	var wg sync.WaitGroup
	t0 := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batchItems := range jobs {
				if *skipExisting {
					filtered := batchItems[:0]
					for _, it := range batchItems {
						if _, err := n.getDoc(*to, it.ID); err == errNotFound {
							filtered = append(filtered, it)
						}
					}
					batchItems = filtered
					if len(batchItems) == 0 {
						continue
					}
				}
				ins, err := n.upsertBulk(*to, batchItems)
				if err != nil {
					errors.Add(1)
					fmt.Fprintf(os.Stderr, "bulk: %v\n", err)
					continue
				}
				inserted.Add(int64(ins))
				batches.Add(1)
			}
		}()
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			ins := inserted.Load()
			elapsed := time.Since(t0).Seconds()
			fmt.Printf("progress inserted=%d batches=%d err=%d rate=%.1f docs/s elapsed=%s\n",
				ins, batches.Load(), errors.Load(), float64(ins)/elapsed, time.Since(t0).Truncate(time.Second))
		}
	}()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 16<<20)
	buf := make([]docOut, 0, *batch)
	lines := 0
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var d docOut
		if err := json.Unmarshal(line, &d); err != nil {
			fmt.Fprintf(os.Stderr, "bad jsonl line %d: %v\n", lines+1, err)
			continue
		}
		if d.ID == "" || strings.TrimSpace(d.Text) == "" {
			continue
		}
		buf = append(buf, d)
		lines++
		if len(buf) >= *batch {
			jobs <- buf
			buf = make([]docOut, 0, *batch)
		}
	}
	if len(buf) > 0 {
		jobs <- buf
	}
	close(jobs)
	wg.Wait()
	fmt.Printf("load done inserted=%d batches=%d err=%d lines=%d took=%s → bucket %s\n",
		inserted.Load(), batches.Load(), errors.Load(), lines, time.Since(t0).Truncate(time.Second), *to)
}

// ---------------------------------------------------------------------------
// mirror — scan + upsert (+ optional JSONL tee)
// ---------------------------------------------------------------------------

func cmdMirror(args []string) {
	fs := flag.NewFlagSet("mirror", flag.ExitOnError)
	from := fs.String("from-bucket", "leads", "source bucket")
	to := fs.String("to-bucket", "uk_companies", "destination bucket")
	idMin := fs.Int64("id-min", 1, "numeric id scan start")
	idMax := fs.Int64("id-max", 40_000_000, "numeric id scan end")
	idWidth := fs.Int("id-width", 8, "zero-pad width")
	workers := fs.Int("workers", 32, "GET+queue workers")
	batch := fs.Int("batch", 64, "bulk upsert size")
	teePath := fs.String("tee", "", "optional JSONL tee path")
	extra := fs.String("extra-ids", "demo-web-001,demo-web-002,demo-web-003,demo-web-004", "extra ids")
	upsertWorkers := fs.Int("upsert-workers", 8, "bulk upsert workers")
	_ = fs.Parse(args)
	if *batch < 1 {
		*batch = 1
	}
	if *batch > bulkMax {
		*batch = bulkMax
	}

	n := newNebula()
	docsCh := make(chan docOut, *batch**upsertWorkers*2)
	ids := make(chan string, *workers*4)

	var teeMu sync.Mutex
	var tee *bufio.Writer
	if *teePath != "" {
		f, err := os.OpenFile(*teePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			fatal(err.Error())
		}
		defer f.Close()
		tee = bufio.NewWriterSize(f, 1<<20)
		defer tee.Flush()
	}

	var found, miss, inserted, errors atomic.Int64
	t0 := time.Now()
	var wgGet, wgUp sync.WaitGroup

	// Upsert workers
	for i := 0; i < *upsertWorkers; i++ {
		wgUp.Add(1)
		go func() {
			defer wgUp.Done()
			buf := make([]docOut, 0, *batch)
			flush := func() {
				if len(buf) == 0 {
					return
				}
				ins, err := n.upsertBulk(*to, buf)
				if err != nil {
					errors.Add(1)
					fmt.Fprintf(os.Stderr, "bulk: %v\n", err)
				} else {
					inserted.Add(int64(ins))
				}
				buf = buf[:0]
			}
			for d := range docsCh {
				buf = append(buf, d)
				if len(buf) >= *batch {
					flush()
				}
			}
			flush()
		}()
	}

	// GET workers
	for i := 0; i < *workers; i++ {
		wgGet.Add(1)
		go func() {
			defer wgGet.Done()
			for id := range ids {
				doc, err := n.getDoc(*from, id)
				if err == errNotFound || err == errEmptyText {
					miss.Add(1)
					continue
				}
				if err != nil {
					errors.Add(1)
					fmt.Fprintf(os.Stderr, "get %s: %v\n", id, err)
					continue
				}
				if tee != nil {
					line, _ := json.Marshal(doc)
					teeMu.Lock()
					tee.Write(line)
					tee.WriteByte('\n')
					teeMu.Unlock()
				}
				found.Add(1)
				docsCh <- *doc
			}
		}()
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			fmt.Printf("progress found=%d inserted=%d miss=%d err=%d elapsed=%s\n",
				found.Load(), inserted.Load(), miss.Load(), errors.Load(),
				time.Since(t0).Truncate(time.Second))
		}
	}()

	for _, id := range splitCSV(*extra) {
		ids <- id
	}
	for i := *idMin; i <= *idMax; i++ {
		ids <- fmt.Sprintf("%0*d", *idWidth, i)
	}
	close(ids)
	wgGet.Wait()
	close(docsCh)
	wgUp.Wait()
	fmt.Printf("mirror done found=%d inserted=%d miss=%d err=%d took=%s → %s\n",
		found.Load(), inserted.Load(), miss.Load(), errors.Load(),
		time.Since(t0).Truncate(time.Second), *to)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func splitCSV(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
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
