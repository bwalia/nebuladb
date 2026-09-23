// enrich_website_ch.go — tiny agent that:
//  1. Pulls top N leads from NebulaDB (company name + town)
//  2. Resolves them via the UK Companies House public API (name + number + locality)
//  3. Researches a likely public website (CH does not publish websites)
//  4. Writes metadata.website (+ company_number) back to NebulaDB when found
//
// Stdlib only. Free Companies House API key (Basic auth, key as username):
//   https://developer.company-information.service.gov.uk/
//
// Usage:
//
//	export NEBULA_URL=https://showcase.nebuladb.net
//	export NEBULA_TOKEN=<api-key>
//	export COMPANIES_HOUSE_API_KEY=<ch-api-key>
//	go run scripts/enrich_website_ch.go -limit 100 -dry-run
//	go run scripts/enrich_website_ch.go -limit 100
//
// Flags:
//
//	-limit 100         how many leads to process (default 100)
//	-bucket leads      bucket name
//	-dry-run           research only; do not upsert
//	-skip-existing     skip leads that already have website set (default true)
//	-query <text>      semantic seed query (default: UK companies businesses)
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"
)

const chAPI = "https://api.company-information.service.gov.uk"

func main() {
	baseURL := envOr("NEBULA_URL", "https://showcase.nebuladb.net")
	token := os.Getenv("NEBULA_TOKEN")
	chKey := os.Getenv("COMPANIES_HOUSE_API_KEY")
	if token == "" {
		fatal("NEBULA_TOKEN is required")
	}
	if chKey == "" {
		fatal("COMPANIES_HOUSE_API_KEY is required (https://developer.company-information.service.gov.uk/)")
	}

	limit := flag.Int("limit", 100, "how many leads to process")
	bucket := flag.String("bucket", "leads", "bucket name")
	dryRun := flag.Bool("dry-run", false, "research only; skip upsert")
	skipExisting := flag.Bool("skip-existing", true, "skip leads that already have website")
	seedQuery := flag.String("query", "UK companies businesses Limited Ltd", "semantic seed query")
	flag.Parse()

	httpClient := &http.Client{Timeout: 30 * time.Second}
	probe := &http.Client{
		Timeout: 8 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	api := &nebulaAPI{base: strings.TrimRight(baseURL, "/"), token: token, http: httpClient}
	ch := &companiesHouse{key: chKey, http: httpClient}

	fmt.Printf("fetching up to %d leads from %q…\n", *limit, *bucket)
	leads, err := api.fetchLeads(*bucket, *seedQuery, *limit)
	if err != nil {
		fatal(err.Error())
	}
	fmt.Printf("got %d lead(s)\n", len(leads))

	saved, skipped, noWeb, failed := 0, 0, 0, 0
	for i, lead := range leads {
		fmt.Printf("\n[%d/%d] %s  city=%q\n", i+1, len(leads), lead.CompanyName, lead.City)
		if *skipExisting && strings.TrimSpace(lead.Website) != "" {
			fmt.Printf("  skip: website already set (%s)\n", lead.Website)
			skipped++
			continue
		}
		if strings.TrimSpace(lead.CompanyName) == "" {
			fmt.Printf("  skip: empty company_name\n")
			skipped++
			continue
		}

		match, err := ch.findCompany(lead.CompanyName, lead.City)
		if err != nil {
			fmt.Printf("  companies house: %v\n", err)
			failed++
			continue
		}
		if match == nil {
			fmt.Printf("  companies house: no confident match\n")
			failed++
			continue
		}
		fmt.Printf("  CH match: %s (%s) status=%s locality=%s\n",
			match.Title, match.CompanyNumber, match.CompanyStatus, match.Address.Locality)

		site, how := researchWebsite(probe, lead, match)
		if site == "" {
			fmt.Printf("  no website found\n")
			noWeb++
			// Still persist company_number when we have a CH match.
			if !*dryRun {
				if err := api.enrichLead(*bucket, lead.ID, match.CompanyNumber, "", how); err != nil {
					fmt.Printf("  upsert company_number failed: %v\n", err)
				} else {
					fmt.Printf("  saved company_number only\n")
				}
			}
			continue
		}
		fmt.Printf("  website=%s (%s)\n", site, how)

		if *dryRun {
			fmt.Printf("  dry-run: would save website + company_number\n")
			saved++
			continue
		}
		if err := api.enrichLead(*bucket, lead.ID, match.CompanyNumber, site, how); err != nil {
			fmt.Printf("  upsert failed: %v\n", err)
			failed++
			continue
		}
		fmt.Printf("  saved website + company_number\n")
		saved++

		// Be polite to CH + search endpoints.
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("\ndone: saved=%d skipped=%d no_website=%d failed=%d\n", saved, skipped, noWeb, failed)
}

// ---------------------------------------------------------------------------
// NebulaDB
// ---------------------------------------------------------------------------

type lead struct {
	ID          string
	CompanyName string
	City        string
	Email       string
	Website     string
	Text        string
}

type nebulaAPI struct {
	base  string
	token string
	http  *http.Client
}

func (a *nebulaAPI) fetchLeads(bucket, query string, limit int) ([]lead, error) {
	sql := fmt.Sprintf(
		`SELECT id, company_name, city, email, website FROM %s WHERE semantic_match(content, '%s') LIMIT %d`,
		bucket, escapeSQL(query), limit,
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
	out := make([]lead, 0, len(resp.Rows))
	seen := map[string]bool{}
	for _, row := range resp.Rows {
		if seen[row.ID] {
			continue
		}
		seen[row.ID] = true
		out = append(out, lead{
			ID:          row.ID,
			CompanyName: stringField(row.Fields, "company_name"),
			City:        stringField(row.Fields, "city"),
			Email:       stringField(row.Fields, "email"),
			Website:     stringField(row.Fields, "website"),
		})
	}
	return out, nil
}

func (a *nebulaAPI) enrichLead(bucket, id, companyNumber, website, how string) error {
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
	if companyNumber != "" {
		meta["company_number"] = companyNumber
	}
	if website != "" {
		meta["website"] = website
		meta["website_source"] = how
		meta["website_enriched_at"] = time.Now().UTC().Format(time.RFC3339)
	}
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

// ---------------------------------------------------------------------------
// Companies House
// ---------------------------------------------------------------------------

type companiesHouse struct {
	key  string
	http *http.Client
}

type chSearchItem struct {
	CompanyNumber string `json:"company_number"`
	Title         string `json:"title"`
	CompanyStatus string `json:"company_status"`
	CompanyType   string `json:"company_type"`
	Address       struct {
		AddressLine1 string `json:"address_line_1"`
		Locality     string `json:"locality"`
		PostalCode   string `json:"postal_code"`
		Region       string `json:"region"`
	} `json:"address"`
	AddressSnippet string `json:"address_snippet"`
}

func (c *companiesHouse) findCompany(name, town string) (*chSearchItem, error) {
	q := url.Values{}
	q.Set("q", name)
	q.Set("items_per_page", "10")
	body, err := c.get("/search/companies?" + q.Encode())
	if err != nil {
		return nil, err
	}
	var resp struct {
		Items []chSearchItem `json:"items"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if len(resp.Items) == 0 {
		return nil, nil
	}

	normName := normalizeName(name)
	normTown := strings.ToLower(strings.TrimSpace(town))

	var best *chSearchItem
	bestScore := -1
	for i := range resp.Items {
		it := &resp.Items[i]
		if it.CompanyStatus != "" && it.CompanyStatus != "active" {
			continue
		}
		score := 0
		nTitle := normalizeName(it.Title)
		if nTitle == normName {
			score += 50
		} else if strings.Contains(nTitle, normName) || strings.Contains(normName, nTitle) {
			score += 25
		} else {
			// weak token overlap
			score += tokenOverlap(normName, nTitle) * 5
		}
		loc := strings.ToLower(it.Address.Locality)
		if normTown != "" && loc != "" {
			if loc == normTown || strings.Contains(loc, normTown) || strings.Contains(normTown, loc) {
				score += 30
			} else if strings.Contains(strings.ToLower(it.AddressSnippet), normTown) {
				score += 15
			}
		}
		if score > bestScore {
			bestScore = score
			best = it
		}
	}
	// Require a minimum confidence so we don't attach the wrong company.
	if best == nil || bestScore < 25 {
		return nil, nil
	}
	return best, nil
}

func (c *companiesHouse) get(path string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, chAPI+path, nil)
	if err != nil {
		return nil, err
	}
	// CH Basic auth: API key as username, blank password.
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(c.key+":")))
	req.Header.Set("Accept", "application/json")
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode == 429 {
		return nil, fmt.Errorf("rate limited by Companies House")
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("CH %s: HTTP %d: %s", path, res.StatusCode, truncate(string(body), 200))
	}
	return body, nil
}

// ---------------------------------------------------------------------------
// Website research
// Companies House does not publish company websites. We:
//  1. Prefer a non-placeholder email domain from the lead
//  2. Else DuckDuckGo HTML search for an official site, then liveness-check
// ---------------------------------------------------------------------------

var (
	reHref    = regexp.MustCompile(`(?i)href="(https?://[^"]+)"`)
	reUDDG    = regexp.MustCompile(`uddg=([^&"]+)`)
	junkHost  = regexp.MustCompile(`(?i)(wikipedia\.|linkedin\.|facebook\.|twitter\.|x\.com|instagram\.|youtube\.|companies-house|company-information\.service\.gov|endole\.|creditsafe\.|duedil\.|beauhurst\.|yell\.com|thomsonlocal|gumtree|amazon\.|ebay\.|google\.|bing\.|duckduckgo\.)`)
	emailJunk = regexp.MustCompile(`(?i)@(workstation\.co\.uk|gmail\.|yahoo\.|hotmail\.|outlook\.|icloud\.|aol\.|mail\.com|protonmail\.)`)
)

func researchWebsite(probe *http.Client, lead lead, ch *chSearchItem) (site, how string) {
	if w := websiteFromEmail(lead.Email); w != "" && websiteLive(probe, w) {
		return w, "email_domain"
	}

	queries := []string{
		fmt.Sprintf(`"%s" %s official website`, lead.CompanyName, lead.City),
		fmt.Sprintf(`"%s" %s website`, ch.Title, ch.Address.Locality),
		fmt.Sprintf("%s UK company website", lead.CompanyName),
	}
	seen := map[string]bool{}
	for _, q := range queries {
		for _, cand := range ddgCandidates(q) {
			host := hostOf(cand)
			if host == "" || seen[host] || junkHost.MatchString(host) {
				continue
			}
			seen[host] = true
			if !websiteLive(probe, cand) {
				continue
			}
			return normalizeURL(cand), "web_search"
		}
	}
	return "", ""
}

func websiteFromEmail(email string) string {
	email = strings.TrimSpace(strings.ToLower(email))
	if email == "" || !strings.Contains(email, "@") || emailJunk.MatchString(email) {
		return ""
	}
	parts := strings.Split(email, "@")
	if len(parts) != 2 || parts[1] == "" {
		return ""
	}
	return "https://" + parts[1]
}

func ddgCandidates(query string) []string {
	u := "https://html.duckduckgo.com/html/?q=" + url.QueryEscape(query)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", "nebuladb-ch-enrich/1.0")
	client := &http.Client{Timeout: 15 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	html := string(body)

	var out []string
	seen := map[string]bool{}
	add := func(raw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		if m := reUDDG.FindStringSubmatch(raw); len(m) == 2 {
			if dec, err := url.QueryUnescape(m[1]); err == nil {
				raw = dec
			}
		}
		n := normalizeURL(raw)
		if n == "" || seen[n] {
			return
		}
		seen[n] = true
		out = append(out, n)
	}
	for _, m := range reHref.FindAllStringSubmatch(html, -1) {
		add(m[1])
		if len(out) >= 12 {
			break
		}
	}
	return out
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
		req.Header.Set("User-Agent", "nebuladb-ch-enrich/1.0")
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
	// Drop tracking paths from DDG redirects.
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func hostOf(raw string) string {
	u, err := url.Parse(normalizeURL(raw))
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func normalizeName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	repl := []string{" limited", " ltd.", " ltd", " llp", " plc", " cic", " &", " and"}
	for _, r := range repl {
		s = strings.ReplaceAll(s, r, " ")
	}
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == ' ' {
			b.WriteRune(r)
		}
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

func tokenOverlap(a, b string) int {
	as := map[string]bool{}
	for _, t := range strings.Fields(a) {
		if len(t) > 2 {
			as[t] = true
		}
	}
	n := 0
	for _, t := range strings.Fields(b) {
		if as[t] {
			n++
		}
	}
	return n
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
	default:
		s := strings.TrimSpace(fmt.Sprint(t))
		if s == "<nil>" || s == "null" {
			return ""
		}
		return s
	}
}

func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
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
