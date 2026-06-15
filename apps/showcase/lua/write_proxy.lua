-- content_by_lua handler that proxies a WRITE request to the current
-- leader and, on a follower-409 (leadership moved mid-flight), retries
-- the other node once within the SAME client request (design 0009 §7).
-- This is what turns a leadership handoff from a visible failed write
-- into a transparent one — nginx alone can't retry on a 409.

local router = require("leader_route")
local http = require("resty.http")

-- Read the full client request body so we can forward (and re-forward
-- on retry) it. Writes here are small JSON docs, so buffering is fine.
ngx.req.read_body()
local body = ngx.req.get_body_data() or ""
local method = ngx.req.get_method()
local uri = ngx.var.request_uri
local headers = ngx.req.get_headers()

-- Forward to one backend base URL. Returns the resty response or nil,err.
local function forward(base)
  local httpc = http.new()
  -- Generous timeouts: a write may queue behind WAL fsync, but we don't
  -- want to hang forever if a node is wedged. 15s read mirrors the old
  -- proxy_read_timeout for /api/.
  httpc:set_timeouts(2000, 10000, 15000) -- connect, send, read (ms)
  return httpc:request_uri(base .. uri, {
    method = method,
    body = body,
    headers = headers,
  })
end

-- A write attempt is "bad" (needs re-routing to the other node) when:
--   * the connection failed entirely (res == nil) — the node we picked
--     was fenced/stopped mid-handoff, or
--   * the node returned 409 read_only_follower (leadership moved), or
--   * the node returned 503 (it's recovering / not yet ready to serve).
-- All three are leadership/availability signals: re-resolve the leader
-- and try the other node. Writes are idempotent by id (upsert/delete),
-- so a retry that double-applies converges rather than duplicating.
local function needs_reroute(res)
  if not res then
    return true
  end
  return res.status == 409 or res.status == 503
end

-- Bounded retry loop across the (two) nodes. A leadership handoff during
-- a rolling upgrade fences the old leader (stop) BEFORE promoting the
-- standby, so for a few seconds NEITHER node is writable. Rather than
-- fail the write (forcing every client to implement retry), we hold and
-- retry here long enough to ride out a typical fence gap — the request
-- just takes a couple extra seconds. The total budget
-- (NEBULA_WRITE_RETRY_BUDGET_SECS, default 8s) stays under the client's
-- own request timeout; if it elapses we return a 503 + Retry-After so a
-- client that DOES retry still works.
local RETRY_BUDGET = tonumber(os.getenv("NEBULA_WRITE_RETRY_BUDGET_SECS")) or 8
local BACKOFF = 0.25
local target = router.leader_url(false)
local res, err
local deadline = ngx.now() + RETRY_BUDGET
local first = true
while true do
  if not first then
    -- Re-resolve the leader. Force a fresh probe so a stale cache entry
    -- pointing at the fenced node is discarded.
    router.invalidate()
    local next_target = router.leader_url(true)
    if next_target == target then
      next_target = router.other_node(target)
    end
    target = next_target
  end
  first = false

  res, err = forward(target)
  if not needs_reroute(res) then
    break
  end
  if ngx.now() >= deadline then
    break
  end
  ngx.sleep(BACKOFF)
end

-- Still not writable after all attempts (no leader right now): surface a
-- clean 503 + Retry-After rather than a 500 or the internal
-- read_only_follower body, so the client backs off and retries.
if needs_reroute(res) then
  ngx.status = 503
  ngx.header["Content-Type"] = "application/json"
  ngx.header["Retry-After"] = "2"
  ngx.say('{"error":"write temporarily unavailable, retry shortly"}')
  if not res then
    ngx.log(ngx.WARN, "write proxy: no writable node after retries: ", err or "unknown")
  end
  return
end

-- Pass the backend response straight through.
ngx.status = res.status
for k, v in pairs(res.headers) do
  -- Skip hop-by-hop / length headers ngx will set itself.
  local lk = k:lower()
  if lk ~= "connection" and lk ~= "transfer-encoding" and lk ~= "content-length" then
    ngx.header[k] = v
  end
end
if res.body then
  ngx.print(res.body)
end
