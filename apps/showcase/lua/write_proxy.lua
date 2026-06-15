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

-- First attempt: the cached/current leader.
local target = router.leader_url(false)
local res, err = forward(target)

-- On a 409 the node we hit is a follower (leadership moved). Invalidate
-- the cache, re-resolve, and retry the OTHER node once. Writes are
-- idempotent by id (upsert/delete), so a retry is safe even if the first
-- attempt partially applied.
if res and res.status == 409 then
  router.invalidate()
  local retry_target = router.leader_url(true)
  -- If re-resolve returned the same node (both still followers in a
  -- brief window), try the explicit other node so we don't just repeat.
  if retry_target == target then
    retry_target = router.other_node(target)
  end
  res, err = forward(retry_target)
end

if not res then
  -- Connection-level failure to the backend. Surface a clean 503 +
  -- Retry-After so the client backs off, matching the nginx intercept
  -- behavior for reads.
  ngx.status = 503
  ngx.header["Content-Type"] = "application/json"
  ngx.header["Retry-After"] = "5"
  ngx.say('{"error":"service temporarily unavailable, retry shortly"}')
  ngx.log(ngx.WARN, "write proxy failed: ", err or "unknown")
  return
end

-- A still-409 after the retry (both nodes refused — no leader right now)
-- becomes a clean 503 + Retry-After rather than leaking the internal
-- read_only_follower body.
if res.status == 409 then
  ngx.status = 503
  ngx.header["Content-Type"] = "application/json"
  ngx.header["Retry-After"] = "5"
  ngx.say('{"error":"write temporarily unavailable, retry shortly"}')
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
