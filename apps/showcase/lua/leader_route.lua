-- Leader-aware write routing for the NebulaDB showcase edge
-- (design 0009 §7). OpenResty Lua so the edge follows leadership at
-- request time WITHOUT an nginx reload: the orchestrator just fences +
-- promotes for correctness, and this module routes writes to whichever
-- node currently reports `role=leader`.
--
-- Why Lua instead of a static weighted upstream: a static `weight=100`
-- on nebula-server sends writes there even when leadership has moved to
-- the follower (a node that's up-but-follower 409s the write). nginx
-- can only fail over on connection-error/5xx, never on the 409. Here we
-- (a) pick the current leader, and (b) on a 409 re-resolve and retry the
-- OTHER node within the same client request — so a leadership handoff
-- doesn't surface as a failed write.
--
-- The leader identity is cached in a shared dict with a short TTL so the
-- steady-state cost is one dict lookup per write; only on miss/expiry or
-- a 409 do we probe /healthz/role.

local _M = {}

-- Candidate backends. Order is irrelevant — we probe both and pick the
-- one reporting leader. Service names resolve via docker DNS.
local NODES = {
  "http://nebula-server:8080",
  "http://nebula-follower:8080",
}

local CACHE = ngx.shared.leader_cache
local CACHE_KEY = "leader_url"
local CACHE_TTL = tonumber(os.getenv("NEBULA_LEADER_CACHE_TTL")) or 2 -- seconds

-- Probe one node's cheap /healthz/role (no network fan-out server-side).
-- Returns the role string ("leader"/"follower"/"standalone") or nil.
local function probe_role(base)
  local httpc = require("resty.http").new()
  httpc:set_timeout(800) -- ms; short so a dead node doesn't stall routing
  local res, err = httpc:request_uri(base .. "/healthz/role", { method = "GET" })
  if not res then
    return nil, err
  end
  if res.status ~= 200 then
    return nil, "status " .. res.status
  end
  -- Body is {"role":"leader"} — a substring match avoids a JSON dep.
  local body = res.body or ""
  local role = body:match('"role"%s*:%s*"(%a+)"')
  return role
end

-- Resolve the current leader URL, probing nodes. Returns a URL or nil.
local function resolve_leader()
  for _, base in ipairs(NODES) do
    local role = probe_role(base)
    if role == "leader" or role == "standalone" then
      return base
    end
  end
  return nil
end

-- Public: pick the upstream base URL for a WRITE, using the cache when
-- warm. `force_refresh` bypasses the cache (used after a 409).
function _M.leader_url(force_refresh)
  if not force_refresh then
    local cached = CACHE:get(CACHE_KEY)
    if cached then
      return cached
    end
  end
  local leader = resolve_leader()
  if leader then
    CACHE:set(CACHE_KEY, leader, CACHE_TTL)
    return leader
  end
  -- No leader found (both probing as follower mid-handoff, or both
  -- down). Fall back to the first node; the request will 503/409 and
  -- the client retries — better than a hard 502 here.
  return NODES[1]
end

-- Invalidate the cache so the next write re-resolves. Called when a
-- write comes back 409 (we routed to a node that is no longer leader).
function _M.invalidate()
  CACHE:delete(CACHE_KEY)
end

-- The OTHER node, given a base URL — used to retry a 409'd write on the
-- node we did NOT just try.
function _M.other_node(base)
  for _, n in ipairs(NODES) do
    if n ~= base then
      return n
    end
  end
  return base
end

return _M
