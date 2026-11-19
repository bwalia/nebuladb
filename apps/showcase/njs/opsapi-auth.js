/*
 * "Login with OpsAPI" for the showcase and customer demo sites (njs).
 *
 * One sign-in for every site: OpsAPI's hosted page offers Google, email +
 * password (+ emailed code) and single sign-on; we never see a password.
 *
 *   /login          -> 302 to OPSAPI /auth/signin?frontend_url=<origin>&from=<state>
 *   /auth/callback  <- ?token=<JWT>&redirect=<state>
 *                      check state, GET /auth/me with the token (never verify
 *                      the JWT locally: it is HS256 with OpsAPI's own key),
 *                      check the user's workspace against this host's access
 *                      policy, then set a signed session cookie.
 *   /auth/verify    auth_request gate: 204 when the session cookie is valid
 *   /auth/session   JSON {email, name} for the app's "Signed in as"
 *   /auth/logout    clear the cookie and end the OpsAPI session
 *
 * Config (env, declared with `env` in the main nginx.conf):
 *   OPSAPI_IDENTITY_URL     browser-facing OpsAPI, e.g. https://identity.demo.opsapi.org
 *   OPSAPI_API_URL          server-side base for /auth/me (default: OPSAPI_IDENTITY_URL)
 *   SHOWCASE_SESSION_SECRET HMAC key for the session cookie (>= 32 chars)
 *   SHOWCASE_OPSAPI_ACCESS  per-host policy, e.g.
 *     "sourcesupplies.nebuladb.net=ns:source-supplies,ns:nebuladb-team;*=ns:nebuladb-team"
 *     rules: ns:<workspace slug> | email:<address> | domain:<example.com>
 */
import cr from "crypto";

const SESSION = "ss_session";
const STATE = "ss_auth_state";
const TTL = 12 * 3600; // seconds

function env(name) {
  const v = process.env[name];
  return v === undefined || v === null ? "" : String(v).trim();
}
const IDP = () => env("OPSAPI_IDENTITY_URL").replace(/\/+$/, "");
const IDP_API = () => (env("OPSAPI_API_URL") || env("OPSAPI_IDENTITY_URL")).replace(/\/+$/, "");
const SECRET = () => env("SHOWCASE_SESSION_SECRET");

function b64u(s) {
  return Buffer.from(s).toString("base64url");
}
function unb64u(s) {
  return Buffer.from(s, "base64url").toString();
}
function sign(data) {
  return cr.createHmac("sha256", SECRET()).update(data).digest("base64url");
}
function safeEqual(a, b) {
  if (typeof a !== "string" || typeof b !== "string" || a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}
function randomHex(n) {
  const bytes = crypto.getRandomValues(new Uint8Array(n));
  return Array.from(bytes, (b) => ("0" + b.toString(16)).slice(-2)).join("");
}
function cookies(r) {
  const out = {};
  const raw = r.headersIn.Cookie || "";
  raw.split(";").forEach((part) => {
    const i = part.indexOf("=");
    if (i > 0) out[part.slice(0, i).trim()] = part.slice(i + 1).trim();
  });
  return out;
}
function host(r) {
  return (r.headersIn.Host || "").toLowerCase().replace(/:\d+$/, "");
}
function cookie(name, value, maxAge, path) {
  return `${name}=${value}; Path=${path || "/"}; Max-Age=${maxAge}; HttpOnly; Secure; SameSite=Lax`;
}
/** Only same-site relative paths; anything else lands on "/". */
function safePath(p) {
  if (typeof p !== "string" || p[0] !== "/" || p[1] === "/" || p[1] === "\\") return "/";
  return p.length > 1024 ? "/" : p;
}
function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}

/** Exported for tests: parse the access policy for one host. */
function rulesFor(policy, h) {
  let fallback = [];
  const entries = String(policy || "").split(";");
  for (let k = 0; k < entries.length; k++) {
    const entry = entries[k];
    const i = entry.indexOf("=");
    if (i < 0) continue;
    const key = entry.slice(0, i).trim().toLowerCase();
    const rules = entry.slice(i + 1).split(",").map((x) => x.trim()).filter(Boolean);
    if (key === h) return rules;
    if (key === "*") fallback = rules;
  }
  return fallback;
}

/** Exported for tests: does this /auth/me body satisfy any rule? */
function allowed(rules, me) {
  const email = String((me && me.user && me.user.email) || "").toLowerCase();
  const slugs = ((me && me.namespaces) || [])
    .filter((n) => n && (n.member_status == null || n.member_status === "active") && (n.status == null || n.status === "active"))
    .map((n) => String(n.slug || "").toLowerCase());
  return rules.some((rule) => {
    const i = rule.indexOf(":");
    const kind = rule.slice(0, i).toLowerCase();
    const val = rule.slice(i + 1).toLowerCase();
    if (!val) return false;
    if (kind === "ns") return slugs.indexOf(val) >= 0;
    if (kind === "email") return email === val;
    if (kind === "domain") return email.endsWith("@" + val);
    return false;
  });
}

function readSession(r) {
  const raw = cookies(r)[SESSION];
  if (!raw || !SECRET()) return null;
  const dot = raw.lastIndexOf(".");
  if (dot < 1) return null;
  const data = raw.slice(0, dot);
  if (!safeEqual(sign(data), raw.slice(dot + 1))) return null;
  let s;
  try {
    s = JSON.parse(unb64u(data));
  } catch (e) {
    return null;
  }
  if (!s || s.h !== host(r) || typeof s.x !== "number" || s.x < Math.floor(Date.now() / 1000)) return null;
  return s;
}

function page(r, status, title, body) {
  r.headersOut["Content-Type"] = "text/html; charset=utf-8";
  r.headersOut["Cache-Control"] = "no-store";
  r.return(
    status,
    `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">` +
      `<title>${esc(title)}</title><style>body{font:16px/1.55 system-ui,-apple-system,Segoe UI,sans-serif;background:#f4f2ee;color:#16130f;` +
      `display:grid;place-items:center;min-height:100vh;margin:0;padding:16px}main{max-width:460px;background:#fff;border:1px solid #e3ddd3;` +
      `border-radius:16px;padding:28px}h1{font-size:22px;margin:0 0 8px}a.btn{display:inline-block;margin-top:16px;background:#0f6b5c;color:#fff;` +
      `text-decoration:none;padding:10px 16px;border-radius:10px}</style></head><body><main><h1>${esc(title)}</h1>${body}</main></body></html>`
  );
}

/* ------------------------------ handlers ------------------------------ */

function login(r) {
  if (!IDP()) {
    r.internalRedirect("/login.html");
    return;
  }
  const state = randomHex(16);
  const rd = safePath(r.args.rd);
  r.headersOut["Set-Cookie"] = cookie(STATE, `${state}.${b64u(rd)}`, 600, "/auth");
  r.headersOut["Cache-Control"] = "no-store";
  const origin = `https://${host(r)}`;
  r.return(302, `${IDP()}/auth/signin?frontend_url=${encodeURIComponent(origin)}&from=${state}`);
}

async function callback(r) {
  const st = (cookies(r)[STATE] || "").split(".");
  const token = r.args.token;
  if (!token || !st[0] || !safeEqual(st[0], String(r.args.redirect || ""))) {
    page(r, 400, "Sign-in expired", `<p>That sign-in link is no longer valid. Please sign in again.</p><a class="btn" href="/login">Sign in</a>`);
    return;
  }
  let me;
  try {
    const resp = await ngx.fetch(`${IDP_API()}/auth/me`, {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
    });
    if (resp.status === 401 || resp.status === 403) {
      page(r, 400, "Sign-in expired", `<p>Your sign-in has expired. Please sign in again.</p><a class="btn" href="/login">Sign in</a>`);
      return;
    }
    if (!resp.ok) throw new Error(`auth/me ${resp.status}`);
    me = await resp.json();
  } catch (e) {
    r.error(`opsapi-auth: /auth/me failed: ${e}`);
    page(r, 502, "Sign-in service unavailable", `<p>We couldn't confirm your sign-in with OpsAPI. Please try again in a moment.</p><a class="btn" href="/login">Try again</a>`);
    return;
  }
  const email = String((me.user && me.user.email) || "");
  const h = host(r);
  if (!email || !allowed(rulesFor(env("SHOWCASE_OPSAPI_ACCESS"), h), me)) {
    r.warn(`opsapi-auth: ${email || "unknown user"} not allowed on ${h}`);
    page(
      r,
      403,
      "No access to these demos yet",
      `<p>You're signed in to OpsAPI as <b>${esc(email || "an unknown user")}</b>, but that account hasn't been given access to <b>${esc(h)}</b>.</p>` +
        `<p>Ask your contact to add you, or use a different account.</p><a class="btn" href="/auth/logout">Use a different account</a>`
    );
    return;
  }
  const name = [me.user.first_name, me.user.last_name].filter(Boolean).join(" ").trim();
  const data = b64u(JSON.stringify({ e: email, n: name, h, x: Math.floor(Date.now() / 1000) + TTL }));
  r.headersOut["Set-Cookie"] = [cookie(SESSION, `${data}.${sign(data)}`, TTL), cookie(STATE, "", 0, "/auth")];
  r.headersOut["Cache-Control"] = "no-store";
  r.return(302, safePath(st[1] ? unb64u(st[1]) : "/"));
}

function verify(r) {
  const s = readSession(r);
  if (!s) {
    r.return(401);
    return;
  }
  r.headersOut["X-Auth-Request-Email"] = s.e;
  r.headersOut["X-Auth-Request-User"] = s.n || s.e;
  r.return(204);
}

function session(r) {
  const s = readSession(r);
  r.headersOut["Content-Type"] = "application/json";
  r.headersOut["Cache-Control"] = "no-store";
  if (!s) {
    r.return(401, JSON.stringify({ error: "not signed in" }));
    return;
  }
  r.return(200, JSON.stringify({ email: s.e, name: s.n || "", expires: s.x }));
}

function logout(r) {
  r.headersOut["Set-Cookie"] = cookie(SESSION, "", 0);
  r.headersOut["Cache-Control"] = "no-store";
  const origin = `https://${host(r)}`;
  r.return(302, IDP() ? `${IDP()}/auth/signout?frontend_url=${encodeURIComponent(origin)}` : "/login");
}

/** js_set $opsapi_email: the signed-in email for upstream identity headers. */
function email(r) {
  const s = readSession(r);
  return s ? s.e : "";
}

export default { login, callback, verify, session, logout, email, rulesFor, allowed };
