/*
 * Visitor analytics endpoint (njs): POST /a/event.
 *
 * The browser (src/analytics.ts) says WHAT happened. This adds WHO and
 * WHERE from sources the browser can't fake, then writes one document to
 * the NebulaDB bucket `showcase_events` through the internal /_a/write
 * location (which injects the API bearer):
 *
 *   user     the signed-in identity: $auth_email (Google / OpsAPI) or
 *            $remote_user (password login). /a/event sits behind the same
 *            sign-in gate as the site, so anonymous events never arrive.
 *   ip       X-WSL-Client-IP, stamped by the wslproxy edge hook (Traefik
 *            rewrites X-Forwarded-For, so that header only holds a pod IP),
 *            else X-Real-IP / X-Forwarded-For / the socket address.
 *   country  X-WSL-Geo-Country (edge IP2Location lookup), else the
 *            browser's guess from its timezone / language.
 *
 * Browser-supplied strings are whitelisted and length-capped.
 */

const TYPES = {
  page_view: "viewed",
  video_play: "started video",
  video_progress: "watched",
  video_complete: "finished video",
  video_stop: "stopped video",
  click: "clicked",
  input: "typed into",
  session_end: "left after",
};

function str(v, n) {
  if (v === undefined || v === null) return "";
  return String(v).replace(/[\u0000-\u001f]+/g, " ").trim().slice(0, n);
}
function num(v) {
  const x = Number(v);
  return isFinite(x) ? Math.round(x * 10) / 10 : null;
}
function cc(v) {
  const c = str(v, 8).toUpperCase();
  return /^[A-Z]{2}$/.test(c) ? (c === "UK" ? "GB" : c) : "";
}
function clientIp(r) {
  const edge = str(r.headersIn["X-WSL-Client-IP"], 64);
  if (edge) return edge;
  const real = str(r.headersIn["X-Real-IP"], 64);
  if (real) return real;
  const xff = str(r.headersIn["X-Forwarded-For"], 256).split(",")[0].trim();
  return xff || r.remoteAddress;
}
function hex(n) {
  const b = crypto.getRandomValues(new Uint8Array(n));
  return Array.from(b, (x) => ("0" + x.toString(16)).slice(-2)).join("");
}

/** Exported for tests: build the stored document from a request's parts. */
function buildDoc(e, ctx) {
  const type = str(e.type, 32);
  if (!TYPES[type]) return null;
  const edgeCountry = cc(ctx.edgeCountry);
  const guess = cc(e.country_guess);
  const now = ctx.now;
  const m = {
    type,
    site: str(e.site, 32),
    host: str(ctx.host, 128),
    user: str(ctx.user, 200) || "anonymous",
    ip: str(ctx.ip, 64),
    country: edgeCountry || guess,
    country_source: edgeCountry ? "ip" : guess ? "browser" : "",
    country_browser: guess,
    lang: str(e.lang, 35),
    langs: str(e.langs, 200),
    tz: str(e.tz, 64),
    screen: str(e.screen, 16),
    viewport: str(e.viewport, 16),
    ua: str(ctx.ua, 300),
    path: str(e.path, 200),
    title: str(e.title, 160),
    demo: str(e.demo, 64),
    video: str(e.video, 160),
    pct: num(e.pct),
    seconds: num(e.seconds),
    label: str(e.label, 80),
    value: str(e.value, 300),
    referrer: str(e.referrer, 300),
    session: str(e.session, 32),
    visitor: str(e.visitor, 32),
    ts: Math.floor(now / 1000),
    day: new Date(now).toISOString().slice(0, 10),
  };
  Object.keys(m).forEach((k) => {
    if (m[k] === "" || m[k] === null) delete m[k];
  });
  const what =
    type === "page_view"
      ? m.title || m.path
      : type.indexOf("video") === 0
        ? `${m.video || "a video"}${m.pct != null ? ` (${m.pct}%)` : ""}`
        : type === "session_end"
          ? `${m.seconds || 0}s`
          : `${m.label || ""}${m.value ? `: ${m.value}` : ""}`;
  const text = `${m.user} ${TYPES[type]} ${what} on ${m.host || m.site || "the site"}${m.country ? ` from ${m.country}` : ""}`;
  return { id: `${m.ts}-${ctx.rand}`, text: text.slice(0, 600), metadata: m };
}

async function event(r) {
  if (r.method !== "POST") {
    r.return(405);
    return;
  }
  let e;
  try {
    e = JSON.parse(r.requestText || "");
  } catch (err) {
    r.return(400);
    return;
  }
  const doc = buildDoc(e || {}, {
    host: (r.headersIn.Host || "").toLowerCase().replace(/:\d+$/, ""),
    user: r.variables.auth_email || r.variables.remote_user || "",
    ip: clientIp(r),
    edgeCountry: r.headersIn["X-WSL-Geo-Country"],
    ua: r.headersIn["User-Agent"],
    now: Date.now(),
    rand: hex(6),
  });
  if (!doc) {
    r.return(400);
    return;
  }
  try {
    const res = await r.subrequest("/_a/write", { method: "POST", body: JSON.stringify(doc) });
    if (res.status >= 300) r.warn(`analytics: write ${res.status}`);
  } catch (err) {
    r.warn(`analytics: write failed: ${err}`);
  }
  r.return(204);
}

export default { event, buildDoc };
