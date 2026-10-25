/*
 * In-browser demonstrations for nebuladb.net.
 *
 * Each demo runs the real algorithm on toy data so the behaviour is honest:
 *   - HNSW: a small layered graph built and searched here, counting every
 *     distance computation against brute force (prod: crates/nebula-vector).
 *   - Hybrid: real BM25 over eight sample docs, min-max fused with vector
 *     similarities (precomputed for the sample queries) using the same
 *     weighted-sum rule as NEBULA_HYBRID_WEIGHTS (prod: crates/nebula-bm25).
 *   - WAL: CRC32-framed records, a torn tail on crash, snapshot + replay on
 *     restart (prod: crates/nebula-wal, nebula-index/src/durability.rs).
 */
(function () {
  "use strict";

  function css(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  // mulberry32: small seeded PRNG so every visitor sees the same corpus
  function rng(seed) {
    return function () {
      seed = (seed + 0x6d2b79f5) | 0;
      var t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  function $(id) { return document.getElementById(id); }

  /* ================================================================ HNSW */
  (function hnswDemo() {
    var canvas = $("hnsw-canvas");
    if (!canvas) return;
    var ctx = canvas.getContext("2d");
    var nIn = $("hnsw-n"), mIn = $("hnsw-m"), efIn = $("hnsw-ef");
    var out = {
      n: $("hnsw-n-out"), m: $("hnsw-m-out"), ef: $("hnsw-ef-out"),
      layers: $("hnsw-layers"), dist: $("hnsw-dist"), touched: $("hnsw-touched"),
      recall: $("hnsw-recall"), ms: $("hnsw-ms"), brute: $("hnsw-brute")
    };
    var K = 5, MAX_LEVEL = 4;
    var g = null, query = null, last = null;

    function build(N, M) {
      var r = rng(42);
      // clustered 2-D points: real embeddings are lumpy, not uniform
      var centres = [];
      for (var c = 0; c < 7; c++) centres.push([0.12 + r() * 0.76, 0.12 + r() * 0.76, 0.05 + r() * 0.09]);
      var pts = [], level = [];
      var mL = 1 / Math.log(M);
      for (var i = 0; i < N; i++) {
        var ce = centres[i % centres.length];
        // Box-Muller
        var u = Math.max(1e-9, r()), v = r();
        var mag = Math.sqrt(-2 * Math.log(u)) * ce[2];
        var x = ce[0] + mag * Math.cos(2 * Math.PI * v);
        var y = ce[1] + mag * Math.sin(2 * Math.PI * v);
        if (x < 0.01 || x > 0.99 || y < 0.01 || y > 0.99) { i--; continue; } // resample, don't pile up on the edge
        pts.push([x, y]);
        level.push(Math.min(MAX_LEVEL, Math.floor(-Math.log(1 - r()) * mL)));
      }
      var maxL = 0, entry = 0;
      for (i = 0; i < N; i++) if (level[i] > maxL) { maxL = level[i]; entry = i; }

      var neigh = [];
      for (var l = 0; l <= maxL; l++) {
        var members = [];
        for (i = 0; i < N; i++) if (level[i] >= l) members.push(i);
        var cap = l === 0 ? 2 * M : M;
        var adj = new Map();
        members.forEach(function (a) { adj.set(a, new Set()); });
        members.forEach(function (a) {
          // keep the `cap` nearest members (insertion into a short sorted list)
          var best = [];
          for (var j = 0; j < members.length; j++) {
            var b = members[j];
            if (b === a) continue;
            var dx = pts[a][0] - pts[b][0], dy = pts[a][1] - pts[b][1], d = dx * dx + dy * dy;
            if (best.length < cap || d < best[best.length - 1][0]) {
              var k = best.length;
              best.push([d, b]);
              while (k > 0 && best[k - 1][0] > d) { best[k] = best[k - 1]; k--; }
              best[k] = [d, b];
              if (best.length > cap) best.pop();
            }
          }
          best.forEach(function (e) { adj.get(a).add(e[1]); adj.get(e[1]).add(a); });
        });
        neigh.push(adj);
      }
      return { pts: pts, level: level, maxL: maxL, entry: entry, neigh: neigh, N: N };
    }

    function search(q, ef) {
      var pts = g.pts, count = 0;
      function d(i) {
        count++;
        var dx = pts[i][0] - q[0], dy = pts[i][1] - q[1];
        return dx * dx + dy * dy;
      }
      var path = [], explored = [];
      var ep = g.entry, epd = d(ep);
      for (var l = g.maxL; l >= 1; l--) {
        var changed = true;
        while (changed) {
          changed = false;
          g.neigh[l].get(ep).forEach(function (n) {
            var dn = d(n);
            if (dn < epd) { path.push([ep, n, l]); ep = n; epd = dn; changed = true; }
          });
        }
      }
      // layer 0: beam search with width ef
      var visited = new Set([ep]);
      var cand = [[epd, ep]], res = [[epd, ep]];
      while (cand.length) {
        var c = cand.shift();
        if (res.length >= ef && c[0] > res[res.length - 1][0]) break;
        g.neigh[0].get(c[1]).forEach(function (n) {
          if (visited.has(n)) return;
          visited.add(n);
          var dn = d(n);
          if (res.length < ef || dn < res[res.length - 1][0]) {
            explored.push([c[1], n]);
            insertSorted(cand, [dn, n]);
            insertSorted(res, [dn, n]);
            if (res.length > ef) res.pop();
          }
        });
      }
      return {
        found: res.slice(0, K).map(function (e) { return e[1]; }),
        count: count, visited: visited, path: path, explored: explored
      };
    }

    function insertSorted(arr, e) {
      var lo = 0, hi = arr.length;
      while (lo < hi) { var mid = (lo + hi) >> 1; if (arr[mid][0] < e[0]) lo = mid + 1; else hi = mid; }
      arr.splice(lo, 0, e);
    }

    function brute(q) {
      return g.pts
        .map(function (p, i) { var dx = p[0] - q[0], dy = p[1] - q[1]; return [dx * dx + dy * dy, i]; })
        .sort(function (a, b) { return a[0] - b[0]; })
        .slice(0, K)
        .map(function (e) { return e[1]; });
    }

    function size() {
      // the canvas is absolutely positioned and fills its stage, whose
      // height follows the controls column on wide screens
      var rect = canvas.parentElement.getBoundingClientRect();
      var dpr = window.devicePixelRatio || 1;
      var w = Math.max(200, rect.width), h = Math.max(200, rect.height);
      canvas.width = Math.round(w * dpr);
      canvas.height = Math.round(h * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      return { w: w, h: h };
    }

    function draw() {
      if (!g) return;
      var s = size(), W = s.w, H = s.h;
      var text = css("--text"), muted = css("--muted"), accent = css("--accent"), ok = css("--ok"), line = css("--line-strong");
      var X = function (p) { return 10 + p[0] * (W - 20); };
      var Y = function (p) { return 10 + p[1] * (H - 20); };
      ctx.clearRect(0, 0, W, H);

      // layer-0 graph, very faint
      ctx.globalAlpha = g.N > 1000 ? 0.05 : 0.09;
      ctx.strokeStyle = muted;
      ctx.lineWidth = 0.6;
      ctx.beginPath();
      g.neigh[0].forEach(function (set, a) {
        set.forEach(function (b) {
          if (b < a) return;
          ctx.moveTo(X(g.pts[a]), Y(g.pts[a]));
          ctx.lineTo(X(g.pts[b]), Y(g.pts[b]));
        });
      });
      ctx.stroke();

      // upper layers: the express lanes
      ctx.globalAlpha = 0.28;
      ctx.strokeStyle = line;
      ctx.lineWidth = 1;
      for (var l = 1; l <= g.maxL; l++) {
        ctx.beginPath();
        g.neigh[l].forEach(function (set, a) {
          set.forEach(function (b) {
            if (b < a) return;
            ctx.moveTo(X(g.pts[a]), Y(g.pts[a]));
            ctx.lineTo(X(g.pts[b]), Y(g.pts[b]));
          });
        });
        ctx.stroke();
      }
      ctx.globalAlpha = 1;

      // points
      g.pts.forEach(function (p, i) {
        var lv = g.level[i];
        ctx.fillStyle = lv > 0 ? text : muted;
        ctx.globalAlpha = lv > 0 ? 0.9 : 0.45;
        ctx.beginPath();
        ctx.arc(X(p), Y(p), lv > 0 ? 1.6 + lv : 1.4, 0, Math.PI * 2);
        ctx.fill();
      });
      ctx.globalAlpha = 1;

      if (!last) return;
      // nodes whose distance was computed
      ctx.fillStyle = accent;
      ctx.globalAlpha = 0.45;
      last.visited.forEach(function (i) {
        ctx.beginPath();
        ctx.arc(X(g.pts[i]), Y(g.pts[i]), 2.4, 0, Math.PI * 2);
        ctx.fill();
      });
      // layer-0 expansions
      ctx.globalAlpha = 0.5;
      ctx.strokeStyle = accent;
      ctx.lineWidth = 0.8;
      ctx.beginPath();
      last.explored.forEach(function (e) {
        ctx.moveTo(X(g.pts[e[0]]), Y(g.pts[e[0]]));
        ctx.lineTo(X(g.pts[e[1]]), Y(g.pts[e[1]]));
      });
      ctx.stroke();
      // greedy descent through the upper layers
      ctx.globalAlpha = 1;
      ctx.lineWidth = 2.2;
      ctx.beginPath();
      last.path.forEach(function (e) {
        ctx.moveTo(X(g.pts[e[0]]), Y(g.pts[e[0]]));
        ctx.lineTo(X(g.pts[e[1]]), Y(g.pts[e[1]]));
      });
      ctx.stroke();
      // entry point
      ctx.strokeStyle = text;
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.arc(X(g.pts[g.entry]), Y(g.pts[g.entry]), 7, 0, Math.PI * 2);
      ctx.stroke();
      // results
      ctx.strokeStyle = ok;
      ctx.lineWidth = 2;
      last.found.forEach(function (i) {
        ctx.beginPath();
        ctx.arc(X(g.pts[i]), Y(g.pts[i]), 5.5, 0, Math.PI * 2);
        ctx.stroke();
      });
      // query
      var qx = X(query), qy = Y(query);
      ctx.strokeStyle = text;
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(qx - 8, qy); ctx.lineTo(qx + 8, qy);
      ctx.moveTo(qx, qy - 8); ctx.lineTo(qx, qy + 8);
      ctx.stroke();
    }

    function run() {
      var t0 = performance.now();
      last = search(query, +efIn.value);
      var ms = performance.now() - t0;
      var truth = brute(query);
      var hit = last.found.filter(function (i) { return truth.indexOf(i) >= 0; }).length;
      out.dist.textContent = last.count.toLocaleString();
      out.brute.textContent = g.N.toLocaleString();
      out.touched.textContent = ((last.count / g.N) * 100).toFixed(1) + "%";
      out.recall.textContent = (hit / K).toFixed(2);
      out.ms.textContent = ms < 1 ? ms.toFixed(2) : ms.toFixed(1);
      draw();
    }

    function rebuild() {
      out.n.textContent = (+nIn.value).toLocaleString();
      out.m.textContent = mIn.value;
      g = build(+nIn.value, +mIn.value);
      out.layers.textContent = String(g.maxL + 1);
      run();
    }

    var pending = 0;
    function schedule() {
      clearTimeout(pending);
      out.n.textContent = (+nIn.value).toLocaleString();
      out.m.textContent = mIn.value;
      pending = setTimeout(rebuild, 120);
    }

    nIn.addEventListener("input", schedule);
    mIn.addEventListener("input", schedule);
    efIn.addEventListener("input", function () { out.ef.textContent = efIn.value; run(); });
    canvas.addEventListener("click", function (e) {
      var rect = canvas.getBoundingClientRect();
      query = [
        Math.min(1, Math.max(0, (e.clientX - rect.left - 10) / (rect.width - 20))),
        Math.min(1, Math.max(0, (e.clientY - rect.top - 10) / (rect.height - 20)))
      ];
      run();
    });
    var qr = rng(7);
    $("hnsw-random").addEventListener("click", function () { query = [0.05 + qr() * 0.9, 0.05 + qr() * 0.9]; run(); });
    window.addEventListener("resize", function () { draw(); });
    document.addEventListener("themechange", draw);
    document.addEventListener("tabshown", function (e) { if (e.detail === "demo-hnsw") draw(); });

    query = [0.62, 0.38];
    out.ef.textContent = efIn.value;
    rebuild();
  })();

  /* ============================================================== HYBRID */
  (function hybridDemo() {
    var list = $("hybrid-hits");
    if (!list) return;

    var DOCS = [
      { id: "pm-2291", region: "us", title: "Postmortem: checkout-api ERR_CONN_RESET at the load balancer",
        text: "checkout-api returned ERR_CONN_RESET for 14 minutes. The load balancer idle timeout was shorter than the upstream keepalive, so pooled connections were reset mid request. Fix: align keepalive and idle timeout." },
      { id: "rb-failover", region: "eu", title: "Runbook: regional failover",
        text: "When a region becomes degraded, promote the follower in the healthy region, lower DNS TTLs and let health checks steer traffic. Confirm replication lag is zero before re-homing buckets." },
      { id: "dns-ha", region: "us", title: "Design: DNS failover with health checks",
        text: "Short TTLs and active health checks move clients away from an unhealthy region within a minute. Weighted records drain traffic gradually during recovery." },
      { id: "zt-contractors", region: "eu", title: "Policy: zero trust access for contractors",
        text: "Zero trust replaces network location with per request verification. Contractors receive short lived, scoped credentials and device posture checks; nothing is trusted by default." },
      { id: "vpn-sunset", region: "eu", title: "Plan: retiring the perimeter VPN",
        text: "The perimeter VPN is replaced by an identity aware proxy. Access is granted per application rather than per network, which removes lateral movement after a stolen credential." },
      { id: "pm-2310", region: "eu", title: "Postmortem: intermittent ECONNRESET in payment gateway",
        text: "The payment gateway logged ECONNRESET under load after a TLS library upgrade. Connection resets stopped once the handshake timeout was restored." },
      { id: "backup-pitr", region: "us", title: "Guide: backup and point in time restore",
        text: "Snapshots and archived WAL segments in object storage let operators restore a cluster to any second, including into another region after a disaster." },
      { id: "tiers", region: "eu", title: "Subscription tiers and SLAs",
        text: "The enterprise tier includes a 99.95 percent availability SLA, multi region replication and named support. Standard runs in one region." }
    ];

    // Cosine similarities for the sample queries. In NebulaDB these come from
    // the configured embedder (e.g. Ollama nomic-embed-text); fixed here so
    // the page needs no model.
    var QUERIES = [
      { q: "ERR_CONN_RESET checkout-api", vec: [0.69, 0.21, 0.24, 0.08, 0.1, 0.72, 0.07, 0.05] },
      { q: "how do we recover when a region goes down", vec: [0.22, 0.83, 0.74, 0.1, 0.12, 0.17, 0.52, 0.31] },
      { q: "zero trust access for contractors", vec: [0.06, 0.12, 0.09, 0.86, 0.7, 0.05, 0.08, 0.14] }
    ];

    function tokens(s) { return s.toLowerCase().split(/[^a-z0-9_]+/).filter(Boolean); }

    // Okapi BM25, k1=1.2, b=0.75 (same defaults as nebula-bm25)
    var k1 = 1.2, b = 0.75;
    var docTok = DOCS.map(function (d) { return tokens(d.title + " " + d.text); });
    var avgdl = docTok.reduce(function (a, t) { return a + t.length; }, 0) / docTok.length;
    var df = {};
    docTok.forEach(function (t) { new Set(t).forEach(function (w) { df[w] = (df[w] || 0) + 1; }); });
    function bm25(q) {
      var qt = Array.from(new Set(tokens(q)));
      return docTok.map(function (t) {
        var s = 0;
        qt.forEach(function (w) {
          if (!df[w]) return;
          var tf = t.filter(function (x) { return x === w; }).length;
          if (!tf) return;
          var idf = Math.log(1 + (DOCS.length - df[w] + 0.5) / (df[w] + 0.5));
          s += idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * t.length / avgdl));
        });
        return s;
      });
    }

    function minmax(xs) {
      var lo = Math.min.apply(null, xs), hi = Math.max.apply(null, xs);
      return xs.map(function (x) { return hi > lo ? (x - lo) / (hi - lo) : 0; });
    }

    var qSel = $("hybrid-query"), wIn = $("hybrid-w"), wOut = $("hybrid-w-out"), euIn = $("hybrid-eu");
    var envOut = $("hybrid-env"), reqOut = $("hybrid-req");

    function render() {
      var Q = QUERIES[+qSel.value];
      var w = +wIn.value / 100;
      wOut.textContent = w.toFixed(2) + " / " + (1 - w).toFixed(2);
      var lex = minmax(bm25(Q.q)), vec = minmax(Q.vec);
      var rows = DOCS.map(function (d, i) {
        return { d: d, vec: vec[i], lex: lex[i], score: w * vec[i] + (1 - w) * lex[i] };
      });
      if (euIn.checked) rows = rows.filter(function (r) { return r.d.region === "eu"; });
      rows.sort(function (a, b) { return b.score - a.score; });
      rows = rows.slice(0, 5);

      list.innerHTML = rows.map(function (r, i) {
        return '<li class="hit">' +
          '<span class="hit-rank">#' + (i + 1) + "</span>" +
          '<span class="hit-title" title="' + r.d.title + '">' + r.d.title + "</span>" +
          '<span class="hit-score">' + r.score.toFixed(3) + "</span>" +
          '<div class="hit-bars">' +
          '<div class="bar"><small>vector ' + r.vec.toFixed(2) + '</small><i><b style="width:' + (r.vec * 100).toFixed(0) + '%"></b></i></div>' +
          '<div class="bar lex"><small>bm25 ' + r.lex.toFixed(2) + '</small><i><b style="width:' + (r.lex * 100).toFixed(0) + '%"></b></i></div>' +
          '<div class="bar meta"><small>region ' + r.d.region + '</small><i><b style="width:' + (r.d.region === "eu" ? 100 : 0) + '%"></b></i></div>' +
          "</div></li>";
      }).join("");

      envOut.textContent = 'NEBULA_HYBRID_WEIGHTS="' + w.toFixed(2) + "," + (1 - w).toFixed(2) + '"';
      reqOut.textContent = 'POST /api/v1/ai/search\n{ "query": "' + Q.q + '", "bucket": "kb", "top_k": 5, "hybrid": true }' +
        (euIn.checked
          ? "\n\n-- metadata filters are expressed in SQL:\nSELECT id, title FROM kb\n WHERE semantic_match(content, '" + Q.q + "')\n   AND region = 'eu'\n LIMIT 5;"
          : "");
    }

    [qSel, wIn, euIn].forEach(function (el) { el.addEventListener("input", render); });
    render();
  })();

  /* ================================================================= WAL */
  (function walDemo() {
    var walRow = $("wal-frames");
    if (!walRow) return;
    var snapRow = $("wal-snap"), memRow = $("wal-mem"), log = $("wal-log");
    var btns = {
      write: $("wal-write"), snap: $("wal-snapshot"), crash: $("wal-crash"), restart: $("wal-restart"), reset: $("wal-reset")
    };

    var CRC_TABLE = (function () {
      var t = [];
      for (var n = 0; n < 256; n++) {
        var c = n;
        for (var k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
        t[n] = c >>> 0;
      }
      return t;
    })();
    function crc32(str) {
      var c = 0xffffffff;
      for (var i = 0; i < str.length; i++) c = CRC_TABLE[(c ^ str.charCodeAt(i)) & 0xff] ^ (c >>> 8);
      return ((c ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0");
    }

    var NAMES = ["runbook-failover", "pm-2291", "zt-policy", "tiers", "dns-ha", "backup-guide", "vpn-sunset", "pm-2310", "sla-faq", "oncall-rota"];
    var state;

    function reset() {
      state = { seq: 0, wal: [], snap: null, mem: [], up: true };
      log.textContent = "";
      say("node up · NEBULA_DATA_DIR=/var/lib/nebula · empty WAL");
      render();
    }

    function say(msg) {
      var t = new Date().toISOString().slice(11, 23);
      log.textContent += t + "  " + msg + "\n";
      log.scrollTop = log.scrollHeight;
    }

    function frameHtml(f, cls) {
      return '<span class="frame ' + (cls || "") + '">#' + f.seq + " " + f.id +
        "<em>len " + f.len + " · crc " + f.crc + "</em></span>";
    }

    function render() {
      var snapSeq = state.snap ? state.snap.seq : 0;
      walRow.innerHTML = state.wal.map(function (f) {
        return frameHtml(f, f.torn ? "torn" : f.seq <= snapSeq ? "applied" : "");
      }).join("") || '<span class="wal-row-label">no segments</span>';
      snapRow.innerHTML = state.snap
        ? '<span class="frame snap">snap-' + String(state.snap.seq).padStart(5, "0") + ".nsnap<em>" + state.snap.docs.length + " docs · lsn " + state.snap.seq + "</em></span>"
        : '<span class="wal-row-label">no snapshot</span>';
      memRow.innerHTML = state.up
        ? state.mem.map(function (id) { return '<span class="frame">' + id + "<em>hnsw · bm25</em></span>"; }).join("") || '<span class="wal-row-label">empty index</span>'
        : '<span class="wal-row-label">process down</span>';
      btns.write.disabled = !state.up;
      btns.snap.disabled = !state.up || !state.mem.length;
      btns.crash.disabled = !state.up;
      btns.restart.disabled = state.up;
    }

    btns.write.addEventListener("click", function () {
      var id = NAMES[state.seq % NAMES.length];
      var body = JSON.stringify({ op: "upsert", bucket: "kb", id: id, seq: state.seq + 1 });
      var f = { seq: ++state.seq, id: id, len: body.length, crc: crc32(body) };
      state.wal.push(f);
      say("append #" + f.seq + " len=" + f.len + " crc32=" + f.crc + " · fsync ok → apply to index");
      if (state.mem.indexOf(id) < 0) state.mem.push(id);
      render();
    });

    btns.snap.addEventListener("click", function () {
      state.snap = { seq: state.seq, docs: state.mem.slice() };
      say("snapshot written to .tmp, fsync, atomic rename → snap-" + String(state.seq).padStart(5, "0") + ".nsnap");
      render();
    });

    btns.crash.addEventListener("click", function () {
      var id = NAMES[state.seq % NAMES.length];
      state.wal.push({ seq: state.seq + 1, id: id, len: 61, crc: "????????", torn: true });
      state.up = false;
      state.mem = [];
      say("kill -9 during fsync of #" + (state.seq + 1) + " · RAM lost · tail frame half written");
      render();
    });

    btns.restart.addEventListener("click", function () {
      state.up = true;
      var from = 0;
      if (state.snap) {
        state.mem = state.snap.docs.slice();
        from = state.snap.seq;
        say("load snapshot lsn " + from + " (" + state.mem.length + " docs)");
      } else {
        say("no snapshot · replay WAL from the start");
      }
      var replayed = 0;
      state.wal = state.wal.filter(function (f) {
        if (f.torn) {
          say("frame #" + f.seq + ": crc mismatch on torn tail → truncate to last good record");
          return false;
        }
        if (f.seq > from) {
          replayed++;
          if (state.mem.indexOf(f.id) < 0) state.mem.push(f.id);
        }
        return true;
      });
      say("replayed " + replayed + " frame(s) · no re-embedding (vectors live in the WAL) · ready");
      render();
    });

    btns.reset.addEventListener("click", reset);
    reset();
  })();
})();
