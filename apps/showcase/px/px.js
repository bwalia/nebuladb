/*! NebulaDB pixel · served at https://px.nebuladb.net/px.js
 * <script async src="https://px.nebuladb.net/px.js" data-key="SITE_KEY"></script>
 * Options (attributes on the script tag):
 *   data-consent="required"  queue everything until nebulaPx('consent', true)
 *   data-inputs="1"          also record what is typed into plain text/search fields
 * API: nebulaPx('identify', id) · nebulaPx('track', type, {…}) · nebulaPx('consent', true|false)
 * Not sent at all when the browser asks for no tracking (Global Privacy Control or Do Not Track).
 * Port of apps/showcase/src/analytics.ts; the server (njs/analytics.js) adds IP and country. */
(function (root, factory) {
  var core = factory();
  if (typeof module === "object" && module.exports) module.exports = core;
  else if (root && root.document) core.start(root);
})(typeof window !== "undefined" ? window : this, function () {
  "use strict";
  var TYPES = ["page_view", "video_play", "video_progress", "video_complete", "video_stop", "click", "input", "session_end"];
  var SENSITIVE = /pass|pwd|email|e-mail|tel|phone|card|cc-|cvc|cvv|iban|sort.?code|account|ssn|secret|token|otp/i;

  /** Options from the script tag's data-* attributes. Inputs are off unless data-inputs="1". */
  function options(get) {
    return { key: get("data-key") || "", consent: get("data-consent") === "required", inputs: get("data-inputs") === "1" };
  }

  /** The browser asked not to be tracked. */
  function optedOut(nav, win) {
    return !!(nav && (nav.globalPrivacyControl === true || nav.doNotTrack === "1" || nav.doNotTrack === "yes")) || !!(win && win.doNotTrack === "1");
  }

  /** Should this field's value be recorded? Never passwords, emails, phone or card fields. */
  function captureInput(el, opts) {
    if (!opts || !opts.inputs || !el) return false;
    var tag = String(el.tagName || "").toLowerCase();
    var type = String(el.type || "text").toLowerCase();
    if (tag === "textarea") type = "text";
    else if (tag !== "input" || (type !== "text" && type !== "search")) return false;
    var hint = [el.name, el.id, el.autocomplete, el.placeholder, el.getAttribute && el.getAttribute("aria-label")].join(" ");
    return !SENSITIVE.test(hint);
  }

  /**
   * Consent gate (pure): offer(ev) returns the events to send now; grant()
   * returns everything queued; deny() drops the queue and everything after.
   */
  function gate(required) {
    var state = required ? "pending" : "granted";
    var queue = [];
    return {
      state: function () { return state; },
      offer: function (ev) {
        if (state === "granted") return [ev];
        if (state === "pending" && queue.length < 50) queue.push(ev);
        return [];
      },
      grant: function () { state = "granted"; var q = queue; queue = []; return q; },
      deny: function () { state = "denied"; queue = []; return []; },
    };
  }

  /** base64url of UTF-8 JSON, for the GIF's `d` parameter. */
  function encode(obj, btoaFn) {
    var json = JSON.stringify(obj);
    var b64 = btoaFn(unescape(encodeURIComponent(json)));
    return b64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  }

  function start(win) {
    var doc = win.document, nav = win.navigator, loc = win.location;
    var script = doc.currentScript || doc.querySelector('script[src*="/px.js"][data-key]');
    if (!script) return;
    var opts = options(function (n) { return script.getAttribute(n); });
    var base = (script.src.match(/^https?:\/\/[^/]+/) || [""])[0];
    var api = function () { (api.q = api.q || []).push(arguments); };
    var queued = (win.nebulaPx && win.nebulaPx.q) || [];
    win.nebulaPx = api;
    if (!opts.key || !base || optedOut(nav, win)) return;

    var g = gate(opts.consent);
    var started = Date.now();
    var sentFirst = false;
    var ls = function (store, k, set) {
      try {
        var s = win[store];
        if (set !== undefined) { if (set === null) s.removeItem(k); else s.setItem(k, set); return set; }
        var v = s.getItem(k);
        if (!v) { v = Math.random().toString(36).slice(2, 12); s.setItem(k, v); }
        return v;
      } catch (e) { return ""; }
    };
    var get = function (store, k) { try { return win[store].getItem(k) || ""; } catch (e) { return ""; } };

    function context() {
      var langs = nav.languages && nav.languages.length ? nav.languages : [nav.language || ""];
      var tz = "";
      try { tz = Intl.DateTimeFormat().resolvedOptions().timeZone || ""; } catch (e) { /* none */ }
      var m = /^[a-z]{2,3}[-_]([A-Za-z]{2})$/.exec(langs[0] || "");
      return {
        lang: nav.language || "", langs: Array.prototype.join.call(langs, ","), tz: tz,
        country_guess: m ? m[1].toUpperCase() : "",
        screen: win.screen ? win.screen.width + "x" + win.screen.height : "",
        viewport: win.innerWidth + "x" + win.innerHeight,
        session: ls("sessionStorage", "npx-s"), visitor: ls("localStorage", "npx-v"),
      };
    }

    function send(ev) {
      var body = JSON.stringify(ev);
      try {
        if (nav.sendBeacon && nav.sendBeacon(base + "/px", body)) return;
      } catch (e) { /* GIF below */ }
      var img = new Image(1, 1);
      img.src = base + "/px.gif?k=" + encodeURIComponent(opts.key) + "&e=" + encodeURIComponent(ev.type) + "&d=" + encode(ev, win.btoa);
    }

    function track(type, props) {
      if (TYPES.indexOf(type) === -1) return;
      var ev = context();
      ev.k = opts.key;
      ev.type = type;
      ev.path = (loc.pathname + loc.search).slice(0, 200);
      ev.title = doc.title;
      for (var p in props || {}) if (Object.prototype.hasOwnProperty.call(props, p)) ev[p] = props[p];
      ev.referrer = sentFirst ? "" : doc.referrer;
      var uid = get("localStorage", "npx-uid");
      if (uid) ev.uid = uid;
      sentFirst = true;
      g.offer(ev).forEach(send);
    }

    api = function (cmd, a, b) {
      if (cmd === "identify") ls("localStorage", "npx-uid", a ? String(a).slice(0, 200) : null);
      else if (cmd === "track") track(a, b);
      else if (cmd === "consent") (a ? g.grant() : g.deny()).forEach(send);
    };
    win.nebulaPx = api;

    /* pages, including single-page apps */
    var lastPage = "";
    function onPage() {
      setTimeout(function () {
        var key = loc.pathname + loc.search + loc.hash;
        if (key === lastPage) return;
        lastPage = key;
        var first = !get("sessionStorage", "npx-started");
        ls("sessionStorage", "npx-started", "1");
        track("page_view", first ? { first: true } : {});
      }, 50);
    }
    ["pushState", "replaceState"].forEach(function (fn) {
      var orig = win.history && win.history[fn];
      if (!orig) return;
      win.history[fn] = function () { var r = orig.apply(this, arguments); onPage(); return r; };
    });
    win.addEventListener("popstate", onPage);
    win.addEventListener("hashchange", onPage);

    /* videos: start, 25/50/75%, finish, stop */
    var vids = [];
    function vstate(v) {
      for (var i = 0; i < vids.length; i++) if (vids[i].v === v) return vids[i];
      var s = { v: v, played: false, watched: 0, last: v.currentTime, max: 0, marks: {}, done: false };
      vids.push(s);
      return s;
    }
    function vtitle(v) { return v.getAttribute("title") || v.getAttribute("aria-label") || String(v.currentSrc || v.src || "video").split("/").pop().slice(0, 160); }
    function onMedia(e) {
      var v = e.target;
      if (!v || v.tagName !== "VIDEO") return;
      var s = vstate(v), pct = v.duration > 0 ? Math.min(100, Math.round((v.currentTime / v.duration) * 100)) : 0;
      if (e.type === "play") {
        s.last = v.currentTime;
        if (!s.played) { s.played = true; track("video_play", { video: vtitle(v), pct: pct }); }
      } else if (e.type === "timeupdate") {
        var d = v.currentTime - s.last;
        if (d > 0 && d < 2) s.watched += d;
        s.last = v.currentTime;
        s.max = Math.max(s.max, pct);
        [25, 50, 75].forEach(function (m) {
          if (pct >= m && !s.marks[m]) { s.marks[m] = 1; track("video_progress", { video: vtitle(v), pct: m, seconds: Math.round(s.watched) }); }
        });
        if (pct >= 95 && !s.done) { s.done = true; track("video_complete", { video: vtitle(v), pct: 100, seconds: Math.round(s.watched) }); }
      } else if (s.played && !s.done && s.watched >= 1) {
        track("video_stop", { video: vtitle(v), pct: s.max, seconds: Math.round(s.watched) });
      }
    }
    ["play", "pause", "ended", "timeupdate"].forEach(function (t) { doc.addEventListener(t, onMedia, true); });

    /* clicks: the label only */
    doc.addEventListener("click", function (e) {
      var el = e.target && e.target.closest ? e.target.closest("button, a, [role=button]") : null;
      if (!el) return;
      var label = (el.getAttribute("aria-label") || el.textContent || "").replace(/\s+/g, " ").trim().slice(0, 80);
      if (label) track("click", { label: label });
    }, true);

    /* typed text: opt-in only, never sensitive fields */
    doc.addEventListener("focusout", function (e) {
      var el = e.target;
      if (!captureInput(el, opts)) return;
      var value = String(el.value || "").trim();
      if (value.length < 2 || el.getAttribute("data-npx-last") === value) return;
      el.setAttribute("data-npx-last", value);
      track("input", { label: (el.getAttribute("aria-label") || el.placeholder || el.name || "input").slice(0, 80), value: value.slice(0, 300) });
    }, true);

    /* leaving */
    var ended = false;
    doc.addEventListener("visibilitychange", function () {
      if (doc.visibilityState === "hidden" && !ended) { ended = true; track("session_end", { seconds: Math.round((Date.now() - started) / 1000) }); }
      else if (doc.visibilityState === "visible") ended = false;
    });

    for (var i = 0; i < queued.length; i++) api.apply(null, queued[i]);
    onPage();
  }

  return { options: options, optedOut: optedOut, captureInput: captureInput, gate: gate, encode: encode, start: start };
});
