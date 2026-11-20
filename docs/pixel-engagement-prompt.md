# Prompt: pixel engagement tracking for another website

Reusable prompt for adding a website to the internal Engagement page. The site
reports visits and actions through a tracking pixel / beacon, and every event is
saved in NebulaDB through the API. Replace `<SITE_HOST>`, `<SITE_KEY>` and
`<SITE_LABEL>` before use.

Design choices baked in:

- **Public collector on its own host.** `/a/event` sits behind the sign-in gate
  and can't accept cross-site hits.
- **Public site key plus an origin allowlist.** The NebulaDB API key stays
  inside nginx.
- **sendBeacon first, 1×1 GIF as fallback.** The GIF also covers `<noscript>`.
- **Separate `web_events` bucket.** Public, anonymous data is kept apart from
  signed-in demo data.
- **Input capture off by default, with consent support.** UK/EU rules require
  consent before tracking IP-level visits on most sites.

---

```text
Add a "pixel" engagement tracker for another website, <SITE_HOST> (label "<SITE_LABEL>").
Every visit and action on that site must be saved in NebulaDB through an API call, and it
must show up in the internal Engagement page alongside sourcesupplies.

## What already exists (read these first, reuse them, don't rebuild)
- apps/showcase/njs/analytics.js: njs endpoint POST /a/event. buildDoc() whitelists and caps
  the browser fields, then stamps user, IP and country server-side. It writes through the
  internal location /_a/write, which proxies to NebulaDB
  POST /api/v1/bucket/showcase_events/doc with the bearer injected by nginx. It also sends
  Slack alerts (alertText, ignoredUser, SHOWCASE_SLACK_WEBHOOK).
- apps/showcase/src/analytics.ts: browser capture (page views, video 25/50/75/complete/stop,
  clicks, inputs, session_end; context lang/langs/tz/country_guess/screen/viewport/
  session/visitor).
- apps/showcase/src/tabs/EngagementTab.tsx: the internal report. It reads showcase_events
  with SQL semantic_match + `host = …`; its site dropdown is hard-coded.
- apps/showcase/nginx.conf, which MUST stay identical to
  deploy/helm/nebuladb/files/showcase/default.conf.template.
- The Dockerfile copies njs/*.js and declares `env` vars for njs.
- The real client IP and country come from the wslproxy edge hook `wsl_client_headers`
  (headers X-WSL-Client-IP and X-WSL-Geo-Country). Traefik rewrites XFF, so the pod only
  sees 10.42.x.
- Tests: apps/showcase/src/authgate/analytics.test.ts (node:test, imports the njs module).

## What to build
1. Public collector (no sign-in gate) on the showcase nginx. Use a dedicated host,
   px.nebuladb.net (preferred), or `location = /px` outside the auth gate:
   - GET /px.gif?k=<SITE_KEY>&e=<type>&d=<base64url JSON>
     - Returns a 1x1 transparent GIF (43 bytes), always 200, Cache-Control: no-store.
     - Works from an <img> and from <noscript>.
   - POST /px (text/plain JSON, via sendBeacon) with the same fields, returning 204.
     - Use text/plain so there's no CORS preflight.
     - Answer OPTIONS anyway.
     - Access-Control-Allow-Origin must echo only an allowed origin.
   - Validate the site key against a server-side map, env SHOWCASE_PIXEL_SITES, for example
     {"<SITE_KEY>":{"host":"<SITE_HOST>","origins":["https://<SITE_HOST>"]}}.
     - The site key is public, NOT a secret.
     - Reject an unknown key, or an Origin/Referer that isn't on the list: silently return the
       GIF and don't write.
   - Reuse buildDoc(): factor it so the pixel path passes host = the site's configured host
     (never the Host header), site = <SITE_LABEL>, and user = the identified id or "anonymous".
   - Write to a new bucket, web_events, through the same internal proxy pattern
     (/_px/write). Never expose the NebulaDB API key to the browser.
   - Abuse limits:
     - limit_req per IP (e.g. 10 r/s, burst 30);
     - query-string payload ≤ 4 KB and POST body ≤ 8 KB;
     - keep the existing event-type whitelist; drop unknown fields.
2. Embeddable snippet, served by the collector at GET /px.js (cacheable, ~3 KB, no deps):
       <script async src="https://px.nebuladb.net/px.js" data-key="<SITE_KEY>"></script>
       <noscript><img src="https://px.nebuladb.net/px.gif?k=<SITE_KEY>&e=page_view" alt=""
            width="1" height="1"></noscript>
   - Port the capture logic from src/analytics.ts as a framework-free IIFE:
     - page_view, including SPA history.pushState/popstate/hashchange;
     - video progress;
     - clicks (label only);
     - session_end.
   - Turn input capture OFF by default (opt in with data-inputs="1"), and never capture
     password/email/tel/card fields.
   - Public API: window.nebulaPx('identify', '<email or customer id>') and
     window.nebulaPx('track', '<type>', {...}).
   - Consent:
     - If data-consent="required", queue events until nebulaPx('consent', true) is called.
     - Respect navigator.globalPrivacyControl and DNT by not sending at all.
   - Transport: sendBeacon POST first, then the GIF fallback.
3. Edge: add the `api_gw` wsl_client_headers hook to the px host's wslproxy server record,
   following deploy/edge/wslproxy-server-sourcesupplies.json and
   .github/wslproxy/data/servers/prod/host:*.json. Register it with
   register-edge-vhost.yml.
   - lon1 has no cp sync: patch /opt/nginx/data/servers/prod/host:<host>.json directly.
   - Back the file up first, and keep owner www-data:root and mode 777.
4. Engagement page:
   - Replace the hard-coded site <select> with a list from a SITES config that includes
     <SITE_HOST>.
   - Read web_events for pixel sites and showcase_events for sourcesupplies.
   - Show anonymous visitors grouped by `visitor` until they're identified.
   - Add a "Top pages" and "Referrers" summary for the selected site.
5. Slack (optional, behind values): extend alertText for <SITE_LABEL>, alerting on an
   identified visit or a finished video only, and honour SHOWCASE_ALERT_IGNORE.
6. Helm:
   - values.yaml gets showcase.pixel {enabled:false, sites:{}}; values-prod.yaml enables it.
   - The deployment gets env SHOWCASE_PIXEL_SITES.
   - Add the px host to the showcase ingress.

## Tests and verification
- Unit tests (analytics.test.ts):
  - key/origin validation;
  - decoding the GIF query payload;
  - host taken from config, not the Host header;
  - input capture off by default;
  - consent queueing (pure function).
- Run a local docker build of the showcase image against a local nebula-server, then:
  - curl the GIF and the POST paths;
  - check a doc lands in web_events with ip/country;
  - check a bad key or origin writes nothing.
- Test page: scratchpad HTML that embeds the snippet, served from a different origin,
  showing that events arrive.
- After deploy:
  - one external request from outside the LAN confirms the real IP and GB country;
  - then DELETE the test docs (DELETE /api/v1/bucket/web_events/doc/<id>).

## Hard rules
- NO prod deploy that can cause a 503. Use only:
      gh workflow run deploy-k3s.yml -f TARGET_ENV=prod -f DEPLOYMENT_TYPE=showcase-only
  Never run a full helm upgrade on prod, and don't restart nebula-server.
- bwalia/nebuladb is PUBLIC: no secrets, hashes, webhooks or private emails in commits.
  Site keys are fine because they're public by design.
- Keep nginx.conf and the Helm template byte-identical.
- Work on a branch, open a PR, and merge and deploy only when I say so.
- Privacy: add a short line to the site's privacy notice (provide the text for the site
  owner), because we store IP and country for every visit.

Report back with:
- the snippet to paste into <SITE_HOST>;
- the PR link;
- a screenshot or description of the Engagement page showing the site.
```
