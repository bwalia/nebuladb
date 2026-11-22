# Engagement pixel (px.nebuladb.net)

Other websites report visits and actions to NebulaDB through a small script
(or a 1×1 GIF), and the internal **Engagement** page shows them next to the
Source Supplies demo site.

## Pieces

| Piece | Where |
|---|---|
| Snippet (`/px.js`) | `apps/showcase/px/px.js`, copied to `/usr/share/nginx/px/` in the showcase image |
| Collector (`/px.gif`, `/px`) | `apps/showcase/njs/analytics.js` (`pxGif`, `pxPost`), nginx server `px.nebuladb.net` in `apps/showcase/nginx.conf` |
| Sites and keys | Helm `showcase.pixel.sites` → env `SHOWCASE_PIXEL_SITES` |
| Storage | NebulaDB bucket `web_events` (through `/_px/write`; the API key stays in nginx) |
| Report | `apps/showcase/src/tabs/EngagementTab.tsx`, sites listed in `src/tabs/engagementSites.ts` |
| Edge | `deploy/edge/wslproxy-server-px.json` (rule `nebuladb-showcase-sites`, `wsl_client_headers` hook for the real IP and country) |

## Embed

```html
<script>window.nebulaPx=window.nebulaPx||function(){(nebulaPx.q=nebulaPx.q||[]).push(arguments)};</script>
<script async src="https://px.nebuladb.net/px.js" data-key="SITE_KEY"></script>
<noscript><img src="https://px.nebuladb.net/px.gif?k=SITE_KEY&e=page_view" alt="" width="1" height="1"></noscript>
```

- `data-consent="required"`: nothing is sent until `nebulaPx('consent', true)`; `nebulaPx('consent', false)` drops the queue.
- `data-inputs="1"`: also record what is typed into plain text and search fields (off by default; never passwords, email, phone or card fields).
- `nebulaPx('identify', 'customer id or email')`: later events carry that id; the visitor's earlier anonymous events are grouped with it on the Engagement page.
- `nebulaPx('track', 'click', {label: '…'})`: report an event yourself (types: page_view, video_play, video_progress, video_complete, video_stop, click, input, session_end).
- Browsers with Global Privacy Control or Do Not Track on send nothing.

Recorded automatically: page views (including single-page-app navigation), video start / 25 / 50 / 75 % / finish / stop, link and button clicks (label only), and time on site when the tab is left.

## Adding a site

1. Make a key: `echo ws_$(openssl rand -hex 8)`. It is public (it sits in the site's HTML); the origin list is what stops other sites using it.
2. Add it to `showcase.pixel.sites` in `deploy/helm/nebuladb/values-prod.yaml` with `host`, `label` and `origins`.
3. Add the host to `SITES` in `apps/showcase/src/tabs/engagementSites.ts` with bucket `web_events`.
4. To be told in Slack when a video starts (not only when one is finished), set `videoStarts: true` on the site.
5. Deploy with `deploy-k3s.yml` (`DEPLOYMENT_TYPE=showcase-only`), then paste the snippet into the site.

An unknown key, or a request whose Origin (or Referer, for the GIF) is not on the site's list, gets the normal response and nothing is stored.

## Limits

10 requests/s per visitor IP (burst 30); GIF payload ≤ 4 KB; POST body ≤ 8 KB; event types and fields are whitelisted and length-capped.

## Privacy notice text for the site owner

Each event stores the visitor's IP address and country. Add something like this to the site's privacy notice:

> **Website analytics.** When you visit this site we record the pages you view, the videos you watch, the buttons and links you click and how long you stay, together with your IP address, the country it is located in, your browser's language and device type. We use this to understand which content is useful and to follow up on enquiries. The data is processed for us by NebulaDB (px.nebuladb.net) and kept for [retention period]. If you sign in or give us your email, we may link this activity to you. We do not record anything when your browser sends a Global Privacy Control or Do Not Track signal. To ask for your data or for it to be deleted, contact us.
