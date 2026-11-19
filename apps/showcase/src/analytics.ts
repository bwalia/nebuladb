/**
 * Visitor analytics for the customer demo sites.
 *
 * The browser reports WHAT happened (page, video progress, clicks, what was
 * typed into a demo). The showcase nginx (njs/analytics.js) adds WHO and
 * WHERE from things the browser can't fake: the signed-in identity, the
 * client IP and the edge's IP-to-country lookup. Events land in the
 * NebulaDB bucket `showcase_events`; the internal Engagement page reads it.
 *
 * Only runs on customer sites (sourcesupplies.…), so internal testing on
 * the combined showcase doesn't pollute the numbers.
 */
import { isCustomerSite } from "./tenant";

export type EventType =
  | "page_view"
  | "video_play"
  | "video_progress"
  | "video_complete"
  | "video_stop"
  | "click"
  | "input"
  | "session_end";

export interface TrackProps {
  path?: string;
  title?: string;
  demo?: string;
  video?: string;
  pct?: number;
  seconds?: number;
  label?: string;
  value?: string;
}

const ENDPOINT = "/a/event";
const started = Date.now();
let sentFirst = false;

function stored(store: () => Storage, key: string): string {
  try {
    const s = store();
    let v = s.getItem(key);
    if (!v) {
      v = Math.random().toString(36).slice(2, 12);
      s.setItem(key, v);
    }
    return v;
  } catch {
    return "";
  }
}

/* Country from browser settings, used when the edge has no IP country.
 * Timezone is the better signal (language "en-US" is common everywhere);
 * the language region is the fallback. */
const TZ_COUNTRY: Record<string, string> = {
  "Europe/London": "GB", "Europe/Belfast": "GB", "Europe/Dublin": "IE", "Europe/Paris": "FR", "Europe/Berlin": "DE",
  "Europe/Madrid": "ES", "Europe/Rome": "IT", "Europe/Amsterdam": "NL", "Europe/Brussels": "BE", "Europe/Lisbon": "PT",
  "Europe/Zurich": "CH", "Europe/Vienna": "AT", "Europe/Stockholm": "SE", "Europe/Oslo": "NO", "Europe/Copenhagen": "DK",
  "Europe/Helsinki": "FI", "Europe/Warsaw": "PL", "Europe/Prague": "CZ", "Europe/Budapest": "HU", "Europe/Bucharest": "RO",
  "Europe/Athens": "GR", "Europe/Istanbul": "TR", "Europe/Kiev": "UA", "Europe/Kyiv": "UA", "Europe/Moscow": "RU",
  "America/New_York": "US", "America/Chicago": "US", "America/Denver": "US", "America/Los_Angeles": "US", "America/Phoenix": "US",
  "America/Toronto": "CA", "America/Vancouver": "CA", "America/Mexico_City": "MX", "America/Sao_Paulo": "BR",
  "America/Argentina/Buenos_Aires": "AR", "America/Bogota": "CO", "America/Lima": "PE", "America/Santiago": "CL",
  "Asia/Kolkata": "IN", "Asia/Calcutta": "IN", "Asia/Karachi": "PK", "Asia/Dhaka": "BD", "Asia/Dubai": "AE", "Asia/Riyadh": "SA",
  "Asia/Bangkok": "TH", "Asia/Singapore": "SG", "Asia/Kuala_Lumpur": "MY", "Asia/Jakarta": "ID", "Asia/Manila": "PH",
  "Asia/Ho_Chi_Minh": "VN", "Asia/Hong_Kong": "HK", "Asia/Shanghai": "CN", "Asia/Taipei": "TW", "Asia/Seoul": "KR",
  "Asia/Tokyo": "JP", "Asia/Jerusalem": "IL", "Australia/Sydney": "AU", "Australia/Melbourne": "AU", "Australia/Perth": "AU",
  "Australia/Brisbane": "AU", "Pacific/Auckland": "NZ", "Africa/Johannesburg": "ZA", "Africa/Lagos": "NG", "Africa/Cairo": "EG",
  "Africa/Nairobi": "KE",
};

export function countryFromBrowser(tz: string, langs: readonly string[]): string {
  if (TZ_COUNTRY[tz]) return TZ_COUNTRY[tz];
  for (const l of langs) {
    const m = /^[a-z]{2,3}[-_]([A-Za-z]{2})$/.exec(l);
    if (m) return m[1].toUpperCase();
  }
  return "";
}

function context() {
  let tz = "";
  try {
    tz = Intl.DateTimeFormat().resolvedOptions().timeZone || "";
  } catch {
    tz = "";
  }
  const langs = navigator.languages?.length ? navigator.languages : [navigator.language];
  return {
    site: "sourcesupplies",
    lang: navigator.language || "",
    langs: langs.join(","),
    tz,
    country_guess: countryFromBrowser(tz, langs),
    screen: `${window.screen.width}x${window.screen.height}`,
    viewport: `${window.innerWidth}x${window.innerHeight}`,
    session: stored(() => sessionStorage, "ss-a-session"),
    visitor: stored(() => localStorage, "ss-a-visitor"),
  };
}

export function track(type: EventType, props: TrackProps = {}): void {
  if (!isCustomerSite()) return;
  const body = JSON.stringify({
    type,
    ...context(),
    path: props.path ?? window.location.hash ?? "",
    title: props.title ?? document.title,
    ...props,
    referrer: sentFirst ? "" : document.referrer,
  });
  sentFirst = true;
  try {
    if (navigator.sendBeacon?.(ENDPOINT, new Blob([body], { type: "application/json" }))) return;
  } catch {
    /* fall through to fetch */
  }
  void fetch(ENDPOINT, { method: "POST", body, keepalive: true, credentials: "same-origin", headers: { "Content-Type": "application/json" } }).catch(
    () => undefined
  );
}

/* ------------------------------ auto-capture ------------------------------ */

interface VideoState {
  played: boolean;
  watched: number;
  last: number;
  maxPct: number;
  marks: Set<number>;
  done: boolean;
}

/**
 * Wire page views, video progress, button clicks and demo inputs once for
 * the whole page. `videoTitle` maps a video src to its human title.
 */
export function startAutoCapture(videoTitle: (src: string) => string | undefined): () => void {
  if (!isCustomerSite()) return () => undefined;
  const videos = new WeakMap<HTMLVideoElement, VideoState>();
  const title = (v: HTMLVideoElement) => videoTitle(new URL(v.currentSrc || v.src, location.href).pathname) ?? v.currentSrc.split("/").pop() ?? "video";
  const pct = (v: HTMLVideoElement) => (v.duration > 0 ? Math.min(100, Math.round((v.currentTime / v.duration) * 100)) : 0);
  const state = (v: HTMLVideoElement): VideoState => {
    let s = videos.get(v);
    if (!s) {
      s = { played: false, watched: 0, last: v.currentTime, maxPct: 0, marks: new Set(), done: false };
      videos.set(v, s);
    }
    return s;
  };

  const onMedia = (e: Event) => {
    const v = e.target;
    if (!(v instanceof HTMLVideoElement)) return;
    const s = state(v);
    if (e.type === "play") {
      s.last = v.currentTime;
      if (!s.played) {
        s.played = true;
        track("video_play", { video: title(v), pct: pct(v) });
      }
    } else if (e.type === "timeupdate") {
      const d = v.currentTime - s.last;
      if (d > 0 && d < 2) s.watched += d;
      s.last = v.currentTime;
      const p = pct(v);
      s.maxPct = Math.max(s.maxPct, p);
      for (const m of [25, 50, 75]) {
        if (p >= m && !s.marks.has(m)) {
          s.marks.add(m);
          track("video_progress", { video: title(v), pct: m, seconds: Math.round(s.watched) });
        }
      }
      if (p >= 95 && !s.done) {
        s.done = true;
        track("video_complete", { video: title(v), pct: 100, seconds: Math.round(s.watched) });
      }
    } else if (e.type === "pause" || e.type === "ended") {
      // A video closed mid-way fires "pause" as it leaves the page.
      if (s.played && !s.done && s.watched >= 1) track("video_stop", { video: title(v), pct: s.maxPct, seconds: Math.round(s.watched) });
    }
  };

  const onClick = (e: MouseEvent) => {
    const el = (e.target as Element | null)?.closest?.("button, a, [role=button]");
    if (!el) return;
    const label = (el.getAttribute("aria-label") || el.textContent || "").replace(/\s+/g, " ").trim().slice(0, 80);
    if (label) track("click", { label });
  };

  const onInput = (e: FocusEvent) => {
    const el = e.target;
    if (!(el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement)) return;
    if (el instanceof HTMLInputElement && !["text", "search", ""].includes(el.type)) return;
    const value = el.value.trim();
    if (value.length < 2 || el.dataset.aLast === value) return;
    el.dataset.aLast = value;
    const label = (el.getAttribute("aria-label") || el.placeholder || el.name || "input").slice(0, 80);
    track("input", { label, value: value.slice(0, 300) });
  };

  let lastPage = "";
  const onPage = () => {
    // Let the route's effect set document.title first.
    setTimeout(() => {
      const key = location.hash;
      if (key === lastPage) return;
      lastPage = key;
      track("page_view");
    }, 50);
  };

  let ended = false;
  const onHide = () => {
    if (document.visibilityState === "hidden" && !ended) {
      ended = true;
      track("session_end", { seconds: Math.round((Date.now() - started) / 1000) });
    } else if (document.visibilityState === "visible") {
      ended = false;
    }
  };

  const media = ["play", "pause", "ended", "timeupdate"];
  media.forEach((t) => document.addEventListener(t, onMedia, true));
  document.addEventListener("click", onClick, true);
  document.addEventListener("focusout", onInput, true);
  window.addEventListener("hashchange", onPage);
  document.addEventListener("visibilitychange", onHide);
  onPage();
  return () => {
    media.forEach((t) => document.removeEventListener(t, onMedia, true));
    document.removeEventListener("click", onClick, true);
    document.removeEventListener("focusout", onInput, true);
    window.removeEventListener("hashchange", onPage);
    document.removeEventListener("visibilitychange", onHide);
  };
}
