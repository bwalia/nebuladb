/**
 * Showcase Google identity (oauth2-proxy) + visit logging.
 *
 * When SHOWCASE_GOOGLE_AUTH is on, nginx gates every page with
 * auth_request → oauth2-proxy. After login the browser can read
 * /oauth2/userinfo (same-origin cookie). We upsert one doc per email
 * into `showcase_visitors` so operators can see who opened the UI.
 */

import { api } from "./api";

export interface ShowcaseUser {
  email: string;
  user: string;
  preferredUsername?: string;
}

type UserinfoJson = {
  email?: string;
  user?: string;
  preferredUsername?: string;
};

/** null when Google auth is off or the session is anonymous. */
export async function fetchShowcaseUser(): Promise<ShowcaseUser | null> {
  try {
    const resp = await fetch("/oauth2/userinfo", { credentials: "same-origin" });
    if (!resp.ok) return null;
    const body = (await resp.json()) as UserinfoJson;
    const email = (body.email || body.user || "").trim();
    if (!email) return null;
    return {
      email,
      user: (body.user || email).trim(),
      preferredUsername: body.preferredUsername?.trim() || undefined,
    };
  } catch {
    return null;
  }
}

const VISIT_KEY_PREFIX = "showcase-visit-logged:";

/** One upsert per browser session per email into `showcase_visitors`. */
export async function logShowcaseVisit(user: ShowcaseUser): Promise<void> {
  const key = `${VISIT_KEY_PREFIX}${user.email.toLowerCase()}`;
  try {
    if (sessionStorage.getItem(key)) return;
  } catch {
    /* private mode — still attempt the write once this load */
  }

  const id = user.email
    .toLowerCase()
    .replace(/[^a-z0-9@._+-]/g, "_")
    .slice(0, 200);
  const now = new Date().toISOString();
  try {
    await api.upsertDoc("showcase_visitors", id, `${user.email} viewed the NebulaDB showcase`, {
      email: user.email,
      user: user.user,
      preferred_username: user.preferredUsername ?? null,
      last_seen: now,
      user_agent: navigator.userAgent.slice(0, 240),
    });
    try {
      sessionStorage.setItem(key, now);
    } catch {
      /* ignore */
    }
  } catch {
    /* visit log is best-effort; never block the UI */
  }
}

export function showcaseSignOutUrl(): string {
  // Land on the branded login page (not `/`, which would bounce straight
  // back into Google when the IdP session is still warm).
  const rd = encodeURIComponent(window.location.origin + "/login");
  return `/oauth2/sign_out?rd=${rd}`;
}
