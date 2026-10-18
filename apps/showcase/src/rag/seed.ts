/**
 * Idempotent demo-corpus seeding for the RAG apps.
 *
 * Each app ships a versioned corpus. On first open we check the last
 * doc of the corpus for the current `seed_version`; if it's missing or
 * stale we bulk-upsert the whole set (upserts are keyed by id, so a
 * re-seed replaces rather than duplicates). Docs the user added later
 * (posted jobs, new tickets) live under other ids and are untouched.
 */
import { api, ApiError } from "../api";

export interface SeedDoc {
  id: string;
  text: string;
  metadata: Record<string, unknown>;
}

export interface SeedResult {
  bucket: string;
  /** Docs written this call; 0 when the corpus was already current. */
  written: number;
  total: number;
}

const BATCH = 32;

/** In-flight seeds, so two tabs mounting at once don't double-write. */
const inflight = new Map<string, Promise<SeedResult>>();

export function ensureSeeded(
  bucket: string,
  docs: SeedDoc[],
  version: string,
  onProgress?: (done: number, total: number) => void
): Promise<SeedResult> {
  const key = `${bucket}@${version}`;
  const existing = inflight.get(key);
  if (existing) return existing;
  const p = seed(bucket, docs, version, onProgress).finally(() => inflight.delete(key));
  inflight.set(key, p);
  return p;
}

async function seed(
  bucket: string,
  docs: SeedDoc[],
  version: string,
  onProgress?: (done: number, total: number) => void
): Promise<SeedResult> {
  const sentinel = docs[docs.length - 1];
  if (sentinel && (await isCurrent(bucket, sentinel.id, version))) {
    return { bucket, written: 0, total: docs.length };
  }
  let written = 0;
  for (let i = 0; i < docs.length; i += BATCH) {
    const batch = docs.slice(i, i + BATCH).map((d) => ({
      ...d,
      metadata: { ...d.metadata, seed_version: version },
    }));
    const r = await api.bulkDocs(bucket, batch);
    written += r.inserted;
    onProgress?.(Math.min(i + BATCH, docs.length), docs.length);
  }
  return { bucket, written, total: docs.length };
}

async function isCurrent(bucket: string, id: string, version: string): Promise<boolean> {
  try {
    const d = await api.getDoc(bucket, id);
    return d.metadata?.seed_version === version;
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) return false;
    throw e;
  }
}
