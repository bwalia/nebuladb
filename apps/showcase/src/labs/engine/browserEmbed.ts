/**
 * A second, independent embedding model that runs in the visitor's
 * browser (transformers.js + ONNX/WASM): MiniLM-L6-v2, 384 dims, mean
 * pooled and normalised. The Labs use it to compare against the
 * server's embedder and to draw the embedding map, since the server
 * does not return stored vectors.
 *
 * The library is imported lazily so it never touches the main bundle,
 * and the ~23 MB of weights are fetched from the Hugging Face hub on
 * first use and cached by the browser.
 */
export const BROWSER_MODEL = "Xenova/all-MiniLM-L6-v2";
export const BROWSER_MODEL_LABEL = "MiniLM-L6-v2 (in your browser, 384-d)";

type Extractor = (texts: string[], opts: { pooling: "mean"; normalize: boolean }) => Promise<{ tolist(): number[][] }>;

let loading: Promise<Extractor> | null = null;

export function loadBrowserModel(onProgress?: (pct: number) => void): Promise<Extractor> {
  if (!loading) {
    loading = (async () => {
      const { pipeline } = await import("@huggingface/transformers");
      const ex = await pipeline("feature-extraction", BROWSER_MODEL, {
        progress_callback: (p: { status?: string; progress?: number }) => {
          if (p.status === "progress" && typeof p.progress === "number") onProgress?.(p.progress);
        },
      });
      return ex as unknown as Extractor;
    })().catch((e) => {
      loading = null;
      throw e;
    });
  }
  return loading;
}

const cache = new Map<string, number[]>();

/** Embed texts (cached by exact text), in batches so the UI stays responsive. */
export async function embedInBrowser(texts: string[], onProgress?: (done: number, total: number) => void): Promise<number[][]> {
  const ex = await loadBrowserModel();
  const todo = [...new Set(texts.filter((t) => !cache.has(t)))];
  const B = 16;
  for (let i = 0; i < todo.length; i += B) {
    const batch = todo.slice(i, i + B);
    const out = (await ex(batch, { pooling: "mean", normalize: true })).tolist();
    batch.forEach((t, j) => cache.set(t, out[j]));
    onProgress?.(Math.min(i + B, todo.length), todo.length);
    await new Promise((r) => setTimeout(r, 0));
  }
  return texts.map((t) => cache.get(t)!);
}
