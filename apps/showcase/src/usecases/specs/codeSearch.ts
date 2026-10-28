/**
 * Code Search — natural-language questions over a repo's functions,
 * config and architecture decision records; the chat explains with
 * file citations. Snippets are from a fictional payments service.
 */
import type { SeedDoc } from "../../rag/seed";
import type { UseCaseSpec } from "../UseCaseDemo";

const LANGS = ["rust", "typescript", "sql", "yaml", "adr"] as const;

function c(id: string, path: string, lang: string, symbol: string, body: string): SeedDoc {
  return {
    id,
    text: `${path} · ${symbol} (${lang})\n${body}`,
    metadata: { chunk_id: id, path, lang, symbol },
  };
}

const DOCS: SeedDoc[] = [
  c("C-001", "services/payments/src/retry.rs", "rust", "fn retry_with_backoff",
    "/// Retries an idempotent call with exponential backoff and full jitter.\n/// Max 5 attempts, base 100ms, cap 5s. Non-retryable: 4xx except 409/429.\npub async fn retry_with_backoff<F, T>(op: F) -> Result<T, Error> { ... }"),
  c("C-002", "services/payments/src/idempotency.rs", "rust", "struct IdempotencyStore",
    "/// Stores request hash + response by Idempotency-Key for 24h in Redis.\n/// A replay with a different body hash returns 422 IDEMPOTENCY_MISMATCH.\npub struct IdempotencyStore { ttl: Duration, ... }"),
  c("C-003", "services/payments/src/webhook.rs", "rust", "fn verify_signature",
    "/// Verifies HMAC-SHA256 over `timestamp.body` with the provider secret.\n/// Rejects timestamps older than 300s to prevent replay. Constant-time compare.\npub fn verify_signature(header: &str, body: &[u8], secret: &[u8]) -> bool { ... }"),
  c("C-004", "services/payments/src/ledger.rs", "rust", "fn post_double_entry",
    "/// Posts balanced debit/credit legs in one transaction; panics in debug if\n/// legs do not sum to zero. Amounts are i64 minor units, never floats.\npub fn post_double_entry(tx: &mut Tx, legs: &[Leg]) -> Result<EntryId> { ... }"),
  c("C-005", "web/checkout/src/hooks/useCheckout.ts", "typescript", "useCheckout",
    "// Generates an Idempotency-Key per checkout attempt (uuid v7) and reuses it\n// on network retry so the server deduplicates double clicks.\nexport function useCheckout(cartId: string) { ... }"),
  c("C-006", "web/checkout/src/lib/money.ts", "typescript", "formatMoney",
    "// Formats minor units with Intl.NumberFormat; currency exponent from ISO 4217\n// table (JPY=0, GBP=2, KWD=3). Never divide by 100 blindly.\nexport function formatMoney(minor: bigint, currency: string): string { ... }"),
  c("C-007", "db/migrations/0042_refunds.sql", "sql", "TABLE refunds",
    "CREATE TABLE refunds (id uuid PRIMARY KEY, payment_id uuid REFERENCES payments(id),\n amount_minor bigint CHECK (amount_minor > 0), status text NOT NULL,\n created_at timestamptz DEFAULT now());\nCREATE INDEX refunds_payment_idx ON refunds(payment_id);"),
  c("C-008", "db/migrations/0043_partial_refund_guard.sql", "sql", "TRIGGER refund_total_guard",
    "-- Prevents the sum of refunds exceeding the captured amount for a payment.\nCREATE TRIGGER refund_total_guard BEFORE INSERT ON refunds FOR EACH ROW EXECUTE FUNCTION check_refund_total();"),
  c("C-009", "deploy/payments/values.yaml", "yaml", "rateLimit",
    "rateLimit:\n  perMerchantRps: 50\n  burst: 100\n  # 429 responses carry Retry-After; clients must honour it (see retry.rs)"),
  c("C-010", "deploy/payments/values.yaml", "yaml", "secrets",
    "secrets:\n  webhookSigning: vault:payments/webhook#secret\n  # rotated every 90 days; both old and new secrets are accepted for 24h"),
  c("C-011", "docs/adr/0007-idempotency.md", "adr", "ADR-0007 Idempotency keys",
    "Decision: all mutating payment endpoints require an Idempotency-Key header. Rationale: mobile clients retry on flaky networks and duplicate charges were 0.03% of volume. Alternatives rejected: server-side dedupe on amount+card (false positives)."),
  c("C-012", "docs/adr/0011-money-type.md", "adr", "ADR-0011 Money as integer minor units",
    "Decision: represent money as i64/bigint minor units plus ISO currency. Floats caused a £0.01 reconciliation drift in 2024. All APIs accept and return minor units."),
];

export const codeSearchSpec: UseCaseSpec = {
  bucket: "code_chunks",
  seedVersion: "code-v1",
  docs: DOCS,
  noun: "code chunks",
  title: "Code Search",
  subtitle: "Natural-language question → functions, migrations, config and ADRs → explained with file citations",
  queryLabel: "What are you trying to find or understand?",
  placeholder: "e.g. where do we stop duplicate charges?",
  scenarios: [
    { label: "Duplicate charges", q: "How do we stop a customer being charged twice when they double click pay?" },
    { label: "Webhook security", q: "How are incoming provider webhooks authenticated and protected from replay?" },
    { label: "Money rounding", q: "Why do we never use floats for amounts, and where is formatting done?" },
    { label: "Over-refund guard", q: "What stops a refund exceeding the original payment?", filters: { lang: "sql" } },
    { label: "429 handling", q: "What happens when a merchant hits the rate limit and how should clients retry?" },
  ],
  filters: [{ key: "lang", label: "Language", kind: "select", options: LANGS }],
  idKey: "chunk_id",
  titleKey: "path",
  subKeys: ["symbol", "lang"],
  resultsTitle: "Matching code",
  chat: {
    title: "Code explainer",
    subtitle: "Cites file paths; says when the code isn't in the results",
    system:
      "You are a staff engineer onboarding a colleague to this codebase. Answer ONLY from the cited code chunks [n]; always name the file path and symbol. " +
      "Explain how pieces fit together end to end. If the answer needs code that is not in the excerpts, say which file is likely missing rather than guessing.",
    suggestions: ["Walk me through the request end to end.", "Where would I add a test for this?", "What's the design rationale?"],
    placeholder: "Ask about this code…",
  },
};
