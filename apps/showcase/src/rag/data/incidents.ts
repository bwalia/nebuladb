/**
 * Incident Copilot corpus: fictional postmortems and runbooks for a UK
 * fintech ("Larkspur Pay"). Facts are dense on purpose — retrieval and
 * the triage vote both feed off them.
 */
import type { SeedDoc } from "../seed";

export const SEED_VERSION = "incidents-v1";
export const INCIDENT_BUCKET = "sre_incidents";
export const RUNBOOK_BUCKET = "sre_runbooks";

export type RootCause =
  | "deploy"
  | "capacity"
  | "dependency"
  | "config"
  | "certificate"
  | "data"
  | "network";

export const ROOT_CAUSES: RootCause[] = [
  "deploy",
  "capacity",
  "dependency",
  "config",
  "certificate",
  "data",
  "network",
];

interface Pm {
  id: string;
  title: string;
  service: string;
  severity: 1 | 2 | 3;
  started_at: string;
  mttr_minutes: number;
  root_cause: RootCause;
  signals: string[];
  fix: string;
  team: string;
  summary: string;
  alerts: string;
  timeline: string;
  cause: string;
  followups: string;
}

const PMS: Pm[] = [
  {
    id: "INC-2025-001",
    title: "Card authorisations failing after payments-api v4.12 rollout",
    service: "payments-api", severity: 1, started_at: "2025-01-14", mttr_minutes: 38,
    root_cause: "deploy", signals: ["5xx", "error-rate", "latency"], team: "payments",
    fix: "Rolled back to v4.11.3; re-shipped with feature flag defaulting off",
    summary: "23% of card authorisations returned HTTP 502 for 38 minutes during the Tuesday lunchtime peak. Approx. £1.9m of payment volume was declined or retried.",
    alerts: "PaymentsApi5xxRateHigh (5xx > 5% for 3m), CheckoutConversionDrop, p99 latency 4.2s on POST /v2/authorisations.",
    timeline: "12:02 canary at 10% → 12:09 promoted to 100% by Argo Rollouts (analysis passed on low traffic) → 12:11 alert fired → 12:24 IC declared SEV1 → 12:31 rollback started → 12:40 error rate normal.",
    cause: "v4.12 added a synchronous call to the new fraud-scoring client with a 5s timeout and no circuit breaker. The fraud service's p99 was 3.8s under peak, saturating the payments-api worker pool (200 threads).",
    followups: "Canary analysis now requires ≥2,000 requests; fraud call made async with 300ms budget; add bulkhead per downstream.",
  },
  {
    id: "INC-2025-002",
    title: "Ledger Postgres primary connection pool exhausted",
    service: "ledger-postgres", severity: 1, started_at: "2025-02-03", mttr_minutes: 52,
    root_cause: "capacity", signals: ["connection-pool", "latency", "5xx", "timeouts"], team: "core-ledger",
    fix: "Raised PgBouncer default_pool_size 80→140, killed idle-in-transaction sessions, added statement_timeout",
    summary: "Ledger writes stalled; payouts and balance reads timed out. PgBouncer showed cl_waiting > 900.",
    alerts: "PgBouncerClientsWaiting > 200, LedgerWriteLatencyP99 > 2s, payments-api 5xx on balance check.",
    timeline: "09:00 month-start payout batch kicked off → 09:06 waiting clients climb → 09:14 SEV1 → 09:30 found 120 'idle in transaction' sessions from reconciliation workers → 09:41 terminated them → 09:58 recovered.",
    cause: "Month-start payout batch doubled concurrent workers; a reconciliation job held transactions open across an HTTP call, pinning server connections. Pool sized for normal load.",
    followups: "idle_in_transaction_session_timeout = 30s; batch concurrency limit; dashboards for pool saturation by application_name.",
  },
  {
    id: "INC-2025-003",
    title: "api.larkspurpay.co.uk TLS certificate expired",
    service: "k8s-ingress", severity: 1, started_at: "2025-02-21", mttr_minutes: 71,
    root_cause: "certificate", signals: ["cert-expiry", "tls-handshake", "5xx"], team: "platform",
    fix: "Manually renewed via cert-manager after fixing DNS-01 solver credentials; rotated Cloudflare API token",
    summary: "All partner API traffic failed TLS handshakes from 00:00 UTC. Mobile apps showed 'cannot connect'.",
    alerts: "SyntheticHttpsCheckFailed (api.larkspurpay.co.uk), IngressTLSHandshakeErrors, partner escalation from two merchants.",
    timeline: "00:00 cert expired → 00:07 synthetic alert paged on-call → 00:25 found cert-manager Certificate stuck 'Issuing' for 29 days → 00:50 Cloudflare token revoked in January cleanup → 01:11 renewed.",
    cause: "cert-manager DNS-01 solver used a Cloudflare API token that was revoked during a credentials clean-up. Renewal had been failing silently for 29 days; no alert on Certificate readiness.",
    followups: "Alert on certmanager_certificate_expiration_timestamp_seconds < 14 days; secrets owners register; quarterly token audit.",
  },
  {
    id: "INC-2025-004",
    title: "Kafka consumer lag on payment-events delayed notifications",
    service: "kafka-events", severity: 2, started_at: "2025-03-09", mttr_minutes: 140,
    root_cause: "capacity", signals: ["queue-backlog", "consumer-lag", "latency"], team: "streaming",
    fix: "Scaled notifications consumer group 6→24 pods after increasing topic partitions 12→48",
    summary: "Push notifications and receipts arrived up to 2 hours late. No payments were lost.",
    alerts: "KafkaConsumerGroupLag > 500k on payment-events (group notifications-v2), NotificationSLOBurnRate fast-burn.",
    timeline: "Sunday 10:00 marketing campaign tripled event volume → 10:20 lag alert → 10:45 scaled pods but only 12 partitions limited parallelism → 11:30 partitions raised → 12:20 lag drained.",
    cause: "Topic had 12 partitions, capping consumer parallelism at 12. Campaign traffic was not shared with platform in advance.",
    followups: "Partition count reviewed per topic quarterly; KEDA autoscaling on lag; campaign calendar shared with SRE.",
  },
  {
    id: "INC-2025-005",
    title: "Redis session cluster OOM logged out 40% of web users",
    service: "redis-session", severity: 2, started_at: "2025-03-27", mttr_minutes: 46,
    root_cause: "config", signals: ["oom", "evictions", "logouts", "5xx"], team: "identity",
    fix: "Set maxmemory-policy volatile-lru, restored TTLs on session keys, resized nodes to 26GB",
    summary: "Users were logged out mid-checkout; login rate spiked 9x causing auth-service load.",
    alerts: "RedisMemoryUsage > 95%, RedisEvictedKeysRate spike, AuthLoginRateAnomaly.",
    timeline: "14:10 memory at 100% → evictions of keys without TTL under allkeys-lru → 14:18 logout reports → 14:35 found a release that wrote sessions without EXPIRE → 14:56 fixed.",
    cause: "checkout-web v2.30 changed session serialisation and dropped the EXPIRE call; memory grew for 6 days until eviction kicked in.",
    followups: "Alert on keys-without-TTL ratio; contract test that every session write has TTL.",
  },
  {
    id: "INC-2025-006",
    title: "Internal DNS resolution failures in eu-west-2b",
    service: "dns", severity: 2, started_at: "2025-04-11", mttr_minutes: 33,
    root_cause: "network", signals: ["dns", "timeouts", "5xx", "latency"], team: "platform",
    fix: "Restarted CoreDNS pods in az-b, raised CoreDNS replicas 3→6 and enabled NodeLocal DNSCache",
    summary: "Intermittent 'no such host' errors for service-to-service calls in one availability zone.",
    alerts: "CoreDNSErrorsRate (SERVFAIL), UpstreamConnectTimeouts across payments-api and ledger.",
    timeline: "16:40 errors start → 16:48 correlated with az-b nodes → 16:58 CoreDNS conntrack table full on 2 nodes → 17:13 stable after restart and scale.",
    cause: "conntrack exhaustion on nodes hosting CoreDNS after a node pool scale-in concentrated all 3 replicas in az-b.",
    followups: "Topology spread constraints for CoreDNS; NodeLocal DNSCache rolled out cluster-wide.",
  },
  {
    id: "INC-2025-007",
    title: "Nightly bank reconciliation produced duplicate ledger entries",
    service: "batch-reconciliation", severity: 1, started_at: "2025-04-29", mttr_minutes: 310,
    root_cause: "data", signals: ["data-integrity", "duplicates", "batch-failure"], team: "core-ledger",
    fix: "Froze payouts, reversed 1,412 duplicate entries via compensating journal, added idempotency key on statement line id",
    summary: "1,412 merchant settlements were credited twice (£612k). Payouts frozen for 5 hours while finance verified balances.",
    alerts: "LedgerTrialBalanceMismatch, ReconBatchDurationAnomaly (job retried), finance ops escalation.",
    timeline: "02:00 batch started → 02:40 bank SFTP timeout → job retried from start without checkpoint → 07:10 trial balance alert → 07:30 payouts frozen → 12:10 reversed and verified.",
    cause: "Retry after a partial failure re-imported statement lines; inserts were not idempotent on bank_statement_line_id.",
    followups: "Unique constraint on (bank_id, statement_line_id); checkpointing; trial balance check before payout release.",
  },
  {
    id: "INC-2025-008",
    title: "CDN edge served stale checkout JavaScript after purge failure",
    service: "cdn-edge", severity: 3, started_at: "2025-05-15", mttr_minutes: 64,
    root_cause: "config", signals: ["stale-cache", "js-errors", "conversion-drop"], team: "web",
    fix: "Purged by surrogate key, switched to content-hashed asset filenames",
    summary: "Some users got old checkout.js against a new API, breaking 3DS challenge on Safari.",
    alerts: "FrontendJsErrorRate (TypeError in checkout.js), CheckoutConversionDrop 6%.",
    timeline: "11:00 deploy → purge API call returned 429 and was not retried → 11:30 JS errors → 12:04 manual purge → 12:04 recovered.",
    cause: "Assets used stable filenames relying on purge; CDN purge rate-limited and the deploy pipeline ignored the failure.",
    followups: "Hashed filenames; deploy fails if purge fails; RUM alert on new JS error signatures.",
  },
  {
    id: "INC-2025-009",
    title: "auth-service JWT validation failing after key rotation",
    service: "auth-service", severity: 1, started_at: "2025-06-02", mttr_minutes: 27,
    root_cause: "config", signals: ["401", "auth-failures", "jwks"], team: "identity",
    fix: "Re-published old signing key in JWKS for overlap window; fixed rotation job ordering",
    summary: "Every API call with a token minted before 10:00 got 401. Mobile users force-logged out.",
    alerts: "Http401RateHigh across all services, AuthTokenValidationErrors 'kid not found'.",
    timeline: "10:00 rotation job swapped keys → 10:02 401 spike → 10:10 SEV1 → 10:22 old key restored to JWKS → 10:29 normal.",
    cause: "Rotation job removed the previous key from JWKS at the same time it started signing with the new one; no overlap window. Services cache JWKS for 10 minutes.",
    followups: "Rotation keeps previous key for 24h; staged rotation runbook; synthetic check with an old token.",
  },
  {
    id: "INC-2025-010",
    title: "Ledger replica replication lag served stale balances",
    service: "ledger-postgres", severity: 2, started_at: "2025-06-19", mttr_minutes: 85,
    root_cause: "capacity", signals: ["replication-lag", "stale-reads", "disk-io"], team: "core-ledger",
    fix: "Paused analytics export on replica, moved it to a dedicated replica, upgraded replica to gp3 12k IOPS",
    summary: "Mobile app balances lagged up to 9 minutes behind; customers saw payments 'missing'.",
    alerts: "PostgresReplicationLagSeconds > 120 on ledger-replica-1, support ticket spike 'balance wrong'.",
    timeline: "13:00 analytics export started → 13:15 lag alert → 13:50 identified IO saturation on replica → 14:25 export paused, lag recovered.",
    cause: "A heavy analytics export ran on the same replica used for customer balance reads, saturating disk IO and slowing WAL replay.",
    followups: "Route analytics to dedicated replica; read-your-writes for balance after payment; lag-aware read routing.",
  },
  {
    id: "INC-2025-011",
    title: "Open banking provider outage blocked account linking",
    service: "payments-api", severity: 2, started_at: "2025-07-07", mttr_minutes: 190,
    root_cause: "dependency", signals: ["dependency-5xx", "timeouts", "error-rate"], team: "payments",
    fix: "Failed over to secondary AIS provider via feature flag; added provider health-based routing",
    summary: "New customers could not link bank accounts for 3 hours. Existing payments unaffected.",
    alerts: "UpstreamProviderErrorRate (ais-provider-a 503), OnboardingFunnelDrop.",
    timeline: "08:40 provider 503s → 08:55 status page confirmed provider outage → 10:30 decision to fail over → 11:50 secondary live.",
    cause: "Third-party Account Information Service provider regional outage. Failover existed but was manual and untested.",
    followups: "Quarterly failover drill; automatic routing on provider SLO breach.",
  },
  {
    id: "INC-2025-012",
    title: "Ingress controller CPU throttling caused p99 latency spike",
    service: "k8s-ingress", severity: 2, started_at: "2025-07-24", mttr_minutes: 41,
    root_cause: "capacity", signals: ["latency", "cpu-throttling", "timeouts"], team: "platform",
    fix: "Removed CPU limits on ingress-nginx, HPA min replicas 3→6",
    summary: "p99 latency for all public APIs rose from 180ms to 3.1s during a merchant flash sale.",
    alerts: "IngressLatencyP99 > 1s, container_cpu_cfs_throttled_periods high on ingress-nginx.",
    timeline: "19:00 flash sale → 19:04 latency alert → 19:20 throttling identified → 19:45 limits removed and scaled.",
    cause: "ingress-nginx pods had 1 CPU limit; CFS throttling under burst, while HPA reacted on averaged CPU too slowly.",
    followups: "No CPU limits on latency-critical pods; HPA on requests-per-second.",
  },
  {
    id: "INC-2025-013",
    title: "Notifications SMS provider rejected messages after sender ID change",
    service: "notifications", severity: 3, started_at: "2025-08-05", mttr_minutes: 220,
    root_cause: "config", signals: ["delivery-failure", "dependency-4xx"], team: "growth",
    fix: "Reverted sender ID to registered 'LARKSPUR'; registered new ID with carriers",
    summary: "One-time passcodes via SMS failed for UK O2 and Vodafone customers; users fell back to email OTP.",
    alerts: "SmsDeliveryRate < 80%, provider error code 'unregistered sender'.",
    timeline: "09:00 config change → 09:30 delivery alert (low traffic) → 12:40 traced to sender ID → 12:40 reverted.",
    cause: "Alphanumeric sender ID was changed in config without carrier registration.",
    followups: "Sender ID changes require compliance sign-off; delivery-rate alert by carrier.",
  },
  {
    id: "INC-2025-014",
    title: "Search cluster red after shard allocation failure",
    service: "search", severity: 3, started_at: "2025-08-21", mttr_minutes: 95,
    root_cause: "capacity", signals: ["disk-full", "cluster-red", "5xx"], team: "data-platform",
    fix: "Deleted old transaction indices past retention, raised disk watermark alerts, added 2 data nodes",
    summary: "Transaction search in the merchant dashboard returned errors for 95 minutes.",
    alerts: "OpenSearchClusterStatusRed, DiskUsage > 95% on 3 data nodes.",
    timeline: "06:10 disk flood-stage watermark → indices read-only → 06:30 alert → 07:20 ILM policy found disabled → 07:45 cleanup.",
    cause: "Index lifecycle policy was disabled during a migration and never re-enabled; disks filled over 5 weeks.",
    followups: "Alert at 75% disk; ILM policy drift check.",
  },
  {
    id: "INC-2025-015",
    title: "checkout-web release broke Apple Pay button",
    service: "checkout-web", severity: 2, started_at: "2025-09-03", mttr_minutes: 29,
    root_cause: "deploy", signals: ["js-errors", "conversion-drop", "error-rate"], team: "web",
    fix: "Rolled back checkout-web to v2.41.0; fixed CSP header to allow apple-pay domain",
    summary: "Apple Pay unavailable for 29 minutes; Apple Pay is 31% of mobile checkout volume.",
    alerts: "ApplePaySessionErrors, CSP violation reports spike, CheckoutConversionDrop 11% on iOS.",
    timeline: "15:12 deploy v2.42.0 → 15:20 CSP reports → 15:30 SEV2 → 15:36 rollback → 15:41 recovered.",
    cause: "New Content-Security-Policy header omitted apple-pay-gateway domain.",
    followups: "CSP changes behind report-only first; synthetic Apple Pay session check.",
  },
  {
    id: "INC-2025-016",
    title: "Kafka broker disk failure triggered under-replicated partitions",
    service: "kafka-events", severity: 2, started_at: "2025-09-18", mttr_minutes: 75,
    root_cause: "network", signals: ["under-replicated", "consumer-lag", "broker-down"], team: "streaming",
    fix: "Replaced broker-3 EBS volume, reassigned partitions, restored ISR",
    summary: "Producers saw elevated latency and 0.4% send failures; consumer lag grew on ledger-events.",
    alerts: "KafkaUnderReplicatedPartitions > 0, ProducerRequestLatencyP99 > 500ms.",
    timeline: "03:20 broker-3 volume degraded → 03:25 URP alert → 04:10 broker drained → 04:35 ISR restored.",
    cause: "Underlying EBS volume degraded (AWS event); broker stayed in cluster with slow disk.",
    followups: "Automated broker eviction on disk latency; min.insync.replicas review.",
  },
  {
    id: "INC-2025-017",
    title: "Ledger migration locked transactions table",
    service: "ledger-postgres", severity: 1, started_at: "2025-10-01", mttr_minutes: 22,
    root_cause: "deploy", signals: ["lock-contention", "latency", "5xx", "timeouts"], team: "core-ledger",
    fix: "Cancelled migration (pg_cancel_backend), re-ran as CREATE INDEX CONCURRENTLY off-peak",
    summary: "All ledger writes blocked for 22 minutes; payments-api returned 503.",
    alerts: "PostgresLocksWaiting > 500, LedgerWriteLatencyP99 > 10s, PaymentsApi5xxRateHigh.",
    timeline: "11:05 deploy ran migration adding index without CONCURRENTLY → 11:07 locks pile up → 11:20 IC found blocking pid → 11:27 cancelled.",
    cause: "Schema migration took an ACCESS EXCLUSIVE-conflicting lock on a 900M-row table during business hours.",
    followups: "Migration linter (squawk) in CI; lock_timeout=5s for migrations.",
  },
  {
    id: "INC-2025-018",
    title: "auth-service brute-force traffic exhausted rate limiter Redis",
    service: "auth-service", severity: 2, started_at: "2025-10-20", mttr_minutes: 58,
    root_cause: "capacity", signals: ["latency", "429", "bot-traffic", "cpu"], team: "identity",
    fix: "Enabled WAF bot rule at edge, moved rate limiter to dedicated Redis, blocked 3 ASNs",
    summary: "Credential-stuffing attack (40k req/s) slowed login for real users; p99 login 6s.",
    alerts: "AuthLoginRateAnomaly, RedisCPU > 90% on ratelimit, WAF rule matches spike.",
    timeline: "21:00 attack start → 21:08 alert → 21:30 WAF rule → 21:58 normal.",
    cause: "Rate limiter shared the session Redis; attack traffic saturated it before limits kicked in.",
    followups: "Edge rate limiting first; dedicated limiter store; credential-stuffing playbook.",
  },
  {
    id: "INC-2025-019",
    title: "Expired intermediate CA broke mTLS to card processor",
    service: "payments-api", severity: 1, started_at: "2025-11-04", mttr_minutes: 96,
    root_cause: "certificate", signals: ["cert-expiry", "tls-handshake", "dependency-5xx"], team: "payments",
    fix: "Installed renewed client certificate chain from processor portal; added expiry monitoring for client certs",
    summary: "Card authorisations to the acquirer failed with handshake errors; fallback acquirer took 60% traffic.",
    alerts: "AcquirerTLSHandshakeFailures, AuthorisationSuccessRate < 70%.",
    timeline: "00:00 client cert chain expired → 00:04 alert → 00:40 identified mTLS client cert (not in cert-manager) → 01:36 renewed.",
    cause: "mTLS client certificate for the acquirer was stored manually in Vault with no expiry tracking.",
    followups: "Inventory of all certs incl. client certs; alert 30 days before expiry.",
  },
  {
    id: "INC-2025-020",
    title: "Config push set payments-api DB pool to 5",
    service: "payments-api", severity: 2, started_at: "2025-11-26", mttr_minutes: 34,
    root_cause: "config", signals: ["connection-pool", "latency", "timeouts", "5xx"], team: "payments",
    fix: "Reverted Helm values change; added schema validation for pool settings",
    summary: "Black Friday morning: payments-api latency p99 2.7s and intermittent 503 due to pool starvation.",
    alerts: "HikariPoolPendingThreads > 50, PaymentsApiLatencyP99 > 1s.",
    timeline: "07:30 config change merged (typo maxPoolSize: 5 instead of 50) → 07:41 alert → 08:04 reverted.",
    cause: "Human error in Helm values; no validation or canary for config-only changes.",
    followups: "Config changes go through canary; values schema with min/max.",
  },
  {
    id: "INC-2025-021",
    title: "Batch reconciliation job OOMKilled at month end",
    service: "batch-reconciliation", severity: 3, started_at: "2025-12-01", mttr_minutes: 180,
    root_cause: "capacity", signals: ["oom", "batch-failure", "restarts"], team: "core-ledger",
    fix: "Streamed bank statements instead of loading in memory; memory limit 4Gi→8Gi",
    summary: "Month-end reconciliation delayed 3 hours; finance close slipped.",
    alerts: "KubeJobFailed recon-monthly, ContainerOOMKilled.",
    timeline: "01:00 job start → 01:35 OOMKilled ×3 → 02:00 paged → 04:00 rerun with more memory.",
    cause: "Statement file 4x normal size after a new bank partner went live; parser loaded whole file in memory.",
    followups: "Streaming parser; job memory alerts at 80%.",
  },
  {
    id: "INC-2026-001",
    title: "Cloud load balancer health check misconfiguration drained checkout",
    service: "checkout-web", severity: 1, started_at: "2026-01-12", mttr_minutes: 19,
    root_cause: "config", signals: ["5xx", "unhealthy-targets", "error-rate"], team: "platform",
    fix: "Reverted Terraform health check path from /healthz to /readyz-old; corrected app readiness route",
    summary: "All checkout targets marked unhealthy; ALB returned 503 to 100% of checkout traffic.",
    alerts: "AlbUnhealthyHostCount = all, Checkout5xxRate 100%, synthetic checkout failed.",
    timeline: "10:02 terraform apply → 10:04 targets unhealthy → 10:08 SEV1 → 10:21 reverted.",
    cause: "Health check path changed in Terraform before the app release that served the new path.",
    followups: "Health check changes coupled to app release; plan review for LB changes.",
  },
  {
    id: "INC-2026-002",
    title: "Payments-api memory leak after HTTP client upgrade",
    service: "payments-api", severity: 2, started_at: "2026-02-17", mttr_minutes: 64,
    root_cause: "deploy", signals: ["oom", "restarts", "latency", "5xx"], team: "payments",
    fix: "Rolled back okhttp upgrade (v4.12 → v4.11); added heap alerts",
    summary: "payments-api pods OOMKilled every ~40 minutes causing brief 5xx bursts and retries.",
    alerts: "ContainerOOMKilled payments-api, PodRestartRate, JVM heap > 90%.",
    timeline: "Release Tuesday → Wednesday 09:00 restart pattern noticed → 09:40 heap dump shows connection pool leak → 10:04 rollback.",
    cause: "Upgraded HTTP client changed connection pool eviction default, leaking connections to the fraud service.",
    followups: "Soak test 24h in staging for dependency upgrades; heap growth alert.",
  },
  {
    id: "INC-2026-003",
    title: "DNS record TTL change caused partner webhook failures",
    service: "dns", severity: 3, started_at: "2026-03-05", mttr_minutes: 150,
    root_cause: "network", signals: ["dns", "webhook-failures", "dependency-4xx"], team: "platform",
    fix: "Restored A record for hooks.larkspurpay.co.uk; partners flushed caches",
    summary: "Merchant webhooks failed for 2.5h after a DNS migration to a new zone.",
    alerts: "WebhookDeliveryFailureRate > 20%, merchant escalations.",
    timeline: "14:00 zone migration → record for hooks missing → 14:30 alert → 16:30 restored.",
    cause: "Zone export missed a record created via the legacy DNS console.",
    followups: "DNS as code only; diff zone before cutover.",
  },
];

function pmText(p: Pm): string {
  return [
    `${p.id} · ${p.title}`,
    `Service: ${p.service} · SEV${p.severity} · ${p.started_at} · MTTR ${p.mttr_minutes} min · team ${p.team}`,
    `Summary: ${p.summary}`,
    `Alerts seen: ${p.alerts}`,
    `Timeline: ${p.timeline}`,
    `Root cause (${p.root_cause}): ${p.cause}`,
    `Resolution: ${p.fix}.`,
    `Follow-ups: ${p.followups}`,
  ].join("\n");
}

export const INCIDENTS: SeedDoc[] = PMS.map((p) => ({
  id: p.id,
  text: pmText(p),
  metadata: {
    title: p.title,
    service: p.service,
    severity: p.severity,
    started_at: p.started_at,
    mttr_minutes: p.mttr_minutes,
    root_cause: p.root_cause,
    signals: p.signals,
    fix: p.fix,
    team: p.team,
  },
}));

export const SERVICES = Array.from(new Set(PMS.map((p) => p.service))).sort();
export const SIGNALS = Array.from(new Set(PMS.flatMap((p) => p.signals))).sort();

interface Rb {
  id: string;
  title: string;
  service: string;
  risk: "safe" | "caution" | "destructive";
  steps: string;
}

const RBS: Rb[] = [
  {
    id: "RB-payments-rollback",
    title: "Roll back a payments-api release (Argo Rollouts)",
    service: "payments-api", risk: "caution",
    steps: `When: 5xx or latency regression within 60 min of a payments-api rollout.
1. Confirm correlation: kubectl argo rollouts get rollout payments-api -n payments (check revision + time of promotion).
2. Compare error rate by version: sum by (version) (rate(http_requests_total{app="payments-api",code=~"5.."}[5m])).
3. Abort/undo: kubectl argo rollouts undo payments-api -n payments (returns to previous stable ReplicaSet).
4. Watch: kubectl argo rollouts status payments-api -n payments --watch; 5xx should fall within 3 min.
Rollback criteria: error rate back under 0.5% and p99 < 400ms for 10 min. If DB migrations shipped with the release, check they are backward compatible BEFORE undo.`,
  },
  {
    id: "RB-pg-connections",
    title: "Diagnose Postgres / PgBouncer connection exhaustion",
    service: "ledger-postgres", risk: "caution",
    steps: `1. PgBouncer pools: psql -h pgbouncer -p 6432 pgbouncer -c "SHOW POOLS;" (look at cl_waiting, sv_active).
2. Sessions by app/state: psql -c "SELECT application_name, state, count(*) FROM pg_stat_activity GROUP BY 1,2 ORDER BY 3 DESC;"
3. Long idle-in-transaction: psql -c "SELECT pid, application_name, now()-xact_start AS age FROM pg_stat_activity WHERE state='idle in transaction' ORDER BY age DESC LIMIT 20;"
4. (Caution) Terminate offenders older than 5 min: SELECT pg_terminate_backend(pid) ... — confirm with ledger owner first.
5. Temporary relief: raise default_pool_size in pgbouncer.ini and RELOAD; never exceed max_connections - 20.
Rollback criteria: cl_waiting < 10 sustained 5 min.`,
  },
  {
    id: "RB-pg-locks",
    title: "Find and clear blocking locks on ledger Postgres",
    service: "ledger-postgres", risk: "destructive",
    steps: `1. Blocking tree: psql -c "SELECT pid, pg_blocking_pids(pid) AS blocked_by, left(query,80) FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid)) > 0;"
2. Identify root blocker query and owner (migrations usually run as app_migrator).
3. Try SELECT pg_cancel_backend(<pid>) first (cancels the query, keeps session).
4. DESTRUCTIVE: SELECT pg_terminate_backend(<pid>) only with IC approval — may roll back a large transaction and lengthen recovery.
5. Post-check: locks waiting < 10, ledger write p99 < 200ms.`,
  },
  {
    id: "RB-pg-replica-lag",
    title: "Replication lag on ledger read replicas",
    service: "ledger-postgres", risk: "safe",
    steps: `1. Lag: psql -h ledger-replica-1 -c "SELECT now() - pg_last_xact_replay_timestamp() AS lag;"
2. On primary: SELECT client_addr, state, replay_lag FROM pg_stat_replication;
3. Check replica IO: CloudWatch ReadIOPS/WriteIOPS/DiskQueueDepth; look for analytics or export jobs (pg_stat_activity on replica).
4. Pause heavy readers or shift balance reads to primary via feature flag balances.read_from_primary=true.
Rollback criteria: lag < 5s; re-enable replica reads gradually.`,
  },
  {
    id: "RB-cert-expiry",
    title: "TLS certificate expired or failing to renew (cert-manager)",
    service: "k8s-ingress", risk: "safe",
    steps: `1. Check expiry from outside: echo | openssl s_client -connect api.larkspurpay.co.uk:443 -servername api.larkspurpay.co.uk 2>/dev/null | openssl x509 -noout -dates
2. cert-manager status: kubectl get certificate -A; kubectl describe certificate <name> -n <ns>; kubectl get challenges -A.
3. DNS-01 failures: check solver secret (Cloudflare token) is valid: kubectl get secret cloudflare-api-token -n cert-manager.
4. Force renewal: cmctl renew <name> -n <ns>.
5. Client (mTLS) certs are NOT in cert-manager — check Vault path secret/payments/acquirer-mtls and processor portal.`,
  },
  {
    id: "RB-kafka-lag",
    title: "Kafka consumer lag / backlog",
    service: "kafka-events", risk: "safe",
    steps: `1. Lag by group: kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group <group>.
2. Partition count vs consumers: kafka-topics.sh --describe --topic <topic> (consumers > partitions add nothing).
3. Scale consumers: kubectl scale deploy/<consumer> --replicas=<n> -n streaming (≤ partitions).
4. Check URP/broker health: kafka-topics.sh --describe --under-replicated-partitions.
5. (Caution) Partition increase is irreversible and changes key ordering — needs streaming owner approval.`,
  },
  {
    id: "RB-redis-memory",
    title: "Redis session memory pressure / evictions",
    service: "redis-session", risk: "caution",
    steps: `1. Memory: redis-cli -h redis-session INFO memory (used_memory, maxmemory, mem_fragmentation_ratio).
2. Evictions: redis-cli INFO stats | grep evicted_keys; policy: redis-cli CONFIG GET maxmemory-policy.
3. Keys without TTL sample: redis-cli --scan --pattern 'sess:*' | head -1000 | xargs -n1 redis-cli TTL | grep -c -- -1
4. Big keys: redis-cli --bigkeys.
5. DESTRUCTIVE: FLUSHDB logs out every user — never without IC approval.`,
  },
  {
    id: "RB-dns",
    title: "Service DNS resolution errors (CoreDNS)",
    service: "dns", risk: "safe",
    steps: `1. From an affected pod: kubectl exec -it <pod> -- nslookup ledger.core.svc.cluster.local.
2. CoreDNS health: kubectl -n kube-system get pods -l k8s-app=kube-dns -o wide (check AZ spread); kubectl logs for SERVFAIL/timeouts.
3. Node conntrack: kubectl debug node/<node> -- chroot /host conntrack -S (insert_failed / drop).
4. Mitigate: kubectl -n kube-system rollout restart deploy/coredns; scale replicas to 6.
5. External DNS: dig +trace hooks.larkspurpay.co.uk; compare against DNS-as-code repo.`,
  },
  {
    id: "RB-ingress-latency",
    title: "Ingress latency / CPU throttling",
    service: "k8s-ingress", risk: "safe",
    steps: `1. Throttling: rate(container_cpu_cfs_throttled_periods_total{pod=~"ingress-nginx.*"}[5m]).
2. Replicas/HPA: kubectl -n ingress get hpa; kubectl top pods -n ingress.
3. Scale out: kubectl -n ingress scale deploy/ingress-nginx-controller --replicas=8.
4. Check upstream latency vs ingress latency to separate ingress from backend problems (nginx_ingress_controller_response_duration_seconds).`,
  },
  {
    id: "RB-feature-flag-failover",
    title: "Fail over a third-party dependency via feature flag",
    service: "payments-api", risk: "caution",
    steps: `1. Confirm provider issue: provider status page + UpstreamProviderErrorRate by provider label.
2. Check secondary health: curl -s https://ais-provider-b.internal/health.
3. Flip flag in LaunchDarkly: ais.provider = "b" for 10% → 50% → 100%, watching success rate.
4. Notify partnerships channel; record start time for SLA claim.
Rollback criteria: secondary success rate < 95% → revert to primary and declare dependency outage.`,
  },
  {
    id: "RB-checkout-web-rollback",
    title: "Roll back checkout-web and purge CDN",
    service: "checkout-web", risk: "caution",
    steps: `1. Identify release: kubectl -n web rollout history deploy/checkout-web.
2. Roll back: kubectl -n web rollout undo deploy/checkout-web.
3. Purge CDN by surrogate key: curl -X POST https://api.cdn.example/purge -d '{"keys":["checkout"]}' — confirm 200, retry on 429.
4. Verify in RUM: JS error rate and CSP violations back to baseline; run synthetic Apple Pay + 3DS checkout.`,
  },
];

export const RUNBOOKS: SeedDoc[] = RBS.map((r) => ({
  id: r.id,
  text: `${r.title}\nService: ${r.service} · risk: ${r.risk}\n${r.steps}`,
  metadata: { title: r.title, service: r.service, risk: r.risk },
}));

export interface SampleAlert {
  title: string;
  body: string;
}

export const SAMPLE_ALERTS: SampleAlert[] = [
  {
    title: "PaymentsApi5xxRateHigh after deploy",
    body: `[FIRING:1] PaymentsApi5xxRateHigh — severity=page service=payments-api env=prod
5xx ratio 7.8% (threshold 5%) on POST /v2/authorisations for 4m.
p99 latency 3.6s. Started 6 minutes after payments-api v4.19.0 promoted to 100% by Argo Rollouts.
Upstream: fraud-scoring p99 2.9s.`,
  },
  {
    title: "Ledger PgBouncer clients waiting",
    body: `[FIRING:2] PgBouncerClientsWaiting — service=ledger-postgres env=prod
cl_waiting=640 sv_active=140/140 on pool ledger_rw.
LedgerWriteLatencyP99 4.1s. payments-api seeing timeouts on balance check. Month-start payout batch running.`,
  },
  {
    title: "Synthetic HTTPS check failing",
    body: `[FIRING:1] SyntheticHttpsCheckFailed — target=https://partners.larkspurpay.co.uk env=prod
error: x509: certificate has expired or is not yet valid. 3/3 regions failing.
IngressTLSHandshakeErrors elevated. Partner integration reporting connection failures.`,
  },
  {
    title: "Kafka consumer lag on notifications",
    body: `[FIRING:1] KafkaConsumerGroupLag — topic=payment-events group=notifications-v2
lag=820k and growing 6k/s. Receipts and push notifications delayed ~40 min.
Consumer pods 12/12 running, CPU 35%.`,
  },
  {
    title: "Users logged out, Redis evictions",
    body: `[FIRING:2] RedisEvictedKeysRate — cluster=redis-session env=prod
evicted_keys 1,900/s, used_memory 99.6% of maxmemory. AuthLoginRateAnomaly: logins 8x baseline.
Support reports users logged out during checkout.`,
  },
];
