/**
 * Support Desk demo corpus for "Ledgerly" — a fictional UK B2B
 * invoicing / accounting SaaS. Every customer, company and agent here
 * is invented. Two buckets:
 *
 * - `support_kb`: help-centre articles (the only source of fix steps
 *   the reply drafter may use).
 * - `support_tickets`: resolved past tickets — the nearest-neighbour
 *   "memory" that triage votes over. Resolving a new ticket in the app
 *   appends to this bucket, so triage learns.
 */
import type { SeedDoc } from "../seed";

export const SEED_VERSION = "support-v1";
export const KB_BUCKET = "support_kb";
export const TICKETS_BUCKET = "support_tickets";

export const CATEGORIES = [
  "billing",
  "auth",
  "integrations",
  "tax",
  "invoicing",
  "api",
  "data",
  "account",
] as const;
export type Category = (typeof CATEGORIES)[number];

export const TEAMS = ["L1", "L2-integrations", "L2-billing", "security", "engineering"] as const;
export type Team = (typeof TEAMS)[number];

export type Plan = "starter" | "growth" | "enterprise";

function kb(
  id: string,
  category: Category,
  updated_at: string,
  title: string,
  body: string
): SeedDoc {
  return {
    id,
    text: `${id} · ${title}\n${body}`,
    metadata: { article_id: id, category, updated_at, title },
  };
}

export const KB: SeedDoc[] = [
  kb("KB-101", "auth", "2026-08-14", "Set up SAML single sign-on (Okta or Azure AD / Entra ID)",
    "Available on Growth and Enterprise. 1) In Ledgerly go to Settings → Security → Single sign-on and copy the ACS URL (https://app.ledgerly.example/sso/acs) and Entity ID. 2) Okta: create a SAML 2.0 app, paste ACS URL and Entity ID, set Name ID format to EmailAddress. Azure AD / Entra ID: Enterprise applications → New → Non-gallery, Single sign-on → SAML, paste the same values. 3) Download the IdP metadata XML and upload it in Ledgerly. 4) Click Test connection with a non-admin account first. 5) Only then toggle 'Enforce SSO' — keep at least one break-glass owner with password + 2FA. Common error 'Invalid audience' means the Entity ID does not match exactly (no trailing slash). Clock skew over 3 minutes causes 'Assertion expired'."),
  kb("KB-102", "auth", "2026-07-02", "Locked out of two-factor authentication (2FA recovery)",
    "1) Use one of the 10 backup codes issued when 2FA was enabled. 2) If no backup codes: another account Owner can reset your 2FA under Settings → Team → user → Reset 2FA. 3) If you are the only Owner, support can reset 2FA only after identity verification: a request from the registered company email domain plus the last 4 digits of the card on file and the most recent invoice number. Support never resets 2FA from a phone call or a free-mail address. Resets take effect immediately and are logged in the audit trail. Route sole-owner resets to the security team."),
  kb("KB-103", "invoicing", "2026-09-01", "Invoice stuck in Draft or will not send",
    "Invoices stay in Draft when: a) the customer contact has no email address, b) a line item has a tax rate that was archived, c) the invoice date is in a locked VAT period, or d) the sending domain failed verification. Fix: open the invoice, the yellow banner names the blocking field. Re-select an active tax rate, or ask an Adviser to unlock the period (Settings → Tax → Lock dates). If the status shows 'Sending' for more than 15 minutes, click Retry send; if that fails check KB-118 (email deliverability)."),
  kb("KB-104", "integrations", "2026-08-20", "Bank feed disconnected or stopped importing transactions",
    "UK bank feeds use Open Banking. Banks require you to re-consent every 90 days — this is a regulatory requirement, not a fault. 1) Go to Banking → Feeds; a red 'Consent expired' badge shows on the account. 2) Click Reconnect and approve in your banking app. 3) Missing transactions back-fill automatically for up to 90 days after reconnecting; allow 2 hours. If the feed shows 'Bank unavailable', check the bank's status page — Ledgerly cannot speed that up. Duplicate transactions after reconnecting: use Banking → Remove duplicates."),
  kb("KB-105", "tax", "2026-09-10", "HMRC Making Tax Digital (MTD) VAT submission errors",
    "1) 'INVALID_CREDENTIALS' or 'CLIENT_OR_AGENT_NOT_AUTHORISED': the HMRC authorisation expired (every 18 months). Go to Tax → VAT → HMRC connection → Reconnect and sign in with the Government Gateway ID that owns the VAT number. 2) 'DUPLICATE_SUBMISSION': HMRC already holds a return for this period — check the HMRC account before retrying; never submit twice. 3) 'VRN_INVALID': the VAT registration number in Settings → Tax does not match HMRC records. 4) 'Obligation not found': the period dates differ from HMRC's; refresh obligations. Ledgerly cannot amend a return once HMRC accepts it; corrections go in the next return (error under £10,000 or 1% of turnover rule)."),
  kb("KB-106", "billing", "2026-06-30", "Refunds, credits and cancellation policy",
    "Monthly plans: cancel any time from Settings → Billing; access continues to the end of the paid month; no partial-month refunds. Annual plans: full refund within 14 days of the initial purchase or renewal; after 14 days no refund, but the plan can be set not to renew. Service credits apply only to Enterprise under the SLA (10% of the monthly fee per full hour below 99.9% monthly uptime, capped at 30%), claimed within 30 days. Duplicate charges are always refunded in full. Agents may not offer goodwill credits; any exception needs approval from the L2 billing team."),
  kb("KB-107", "billing", "2026-07-18", "Upgrade, downgrade and proration",
    "Upgrades take effect immediately; you pay a prorated amount for the rest of the billing period. Downgrades take effect at the next renewal date; features above the new plan's limits become read-only, nothing is deleted. Switching monthly → annual applies a 2-months-free discount from the next bill. Seat reductions apply at renewal. The invoice for any change appears under Settings → Billing → History within 5 minutes."),
  kb("KB-108", "api", "2026-09-05", "API rate limits and HTTP 429 Too Many Requests",
    "Limits per organisation: Starter 60 requests/minute, Growth 300/minute, Enterprise 1,200/minute, plus 5 concurrent requests per token. A 429 response includes a Retry-After header in seconds — wait that long, then retry with exponential backoff and jitter. Use the bulk endpoints (/v2/invoices/batch, up to 100 per call) instead of loops. Webhooks are better than polling for status changes. Enterprise customers can request a temporary limit raise for a migration through their account manager with 2 business days' notice."),
  kb("KB-109", "api", "2026-08-28", "Webhook delivery failures and signature verification",
    "Every webhook has an X-Ledgerly-Signature header: HMAC-SHA256 of the raw request body using your endpoint's signing secret, hex-encoded, prefixed 't=<timestamp>,v1='. Verify against the RAW body bytes — parsing and re-serialising JSON changes whitespace and breaks the signature. Reject timestamps older than 5 minutes. After rotating the secret, the old secret stays valid for 24 hours. Failed deliveries (non-2xx or timeout over 10 seconds) retry with backoff for 72 hours; see Developer → Webhooks → Delivery log, and click Replay to resend."),
  kb("KB-110", "data", "2026-05-12", "CSV import errors (contacts, invoices, chart of accounts)",
    "Use the template from Import → Download template; columns must keep the header names. Dates must be DD/MM/YYYY. Amounts must not include currency symbols or thousands separators. Files must be UTF-8 encoded — Excel 'CSV UTF-8' option — otherwise £ signs show as Â£. Maximum 5,000 rows per file. Row-level errors are listed after the dry run; fix and re-upload only the failed rows."),
  kb("KB-111", "invoicing", "2026-04-22", "Multi-currency invoices and exchange-rate revaluation",
    "Enable under Settings → Currencies (Growth and Enterprise). Rates come from the daily mid-market feed at 16:00 UK time; you can override a rate per invoice. Unrealised gains and losses are revalued at month end via Accounting → Revalue currencies, posting to the 'Unrealised FX' account. Realised gains/losses post automatically when payment is received at a different rate."),
  kb("KB-112", "data", "2026-06-03", "Export all your data",
    "Owners can export everything from Settings → Data → Full export: a ZIP of CSVs plus PDF copies of invoices, emailed as a secure link valid for 7 days. Large organisations may take up to 2 hours. Exports remain available for 90 days after cancellation, after which data is deleted under the retention policy."),
  kb("KB-113", "account", "2026-07-25", "UK GDPR data subject requests (access and erasure)",
    "Ledgerly is a processor for your customers' data and a controller for your user accounts. For a user's own account: send the request from the account email; we respond within one month. For your customers' personal data held in your Ledgerly account, you (the controller) action it: Contacts → contact → Anonymise. Financial records that HMRC requires you to keep for 6 years cannot be erased; they are anonymised instead. Requests mentioning legal action or the ICO are routed to the security & privacy team."),
  kb("KB-114", "account", "2026-08-01", "User roles and permissions",
    "Roles: Owner (billing, security, delete organisation), Adviser (full accounting incl. lock dates and VAT submission), Standard (create invoices and bills, no settings), Read-only, and Invoice-only. Only Owners can change roles. Removing a user does not delete their work; it remains attributed in the audit log. Growth includes 10 users, Enterprise unlimited."),
  kb("KB-115", "invoicing", "2026-03-15", "Recurring invoices",
    "Create from any invoice → Make recurring. Choose frequency, start date and whether to send automatically or save as draft. Recurring invoices generate at 06:00 UK time. If a run is skipped it is because the template's customer was archived or a tax rate was archived — fix the template and click Generate now."),
  kb("KB-116", "invoicing", "2026-05-30", "Automatic payment reminders",
    "Settings → Reminders: up to 3 reminders before and 5 after the due date, each with its own template. Reminders are not sent for invoices marked 'Disputed' or with an active payment plan. Late payment interest under the Late Payment of Commercial Debts Act can be added as a separate line; Ledgerly does not add it automatically."),
  kb("KB-117", "integrations", "2026-09-12", "Migrating from Xero or QuickBooks",
    "Use Settings → Migrate. Connect the source account; we import contacts, chart of accounts, open invoices and bills, and opening balances as at the conversion date. Historical closed transactions are imported as summarised journals per month. Run a trial import first, compare the trial balance, then run the final import. Bank feeds must be reconnected in Ledgerly (see KB-104). Enterprise migrations can be assisted by the onboarding team."),
  kb("KB-118", "invoicing", "2026-08-09", "Email deliverability for invoices (SPF, DKIM, custom sending domain)",
    "To send invoices from your own domain add the DNS records under Settings → Email → Sending domain: an SPF include (include:mail.ledgerly.example) and two DKIM CNAMEs. Verification can take up to 48 hours. Invoices landing in spam usually mean SPF is missing or the domain has a strict DMARC policy without DKIM alignment. Bounced emails are listed on the invoice's activity log with the receiving server's reason."),
];

interface PastTicket {
  n: number;
  subject: string;
  message: string;
  resolution: string;
  category: Category;
  priority: 1 | 2 | 3 | 4;
  plan: Plan;
  resolution_minutes: number;
  csat: number;
  article_id: string;
  team: Team;
  customer: string;
}

function ticketDoc(t: PastTicket): SeedDoc {
  const id = `TCK-${t.n}`;
  return {
    id,
    text: `${id} · ${t.subject}\nCustomer (${t.customer}, ${t.plan}): ${t.message}\nResolution: ${t.resolution}`,
    metadata: {
      ticket_id: id,
      subject: t.subject,
      customer: t.customer,
      category: t.category,
      priority: t.priority,
      plan: t.plan,
      resolution_minutes: t.resolution_minutes,
      csat: t.csat,
      article_id: t.article_id,
      team: t.team,
      status: "resolved",
    },
  };
}

const PAST: PastTicket[] = [
  { n: 4101, customer: "Harbour Lane Bakery Ltd", plan: "starter", category: "integrations", priority: 3, team: "L1", article_id: "KB-104", resolution_minutes: 25, csat: 5,
    subject: "Barclays feed stopped updating", message: "Our Barclays transactions haven't come through since last Tuesday.",
    resolution: "Open Banking consent had expired after 90 days. Walked them through Reconnect; transactions back-filled within the hour." },
  { n: 4102, customer: "Northgate Joinery", plan: "growth", category: "integrations", priority: 3, team: "L1", article_id: "KB-104", resolution_minutes: 40, csat: 4,
    subject: "Bank feed showing duplicates after reconnect", message: "We reconnected Lloyds and now every transaction appears twice.",
    resolution: "Used Banking → Remove duplicates; explained back-fill overlap after re-consent." },
  { n: 4103, customer: "Pellow & Finch Architects", plan: "enterprise", category: "auth", priority: 2, team: "security", article_id: "KB-101", resolution_minutes: 95, csat: 5,
    subject: "Okta SAML error: invalid audience", message: "Testing SSO with Okta gives 'Invalid audience' and nobody can log in through SSO.",
    resolution: "Entity ID in Okta had a trailing slash. Removed it, test connection passed, then enforced SSO with a break-glass owner kept." },
  { n: 4104, customer: "Clydeside Freight Ltd", plan: "enterprise", category: "auth", priority: 2, team: "security", article_id: "KB-101", resolution_minutes: 120, csat: 4,
    subject: "Azure AD SSO assertion expired", message: "Entra ID login fails with 'Assertion expired' for some users only.",
    resolution: "One office's domain controller clock was 6 minutes fast. Customer fixed NTP; errors stopped." },
  { n: 4105, customer: "Moorcroft Dental Group", plan: "growth", category: "auth", priority: 3, team: "L1", article_id: "KB-101", resolution_minutes: 30, csat: 5,
    subject: "How do we set up SSO with Azure?", message: "We'd like staff to log in with Microsoft accounts. Is that possible on our plan?",
    resolution: "Confirmed SSO is on Growth; sent the Entra ID steps from the SSO article and advised testing before enforcing." },
  { n: 4106, customer: "Brightwater Lettings", plan: "starter", category: "auth", priority: 2, team: "security", article_id: "KB-102", resolution_minutes: 180, csat: 4,
    subject: "Lost phone, locked out of 2FA", message: "I'm the only owner and my phone was stolen. I can't log in at all.",
    resolution: "Sole owner — identity verified via company domain email, card last 4 and last invoice number. Security reset 2FA; advised saving backup codes." },
  { n: 4107, customer: "Oakham Print Co", plan: "growth", category: "auth", priority: 3, team: "L1", article_id: "KB-102", resolution_minutes: 15, csat: 5,
    subject: "Staff member can't get 2FA code", message: "One of our bookkeepers changed phones and can't log in.",
    resolution: "Another Owner reset the user's 2FA from Settings → Team. Resolved without support involvement." },
  { n: 4108, customer: "Fenwick Garden Supplies", plan: "starter", category: "invoicing", priority: 3, team: "L1", article_id: "KB-103", resolution_minutes: 20, csat: 5,
    subject: "Invoice won't leave draft", message: "I click Approve & send and it just stays in draft.",
    resolution: "Line item used an archived 20% tax rate from an old setup. Selected the active rate and it sent." },
  { n: 4109, customer: "Kestrel Events Ltd", plan: "growth", category: "invoicing", priority: 3, team: "L1", article_id: "KB-103", resolution_minutes: 35, csat: 4,
    subject: "Invoices dated last quarter stuck", message: "Can't send invoices dated in June — they stay draft with a lock warning.",
    resolution: "VAT period locked by their accountant. Adviser unlocked the date temporarily; invoices sent." },
  { n: 4110, customer: "Ridgeway Vets", plan: "growth", category: "invoicing", priority: 3, team: "L1", article_id: "KB-118", resolution_minutes: 60, csat: 4,
    subject: "Customers say invoices go to spam", message: "Several clients found our invoices in their junk folder.",
    resolution: "Custom sending domain had SPF but no DKIM; strict DMARC. Added DKIM CNAMEs, verified next day." },
  { n: 4111, customer: "Thornbury Plumbing", plan: "starter", category: "tax", priority: 2, team: "L2-billing", article_id: "KB-105", resolution_minutes: 70, csat: 5,
    subject: "VAT return error INVALID_CREDENTIALS", message: "Trying to submit our VAT return, deadline is Friday, keep getting INVALID_CREDENTIALS.",
    resolution: "HMRC authorisation had expired after 18 months. Reconnected with the Government Gateway ID that owns the VRN; return submitted." },
  { n: 4112, customer: "Aldgate Coffee Roasters", plan: "growth", category: "tax", priority: 2, team: "L2-billing", article_id: "KB-105", resolution_minutes: 90, csat: 3,
    subject: "MTD says duplicate submission", message: "HMRC returned DUPLICATE_SUBMISSION but we never got confirmation.",
    resolution: "Return had been accepted on first attempt; confirmation email delayed. Verified in the HMRC account; advised not to resubmit." },
  { n: 4113, customer: "Wyecliffe Holiday Cottages", plan: "starter", category: "tax", priority: 3, team: "L1", article_id: "KB-105", resolution_minutes: 30, csat: 4,
    subject: "VAT obligation not found", message: "Ledgerly says no obligation for Q2 but HMRC shows it's due.",
    resolution: "Stagger dates had changed. Refreshed obligations from HMRC; correct period appeared." },
  { n: 4114, customer: "Marlow Physio Clinic", plan: "growth", category: "tax", priority: 3, team: "L2-billing", article_id: "KB-105", resolution_minutes: 45, csat: 4,
    subject: "Made a mistake on submitted VAT return", message: "We submitted the wrong figure. Can you amend it?",
    resolution: "Explained accepted returns can't be amended; error under threshold so corrected in next return per HMRC rules." },
  { n: 4115, customer: "Pennine Outdoor Gear", plan: "growth", category: "billing", priority: 2, team: "L2-billing", article_id: "KB-106", resolution_minutes: 50, csat: 2,
    subject: "Want to cancel and get a refund", message: "We renewed annually 3 weeks ago by accident and want our money back.",
    resolution: "Outside the 14-day window. Per policy no refund; set plan not to renew. L2 billing declined goodwill exception. Customer unhappy." },
  { n: 4116, customer: "Saltmarsh Studio", plan: "starter", category: "billing", priority: 3, team: "L2-billing", article_id: "KB-106", resolution_minutes: 20, csat: 5,
    subject: "Charged twice this month", message: "Our card statement shows two Ledgerly charges for September.",
    resolution: "Confirmed duplicate charge from a payment retry. Refunded the duplicate in full; 5–10 working days to appear." },
  { n: 4117, customer: "Highfield Recruitment", plan: "growth", category: "billing", priority: 3, team: "L2-billing", article_id: "KB-106", resolution_minutes: 25, csat: 4,
    subject: "Annual renewal refund", message: "Renewed 5 days ago but we're moving to another tool — refund please.",
    resolution: "Within 14 days of renewal: processed full refund and cancelled." },
  { n: 4118, customer: "Gresham Logistics plc", plan: "enterprise", category: "billing", priority: 2, team: "L2-billing", article_id: "KB-106", resolution_minutes: 240, csat: 4,
    subject: "SLA credit claim after outage", message: "The 2-hour outage on the 3rd breached our SLA. We want our credit.",
    resolution: "Monthly uptime 99.71%: two full hours below target → 20% service credit applied to next invoice, within 30-day claim window." },
  { n: 4119, customer: "Copperleaf Florists", plan: "starter", category: "billing", priority: 4, team: "L1", article_id: "KB-107", resolution_minutes: 10, csat: 5,
    subject: "What happens if I downgrade?", message: "If I go back to Starter do I lose my multi-currency invoices?",
    resolution: "Explained downgrade at renewal; features become read-only, nothing deleted." },
  { n: 4120, customer: "Brackenridge Surveyors", plan: "growth", category: "billing", priority: 4, team: "L1", article_id: "KB-107", resolution_minutes: 15, csat: 5,
    subject: "Upgrade proration question", message: "If we upgrade to Enterprise mid-month how much do we pay?",
    resolution: "Explained immediate upgrade with prorated charge; pointed to Billing → History for the invoice." },
  { n: 4121, customer: "Quayside Software Ltd", plan: "growth", category: "api", priority: 2, team: "L2-integrations", article_id: "KB-108", resolution_minutes: 55, csat: 4,
    subject: "Getting 429 errors from the API", message: "Our nightly sync fails with 429 Too Many Requests after a few hundred calls.",
    resolution: "Looping single-invoice POSTs at ~500/min over the Growth limit. Switched to /v2/invoices/batch and honoured Retry-After; sync runs clean." },
  { n: 4122, customer: "Tidewater Payments", plan: "enterprise", category: "api", priority: 2, team: "L2-integrations", article_id: "KB-108", resolution_minutes: 1440, csat: 5,
    subject: "Need temporary rate limit raise for migration", message: "Migrating 2 million invoices next week, current limits too low.",
    resolution: "Account manager approved a temporary 5,000/min raise for 72 hours with 2 days' notice." },
  { n: 4123, customer: "Ashdown Analytics", plan: "growth", category: "api", priority: 3, team: "L2-integrations", article_id: "KB-108", resolution_minutes: 40, csat: 4,
    subject: "429 with only a few requests", message: "We get 429s even though we only make ~50 requests a minute.",
    resolution: "They were firing 20 requests concurrently; concurrency limit is 5 per token. Added a client-side semaphore." },
  { n: 4124, customer: "Ferrybridge Commerce", plan: "growth", category: "api", priority: 2, team: "L2-integrations", article_id: "KB-109", resolution_minutes: 80, csat: 5,
    subject: "Webhook signature never matches", message: "Our HMAC check fails on every webhook even with the right secret.",
    resolution: "Their framework parsed JSON before verifying. Switched to raw-body verification; signatures matched." },
  { n: 4125, customer: "Lanyard Systems", plan: "enterprise", category: "api", priority: 2, team: "L2-integrations", article_id: "KB-109", resolution_minutes: 65, csat: 4,
    subject: "Webhooks failing after secret rotation", message: "We rotated the signing secret yesterday and now all webhooks are rejected.",
    resolution: "They updated the secret in staging only. Updated production; replayed failed deliveries from the log." },
  { n: 4126, customer: "Orchard Way Retail", plan: "growth", category: "api", priority: 3, team: "L2-integrations", article_id: "KB-109", resolution_minutes: 35, csat: 4,
    subject: "Missed webhooks over the weekend", message: "Our endpoint was down Saturday; did we lose the events?",
    resolution: "Retries run for 72 hours; most delivered on recovery. Replayed the remainder from the delivery log." },
  { n: 4127, customer: "Kingfisher Tutors", plan: "starter", category: "data", priority: 4, team: "L1", article_id: "KB-110", resolution_minutes: 15, csat: 5,
    subject: "CSV import shows weird £ characters", message: "After importing contacts all the pound signs show as Â£.",
    resolution: "File saved as ANSI. Re-saved as CSV UTF-8 and re-imported." },
  { n: 4128, customer: "Elmstead Builders", plan: "growth", category: "data", priority: 3, team: "L1", article_id: "KB-110", resolution_minutes: 30, csat: 4,
    subject: "Invoice import rejects dates", message: "Import says every row has an invalid date.",
    resolution: "Dates were MM/DD/YYYY from a US export. Converted to DD/MM/YYYY; import succeeded." },
  { n: 4129, customer: "Rowan Hill Nurseries", plan: "growth", category: "data", priority: 3, team: "L1", article_id: "KB-112", resolution_minutes: 20, csat: 5,
    subject: "Need a full export before we leave", message: "We're closing the business. How do we get all our records?",
    resolution: "Owner ran Full export; link valid 7 days; explained 90-day retention after cancellation." },
  { n: 4130, customer: "Cobalt Marine Services", plan: "enterprise", category: "account", priority: 2, team: "security", article_id: "KB-113", resolution_minutes: 600, csat: 4,
    subject: "GDPR erasure request from former client", message: "A former client's solicitor demands we erase all their data from Ledgerly.",
    resolution: "Mentioned legal action → routed to security & privacy. Explained controller responsibility; contact anonymised, invoices retained 6 years for HMRC." },
  { n: 4131, customer: "Hazelmere Care Homes", plan: "growth", category: "account", priority: 3, team: "L1", article_id: "KB-114", resolution_minutes: 15, csat: 5,
    subject: "Give accountant access without settings", message: "Our external accountant needs to do VAT but not change billing.",
    resolution: "Recommended the Adviser role; Owner assigned it." },
  { n: 4132, customer: "Stonegate Electrical", plan: "starter", category: "invoicing", priority: 3, team: "L1", article_id: "KB-115", resolution_minutes: 25, csat: 4,
    subject: "Recurring invoice didn't generate", message: "Our monthly retainer invoice didn't go out on the 1st.",
    resolution: "Template's customer had been archived. Unarchived and clicked Generate now." },
  { n: 4133, customer: "Wren & Daughters Solicitors", plan: "growth", category: "invoicing", priority: 4, team: "L1", article_id: "KB-116", resolution_minutes: 15, csat: 5,
    subject: "Reminders not sending for one client", message: "Payment reminders aren't going to one particular client.",
    resolution: "Invoice was marked Disputed, which suppresses reminders. Cleared the flag once resolved." },
  { n: 4134, customer: "Beacon Fitness Ltd", plan: "growth", category: "integrations", priority: 3, team: "L2-integrations", article_id: "KB-117", resolution_minutes: 240, csat: 4,
    subject: "Migrating from Xero — trial balance off", message: "After trial import our trial balance differs from Xero by £1,240.",
    resolution: "Conversion date mismatch; one month of bills counted twice. Re-ran trial with correct date; balances matched." },
  { n: 4135, customer: "Larkspur Media", plan: "enterprise", category: "integrations", priority: 3, team: "L2-integrations", article_id: "KB-117", resolution_minutes: 2880, csat: 5,
    subject: "Assisted QuickBooks migration", message: "We want help moving 6 entities from QuickBooks Online.",
    resolution: "Onboarding team ran assisted migration; bank feeds reconnected after go-live." },
  { n: 4136, customer: "Gresham Logistics plc", plan: "enterprise", category: "invoicing", priority: 1, team: "engineering", article_id: "", resolution_minutes: 75, csat: 4,
    subject: "URGENT: no invoices sending for anyone", message: "None of our invoices are sending, all stuck in 'Sending'. This is blocking month end!!",
    resolution: "Platform incident: email provider outage. Engineering failed over to secondary provider; queued invoices sent; status page updated." },
  { n: 4137, customer: "Quayside Software Ltd", plan: "growth", category: "api", priority: 1, team: "engineering", article_id: "", resolution_minutes: 50, csat: 3,
    subject: "API returning 500 errors on all endpoints", message: "Every API call returns 500 since 09:10. Our checkout is down.",
    resolution: "Bad deploy to the API gateway. Engineering rolled back at 09:58; incident report sent." },
  { n: 4138, customer: "Harbour Lane Bakery Ltd", plan: "starter", category: "account", priority: 1, team: "engineering", article_id: "", resolution_minutes: 45, csat: 4,
    subject: "Can't log in, site down", message: "Ledgerly won't load at all — just a blank page. Is it down?",
    resolution: "Regional CDN outage. Engineering re-routed traffic; confirmed via status page." },
  { n: 4139, customer: "Pennine Outdoor Gear", plan: "growth", category: "billing", priority: 2, team: "L2-billing", article_id: "KB-106", resolution_minutes: 90, csat: 2,
    subject: "This is unacceptable, cancelling", message: "THIRD time the bank feed broke. Cancel our account and refund the year or we'll take this further.",
    resolution: "Escalated to L2 billing. Explained 90-day Open Banking consent; outside refund window; set non-renewal. No goodwill credit approved." },
  { n: 4140, customer: "Ashdown Analytics", plan: "growth", category: "integrations", priority: 3, team: "L2-integrations", article_id: "KB-111", resolution_minutes: 45, csat: 4,
    subject: "FX revaluation looks wrong", message: "Month-end USD revaluation posted a huge loss.",
    resolution: "Manual rate override left on one invoice. Removed override; re-ran Revalue currencies." },
];

export const TICKETS: SeedDoc[] = PAST.map(ticketDoc);

export interface InboxTicket {
  id: string;
  customer: string;
  plan: Plan;
  subject: string;
  body: string;
}

export const INBOX: InboxTicket[] = [
  { id: "NEW-5001", customer: "Gresham Logistics plc", plan: "enterprise",
    subject: "Invoices stuck in Sending — month end today",
    body: "Since about 08:30 none of our invoices are going out, they all sit in 'Sending'. It's our month end close today and finance is blocked. Is there an outage?? Please call us ASAP!!" },
  { id: "NEW-5002", customer: "Pennine Outdoor Gear", plan: "growth",
    subject: "CANCEL MY ACCOUNT",
    body: "Your bank feed has disconnected AGAIN. I am sick of this. Cancel the subscription today and refund the annual fee we paid last month, or I'll be speaking to my solicitor." },
  { id: "NEW-5003", customer: "Moorcroft Dental Group", plan: "growth",
    subject: "Okta SSO setup question",
    body: "We're moving to Okta. What do I put in the ACS URL and Entity ID fields, and should we turn on enforce SSO straight away?" },
  { id: "NEW-5004", customer: "Thornbury Plumbing", plan: "starter",
    subject: "VAT return won't submit",
    body: "Trying to file our VAT return, due in 3 days. Ledgerly says CLIENT_OR_AGENT_NOT_AUTHORISED. Our accountant set the HMRC link up ages ago." },
  { id: "NEW-5005", customer: "Ferrybridge Commerce", plan: "growth",
    subject: "Lots of 429s from invoices endpoint",
    body: "Our order sync creates an invoice per order and since Black Friday traffic started we get HTTP 429 on most calls. What are the limits and how should we handle this?" },
  { id: "NEW-5006", customer: "Lanyard Systems", plan: "enterprise",
    subject: "Webhook signatures invalid since this morning",
    body: "Our webhook handler started rejecting every event at 07:00 with signature mismatch. We rotated the signing secret last night. Have you changed the signing format?" },
];
