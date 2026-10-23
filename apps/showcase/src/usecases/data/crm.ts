/**
 * CRM accounts for Entity Resolve + Lookalike demos (fictional UK B2B).
 */
import type { SeedDoc } from "../../rag/seed";

export const SEED_VERSION = "crm-v1";
export const CRM_BUCKET = "crm_accounts";

export const INDUSTRIES = ["saas", "fintech", "retail", "manufacturing", "public"] as const;
export type Industry = (typeof INDUSTRIES)[number];

function acct(
  id: string,
  name: string,
  industry: Industry,
  country: string,
  arr_gbp: number,
  employees: number,
  stage: "prospect" | "customer" | "churned",
  domain: string,
  blurb: string
): SeedDoc {
  return {
    id,
    text: `${name} (${domain}) · ${industry} · ${country} · ${stage}\nARR £${arr_gbp.toLocaleString()} · ${employees} employees\n${blurb}`,
    metadata: {
      account_id: id,
      name,
      industry,
      country,
      arr_gbp,
      employees,
      stage,
      domain,
    },
  };
}

export const ACCOUNTS: SeedDoc[] = [
  acct("ACC-100", "Ledgerly Ltd", "saas", "GB", 420_000, 48, "customer", "ledgerly.example",
    "UK invoicing SaaS for accountants. Selling into mid-market practices. Strong Open Banking integrations."),
  acct("ACC-101", "Ledgerlee Accounting", "saas", "GB", 0, 12, "prospect", "ledgerlee.example",
    "Small bookkeeping SaaS — near-duplicate name of Ledgerly; same ICP, early stage, no ARR yet."),
  acct("ACC-102", "Ledgerly Solutions Inc", "saas", "US", 180_000, 22, "prospect", "ledgerly-us.example",
    "US entity spelling of Ledgerly brand — possible duplicate of ACC-100 for merge review."),
  acct("ACC-110", "PayNest Fintech", "fintech", "GB", 1_200_000, 95, "customer", "paynest.example",
    "Embedded payments for marketplaces. PCI DSS, multi-acquirer routing, strong risk team."),
  acct("ACC-111", "NestPay Markets", "fintech", "GB", 0, 18, "prospect", "nestpay.example",
    "Early payments startup — similar pitch to PayNest; lookalike for outbound."),
  acct("ACC-112", "ClearSettle Ltd", "fintech", "IE", 650_000, 40, "customer", "clearsettle.example",
    "EU settlement rails, Instant SEPA, treasury APIs for fintechs."),
  acct("ACC-120", "Shopwright PLC", "retail", "GB", 2_800_000, 310, "customer", "shopwright.example",
    "Omnichannel retailer. Needs catalogue search, stock SOT, store associate copilots."),
  acct("ACC-121", "WrightShop Online", "retail", "GB", 0, 25, "prospect", "wrightshop.example",
    "DTC retail — name collision risk with Shopwright; different company number."),
  acct("ACC-130", "NorthGrid Mobile", "manufacturing", "GB", 5_100_000, 800, "customer", "northgrid.example",
    "Telecom equipment OEM. Observability and multi-region HA requirements."),
  acct("ACC-131", "GridNorth Components", "manufacturing", "DE", 900_000, 120, "prospect", "gridnorth.example",
    "Industrial IoT sensors — similar to NorthGrid supply chain, DACH focus."),
  acct("ACC-140", "City of Riverside", "public", "GB", 350_000, 0, "customer", "riverside.gov.example",
    "Local authority digital services. UK residency and DPIA-heavy buying."),
  acct("ACC-141", "Riverside Borough Council", "public", "GB", 0, 0, "prospect", "riverside-bc.gov.example",
    "Likely same economic buyer as City of Riverside — entity-resolution candidate."),
  acct("ACC-150", "MutualCover", "fintech", "GB", 0, 60, "churned", "mutualcover.example",
    "Insurer that churned over CMK timeline. Useful negative lookalike."),
  acct("ACC-160", "BrightBooks Cloud", "saas", "GB", 95_000, 15, "customer", "brightbooks.example",
    "SMB accounting adjacent to Ledgerly — expansion lookalike."),
  acct("ACC-161", "InvoiceHive", "saas", "NL", 210_000, 28, "prospect", "invoicehive.example",
    "EU invoicing — lookalike to Ledgerly for geo expansion plays."),
];

/** Seeds shown as “suspect pairs” for the Entity Resolve UI. */
export const SUSPECT_SEEDS = [
  { left: "ACC-100", right: "ACC-101", reason: "Near-identical trading name" },
  { left: "ACC-100", right: "ACC-102", reason: "Same brand, different country entity" },
  { left: "ACC-120", right: "ACC-121", reason: "Name collision in retail" },
  { left: "ACC-140", right: "ACC-141", reason: "Same council / authority cluster" },
];
