/**
 * Demo data for the Stock Sourcer — the back office of a GPU workstation and
 * parts shop (modelled on Workstation, workstation.co.uk).
 *
 * - CATALOG: what the shop sells, with stock on hand, reorder point and the
 *   margin it needs. SYS-* items are machines the shop builds from parts (BOM).
 * - SUPPLIERS: six fictional suppliers — authorised distributors, a refurb
 *   specialist and two brokers — with a reliability score from past orders.
 * - OFFERS: the lines of the stock lists and price emails those suppliers sent
 *   over the last two weeks, in their own words. The agent finds the offers
 *   for each catalog item by vector search; `sku` here is the answer key for
 *   the tests only and is not stored in NebulaDB.
 * - ENQUIRIES: customer emails in the sales inbox.
 *
 * All suppliers, customers and people are fictional (.example addresses);
 * prices are illustrative.
 */

export const TODAY = "2026-11-18";
/** Offers older than this many days are stale: shown, never bought from. */
export const FRESH_DAYS = 7;

export type Currency = "GBP" | "EUR" | "USD";
/** GBP per unit of currency (the rate the agent uses for landed cost). */
export const FX: Record<Currency, number> = { GBP: 1, EUR: 0.86, USD: 0.79 };

export type Condition = "new" | "refurb" | "used";

export interface Product {
  sku: string;
  name: string;
  category: "GPU" | "CPU" | "Memory" | "Storage" | "Networking" | "Power" | "System";
  description: string;
  condition: "new" | "refurb";
  /** Our selling price, GBP ex VAT. */
  sell: number;
  stock: number;
  reorderPoint: number;
  /** Minimum gross margin on the landed cost, 0–1. */
  targetMargin: number;
  /** Words an offer must contain (letters and digits only, case-insensitive) to be this product. */
  must: string[];
  /** Words that make an offer a different product even when it looks close. */
  mustNot: string[];
  /** Built-to-order machines: parts per unit. */
  bom?: Record<string, number>;
  buildDays?: number;
}

export interface Supplier {
  id: string;
  name: string;
  kind: "authorised distributor" | "refurb specialist" | "broker";
  country: string;
  currency: Currency;
  contact: string;
  email: string;
  /** On-time, as-described delivery rate over past orders, 0–100. */
  reliability: number;
  /** Freight, duty and brokerage on top of the price, 0–1. */
  importPct: number;
  terms: string;
}

export interface Offer {
  id: string;
  supplier: string;
  date: string;
  /** The line as the supplier wrote it. */
  text: string;
  qty: number;
  price: number;
  condition: Condition;
  leadDays: number;
  /** Answer key for tests: the catalog item this line really is (undefined = none of ours). */
  sku?: string;
}

export interface EnquiryLine {
  text: string;
  qty: number;
  /** Answer key for tests. */
  sku: string;
}

export interface Enquiry {
  id: string;
  received: string;
  company: string;
  contact: string;
  email: string;
  deadline: string;
  body: string;
  lines: EnquiryLine[];
}

export const CATALOG: Product[] = [
  { sku: "GPU-5090", name: "NVIDIA GeForce RTX 5090 32GB", category: "GPU", description: "Blackwell desktop GPU, 32GB GDDR7, for local LLM inference and rendering.", condition: "new", sell: 2449, stock: 3, reorderPoint: 8, targetMargin: 0.12, must: ["5090"], mustNot: ["5090d", "laptop"] },
  { sku: "GPU-6000", name: "NVIDIA RTX PRO 6000 Blackwell Workstation Edition 96GB", category: "GPU", description: "Workstation GPU, 96GB GDDR7, 600W, actively cooled.", condition: "new", sell: 8499, stock: 1, reorderPoint: 4, targetMargin: 0.15, must: ["6000", "96gb"], mustNot: ["maxq", "ada", "server"] },
  { sku: "GPU-6000M", name: "NVIDIA RTX PRO 6000 Blackwell Max-Q 96GB", category: "GPU", description: "300W Max-Q workstation GPU, 96GB GDDR7, blower cooler for multi-GPU builds.", condition: "new", sell: 8299, stock: 0, reorderPoint: 2, targetMargin: 0.15, must: ["6000", "maxq"], mustNot: ["ada"] },
  { sku: "GPU-L40S", name: "NVIDIA L40S 48GB PCIe", category: "GPU", description: "Data-centre GPU, 48GB GDDR6, passive cooling for servers.", condition: "new", sell: 7150, stock: 2, reorderPoint: 2, targetMargin: 0.1, must: ["l40s"], mustNot: [] },
  { sku: "GPU-H200", name: "NVIDIA H200 NVL 141GB PCIe", category: "GPU", description: "Hopper data-centre GPU, 141GB HBM3e, PCIe card for fine-tuning and inference servers.", condition: "new", sell: 27900, stock: 0, reorderPoint: 1, targetMargin: 0.1, must: ["h200"], mustNot: ["sxm", "hgx"] },
  { sku: "GPU-A100R", name: "NVIDIA A100 80GB PCIe (refurbished)", category: "GPU", description: "Refurbished Ampere data-centre GPU, 80GB HBM2e, tested with 12-month warranty.", condition: "refurb", sell: 9450, stock: 1, reorderPoint: 2, targetMargin: 0.15, must: ["a100", "80gb"], mustNot: ["sxm", "40gb"] },
  { sku: "CPU-9975", name: "AMD Ryzen Threadripper PRO 9975WX 32-core", category: "CPU", description: "Workstation CPU, 32 cores, 8-channel DDR5, 128 PCIe 5.0 lanes.", condition: "new", sell: 3999, stock: 4, reorderPoint: 3, targetMargin: 0.12, must: ["9975wx"], mustNot: [] },
  { sku: "CPU-9655", name: "AMD EPYC 9655 96-core", category: "CPU", description: "Server CPU, 96 cores, dual-socket capable, SP5.", condition: "new", sell: 9200, stock: 1, reorderPoint: 2, targetMargin: 0.12, must: ["9655"], mustNot: ["9655p"] },
  { sku: "MEM-6496", name: "64GB DDR5-6400 ECC RDIMM", category: "Memory", description: "Registered ECC server and workstation memory, 64GB module, 6400 MT/s.", condition: "new", sell: 389, stock: 24, reorderPoint: 48, targetMargin: 0.1, must: ["64gb", "6400"], mustNot: ["udimm", "sodimm"] },
  { sku: "MEM-9664", name: "96GB DDR5-6400 ECC RDIMM", category: "Memory", description: "Registered ECC memory, 96GB module, 6400 MT/s.", condition: "new", sell: 589, stock: 8, reorderPoint: 16, targetMargin: 0.1, must: ["96gb", "6400"], mustNot: ["udimm"] },
  { sku: "SSD-7680", name: "7.68TB U.3 enterprise NVMe SSD", category: "Storage", description: "Data-centre NVMe SSD, 7.68TB, U.3, PCIe 4.0 (Micron 7500 PRO or equivalent).", condition: "new", sell: 899, stock: 6, reorderPoint: 6, targetMargin: 0.12, must: ["768tb"], mustNot: ["1536"] },
  { sku: "NIC-CX7", name: "NVIDIA ConnectX-7 400GbE single-port adapter", category: "Networking", description: "400Gb/s Ethernet / NDR InfiniBand adapter, OSFP, for GPU clusters.", condition: "new", sell: 1690, stock: 2, reorderPoint: 4, targetMargin: 0.12, must: ["connectx7", "400"], mustNot: [] },
  { sku: "PSU-1600", name: "1600W 80 PLUS Titanium ATX 3.1 power supply", category: "Power", description: "1600W PSU with two 12V-2x6 connectors for dual-GPU workstations.", condition: "new", sell: 489, stock: 9, reorderPoint: 6, targetMargin: 0.15, must: ["1600w"], mustNot: [] },
  {
    sku: "SYS-W2",
    name: "Workstation AI W2 dual-GPU workstation",
    category: "System",
    description: "Built to order: 2 × RTX PRO 6000 Blackwell 96GB, Threadripper PRO 9975WX, 384GB DDR5 ECC, 7.68TB NVMe, 1600W. For fine-tuning, local LLMs and rendering.",
    condition: "new",
    sell: 24990,
    stock: 0,
    reorderPoint: 0,
    targetMargin: 0.12,
    must: [],
    mustNot: [],
    bom: { "GPU-6000": 2, "CPU-9975": 1, "MEM-9664": 4, "SSD-7680": 1, "PSU-1600": 1 },
    buildDays: 5,
  },
];

export const SUPPLIERS: Supplier[] = [
  { id: "NB", name: "Northbeam Distribution", kind: "authorised distributor", country: "UK", currency: "GBP", contact: "Hannah Moss", email: "hannah.moss@northbeam-dist.example", reliability: 96, importPct: 0.005, terms: "30 days" },
  { id: "RF", name: "Rheinfeld Compute GmbH", kind: "authorised distributor", country: "Germany", currency: "EUR", contact: "Jonas Weber", email: "j.weber@rheinfeld-compute.example", reliability: 93, importPct: 0.025, terms: "30 days" },
  { id: "BS", name: "Bayshore Server Parts", kind: "authorised distributor", country: "Taiwan", currency: "USD", contact: "Kevin Lin", email: "kevin.lin@bayshore-parts.example", reliability: 88, importPct: 0.04, terms: "50% deposit" },
  { id: "KT", name: "Kestrel IT Trading", kind: "broker", country: "UK", currency: "GBP", contact: "Dean Carter", email: "dean@kestrel-it.example", reliability: 71, importPct: 0.005, terms: "pro forma" },
  { id: "PR", name: "Pennine Renew Systems", kind: "refurb specialist", country: "UK", currency: "GBP", contact: "Sophie Hall", email: "sophie.hall@pennine-renew.example", reliability: 90, importPct: 0.01, terms: "30 days" },
  { id: "GF", name: "Goldfinch Global Supply", kind: "broker", country: "Hong Kong", currency: "USD", contact: "Sales desk", email: "sales@goldfinch-global.example", reliability: 48, importPct: 0.05, terms: "TT in advance" },
];

const o = (id: string, supplier: string, date: string, text: string, qty: number, price: number, leadDays: number, sku?: string, condition: Condition = "new"): Offer => ({ id, supplier, date, text, qty, price, condition, leadDays, sku });

export const OFFERS: Offer[] = [
  // RTX 5090 — prices creeping up; a broker's "too good" bulk lot and a China-market 5090 D.
  o("of-01", "NB", "2026-11-06", "NVIDIA GeForce RTX 5090 32GB GDDR7 (PNY retail box) — 10 in stock — £2,040 each ex VAT — next-day dispatch", 10, 2040, 2, "GPU-5090"),
  o("of-02", "NB", "2026-11-13", "NVIDIA GeForce RTX 5090 32GB GDDR7 (PNY retail box) — 6 in stock — £2,060 each ex VAT", 6, 2060, 2, "GPU-5090"),
  o("of-03", "NB", "2026-11-18", "RTX 5090 32GB GDDR7 PNY — 4 left — £2,110 each ex VAT — allocation tight into December", 4, 2110, 2, "GPU-5090"),
  o("of-04", "RF", "2026-11-14", "GeForce RTX 5090 32GB OEM 2-slot — 10 Stk — EUR 2,390 per unit", 10, 2390, 4, "GPU-5090"),
  o("of-05", "RF", "2026-11-18", "GeForce RTX 5090 32GB OEM 2-slot — 8 Stk — EUR 2,450 per unit — ETA 4 working days", 8, 2450, 4, "GPU-5090"),
  o("of-06", "KT", "2026-11-15", "5090 32GB Gigabyte Windforce x3 sealed, £1,990 each, courier next day", 3, 1990, 1, "GPU-5090"),
  o("of-07", "GF", "2026-11-16", "RTX 5090 D 32GB China-market cards, 40 pcs, USD 1,350", 40, 1350, 12),
  o("of-08", "GF", "2026-11-17", "RTX 5090 32GB brand new sealed bulk 50 pcs USD 1,290 TT in advance", 50, 1290, 12, "GPU-5090"),
  // RTX PRO 6000 Blackwell — three real sources and two near misses (Ada, Server Edition).
  o("of-09", "NB", "2026-11-16", "PNY NVIDIA RTX PRO 6000 Blackwell Workstation Edition 96GB GDDR7 — 2 units — £6,940 each", 2, 6940, 2, "GPU-6000"),
  o("of-10", "RF", "2026-11-17", "RTX PRO 6000 Blackwell 96GB Workstation Edition (VCNRTXPRO6000) — 6 Stk — EUR 7,950", 6, 7950, 4, "GPU-6000"),
  o("of-11", "BS", "2026-11-12", "RTX PRO 6000 Blackwell WS 96GB bulk pack, 10 pcs, USD 8,450, ships in 7-9 days", 10, 8450, 9, "GPU-6000"),
  o("of-12", "KT", "2026-11-15", "NVIDIA RTX 6000 Ada Generation 48GB x5 £5,200", 5, 5200, 1),
  o("of-13", "RF", "2026-11-17", "RTX PRO 6000 Blackwell Server Edition 96GB passive — 4 Stk — EUR 7,600", 4, 7600, 4),
  o("of-14", "RF", "2026-11-17", "RTX PRO 6000 Blackwell Max-Q Workstation Edition 96GB — 3 Stk — EUR 7,700", 3, 7700, 4, "GPU-6000M"),
  o("of-15", "NB", "2026-11-10", "RTX PRO 6000 Max-Q 96GB — 1 unit — £7,050", 1, 7050, 2, "GPU-6000M"),
  // L40S / H200 / A100
  o("of-16", "NB", "2026-11-12", "NVIDIA L40S 48GB PCIe passive — 1 unit — £6,350", 1, 6350, 2, "GPU-L40S"),
  o("of-17", "BS", "2026-11-14", "L40S 48GB GDDR6 PCIe, 8 pcs, USD 7,600, 7 days", 8, 7600, 7, "GPU-L40S"),
  o("of-18", "KT", "2026-11-16", "NVIDIA L40 48GB PCIe x2 £4,900", 2, 4900, 1),
  o("of-19", "NB", "2026-11-17", "NVIDIA H200 NVL 141GB PCIe — allocation only, 0 available, next allocation January — £24,900", 0, 24900, 45, "GPU-H200"),
  o("of-20", "BS", "2026-11-15", "H200 NVL 141GB HBM3e PCIe — 2 pcs — USD 29,800 — 10-14 days", 2, 29800, 14, "GPU-H200"),
  o("of-21", "GF", "2026-11-16", "H200 141GB SXM5 modules pulled from HGX boards, 16 pcs, USD 19,000", 16, 19000, 12),
  o("of-22", "PR", "2026-11-13", "NVIDIA A100 80GB PCIe refurbished, fully tested, 12 month warranty — 4 units — £7,450", 4, 7450, 3, "GPU-A100R", "refurb"),
  o("of-23", "KT", "2026-11-14", "A100 40GB PCIe used x6 £3,600", 6, 3600, 1, undefined, "used"),
  o("of-24", "GF", "2026-11-17", "A100 80GB PCIe used, 30 pcs, USD 4,100, no warranty", 30, 4100, 12, "GPU-A100R", "used"),
  // CPUs
  o("of-25", "NB", "2026-11-14", "AMD Ryzen Threadripper PRO 9975WX 32-core WOF — 12 in stock — £3,420", 12, 3420, 2, "CPU-9975"),
  o("of-26", "RF", "2026-11-13", "AMD EPYC 9655 96-Core 2.6GHz tray — 5 Stk — EUR 8,300", 5, 8300, 4, "CPU-9655"),
  o("of-27", "NB", "2026-11-17", "AMD EPYC 9655 96-core tray — 2 in stock — £7,390", 2, 7390, 2, "CPU-9655"),
  o("of-28", "GF", "2026-11-15", "EPYC 9655P 96C tray x20 USD 6,900", 20, 6900, 12),
  // DDR5 RDIMM — prices up sharply in two weeks.
  o("of-29", "NB", "2026-11-06", "64GB DDR5-6400 ECC RDIMM 2Rx4 (Samsung) — 120 in stock — £349", 120, 349, 2, "MEM-6496"),
  o("of-30", "NB", "2026-11-12", "64GB DDR5-6400 ECC RDIMM 2Rx4 (Samsung) — 80 in stock — £372", 80, 372, 2, "MEM-6496"),
  o("of-31", "NB", "2026-11-18", "64GB DDR5-6400 ECC RDIMM 2Rx4 (Samsung) — 40 in stock — £398 — DRAM prices rising weekly", 40, 398, 2, "MEM-6496"),
  o("of-32", "RF", "2026-11-17", "64GB DDR5 6400 RDIMM ECC Registered (Hynix) — 200 Stk — EUR 455", 200, 455, 4, "MEM-6496"),
  o("of-33", "KT", "2026-11-16", "DDR5 64GB 6400 RDIMM mixed brands x64 £379", 64, 379, 1, "MEM-6496"),
  o("of-34", "KT", "2026-11-16", "64GB DDR5-5600 RDIMM x100 £310", 100, 310, 1),
  o("of-35", "NB", "2026-11-16", "96GB DDR5-6400 ECC RDIMM 2Rx4 — 30 in stock — £548", 30, 548, 2, "MEM-9664"),
  o("of-36", "RF", "2026-11-17", "96GB DDR5 6400 RDIMM ECC — 50 Stk — EUR 655", 50, 655, 4, "MEM-9664"),
  // Storage, networking, power
  o("of-37", "NB", "2026-11-15", "Micron 7500 PRO 7.68TB U.3 NVMe — 20 in stock — £780", 20, 780, 2, "SSD-7680"),
  o("of-38", "BS", "2026-11-06", "NVIDIA ConnectX-7 400GbE single-port OSFP (MCX75310AAS-NEAT), 6 pcs, USD 1,520", 6, 1520, 9, "NIC-CX7"),
  o("of-39", "KT", "2026-11-15", "ConnectX-7 200GbE dual-port x4 £980", 4, 980, 1),
  o("of-40", "NB", "2026-11-12", "1600W 80 PLUS Titanium ATX 3.1 PSU — 25 in stock — £352", 25, 352, 2, "PSU-1600"),
];

export const ENQUIRIES: Enquiry[] = [
  {
    id: "ENQ-3101",
    received: "2026-11-17",
    company: "Halden University Robotics Lab",
    contact: "Dr Priya Raman",
    email: "p.raman@halden-uni.example",
    deadline: "2026-12-12",
    body: "Hi, we have grant money to spend before the end of term. Could you quote 4 x RTX PRO 6000 Blackwell 96GB workstation cards? Delivery to campus by 12 December at the latest. We can raise a PO this week.",
    lines: [{ text: "RTX PRO 6000 Blackwell 96GB workstation cards", qty: 4, sku: "GPU-6000" }],
  },
  {
    id: "ENQ-3102",
    received: "2026-11-18",
    company: "Lumen & Pike VFX",
    contact: "Tom Achebe",
    email: "tom@lumenpike-vfx.example",
    deadline: "2026-11-25",
    body: "We need two of your W2 dual-GPU AI workstations for a render and fine-tuning job that starts on 26 November, plus 2 spare 1600W power supplies. What's the earliest you can deliver?",
    lines: [
      { text: "W2 dual-GPU AI workstation", qty: 2, sku: "SYS-W2" },
      { text: "spare 1600W power supply", qty: 2, sku: "PSU-1600" },
    ],
  },
  {
    id: "ENQ-3103",
    received: "2026-11-16",
    company: "Copperleaf AI",
    contact: "Mei Lin",
    email: "mei@copperleaf-ai.example",
    deadline: "2026-12-10",
    body: "Looking for one H200 NVL 141GB for fine-tuning. We can wait up to three weeks. Best price please.",
    lines: [{ text: "H200 NVL 141GB GPU for fine-tuning", qty: 1, sku: "GPU-H200" }],
  },
  {
    id: "ENQ-3104",
    received: "2026-11-18",
    company: "Ashby Marsh LLP",
    contact: "Owen Price",
    email: "owen.price@ashbymarsh.example",
    deadline: "2026-11-28",
    body: "Our document server needs 8 more 64GB DDR5 ECC registered memory sticks, same as last time (6400). Can you do them this month?",
    lines: [{ text: "64GB DDR5 6400 ECC registered memory", qty: 8, sku: "MEM-6496" }],
  },
];

export const SHOP = {
  name: "Workstation",
  desk: "Workstation sourcing desk",
  sales: "Workstation sales",
  from: "sourcing@workstation.example",
  salesFrom: "sales@workstation.example",
  site: "https://www.workstation.co.uk/en",
};
