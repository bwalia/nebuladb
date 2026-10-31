/**
 * `ss_tender_kb` — capability statements and past tender answers for
 * the Tender Responder and the washroom quote helper.
 *
 * Capability statements describe things Source Supplies say publicly
 * (B Corp, ISO 9001/14001/45001, electric vans inside the M25, a
 * solar-powered site, SDS on every product, COSHH support). Any number
 * here that is not on their public site is invented and marked
 * "(demo figure)". Past answers are invented; buyers are described, not
 * named.
 */
import { B, type SeedDoc } from "./common.ts";

export const SEED_VERSION = "ss-tender-v1";
export const BUCKET = B.tender;

export const TOPICS = [
  "sustainability",
  "health-safety",
  "quality",
  "delivery",
  "social-value",
  "pricing",
  "training",
  "washroom",
] as const;
export type Topic = (typeof TOPICS)[number];

export const TENDER_SECTORS = ["education", "higher-education", "healthcare", "public", "care", "office"] as const;
export type TenderSector = (typeof TENDER_SECTORS)[number];

export const TENDER_SECTOR_LABEL: Record<TenderSector, string> = {
  education: "Schools and academy trusts",
  "higher-education": "Colleges and universities",
  healthcare: "NHS and healthcare",
  public: "Councils and public sector",
  care: "Care groups",
  office: "Offices and FM",
};

interface Capability {
  id: string;
  topic: Topic;
  cert?: string;
  title: string;
  text: string;
}

const CAPABILITIES: Capability[] = [
  // sustainability
  { id: "CAP-SUS-01", topic: "sustainability", cert: "B Corp", title: "B Corp certified", text: "Source Supplies is a certified B Corp. Our social and environmental performance is independently assessed and we report on it." },
  { id: "CAP-SUS-02", topic: "sustainability", cert: "ISO 14001", title: "Environmental management", text: "We hold ISO 14001 for environmental management. Objectives cover fuel, energy, packaging and waste, and are reviewed by management every year." },
  { id: "CAP-SUS-03", topic: "sustainability", title: "Electric delivery vans", text: "Most of our deliveries inside the M25 are made with electric vans. Our own fleet covers London and the Home Counties every day." },
  { id: "CAP-SUS-04", topic: "sustainability", title: "Solar-powered site", text: "Our Camberley site is solar-powered, which cuts the carbon footprint of picking and packing your order." },
  { id: "CAP-SUS-05", topic: "sustainability", title: "GPS-tracked, planned routes", text: "Every van is GPS-tracked and routes are planned to cut miles and idling. Customers can be told when their delivery is close." },
  { id: "CAP-SUS-06", topic: "sustainability", title: "Greener product switching", text: "Account managers review each customer's order history and suggest greener swaps: concentrates instead of ready-to-use sprays, recycled paper instead of virgin fibre. Each swap shows the CO2e and cost change per year (demo figures in this demo)." },
  { id: "CAP-SUS-07", topic: "sustainability", title: "Concentrates and refills", text: "Concentrates and refill pods cut the water and plastic we ship. A 10 L concentrate at 1:100 replaces around 1,000 litres of ready-to-use product (demo figure)." },
  { id: "CAP-SUS-08", topic: "sustainability", title: "Eco-certified range", text: "Our range carries recognised eco badges including Nordic Swan, EU Ecolabel, ECOCERT, FSC, PEFC, Cruelty Free International and the Vegan Society. Buyers can filter the shop by badge." },
  { id: "CAP-SUS-09", topic: "sustainability", title: "Carbon reporting per account", text: "We can provide a CO2e summary per account and per site, based on the products ordered (demo feature: figures in this demo are invented)." },
  // health and safety
  { id: "CAP-HS-01", topic: "health-safety", cert: "ISO 45001", title: "Occupational health and safety", text: "We hold ISO 45001 for occupational health and safety management, covering our warehouse, drivers and site visits." },
  { id: "CAP-HS-02", topic: "health-safety", title: "Safety data sheet on every product", text: "A safety data sheet is available on every product page and is sent with first orders of any hazardous product." },
  { id: "CAP-HS-03", topic: "health-safety", title: "On-site COSHH support", text: "Our team helps customers put COSHH assessments in place on site, matched to the products they use." },
  { id: "CAP-HS-04", topic: "health-safety", title: "COSHH training videos", text: "We provide COSHH training videos that cleaning teams can watch on a phone before starting a job." },
  { id: "CAP-HS-05", topic: "health-safety", title: "Dosing and colour-coding", text: "Wall dosing units and colour-coded cloths, mops and bottles reduce over-dosing and cross-contamination." },
  // quality
  { id: "CAP-QA-01", topic: "quality", cert: "ISO 9001", title: "Quality management", text: "We hold ISO 9001 for quality management. Orders, deliveries and complaints follow documented processes that are audited each year." },
  { id: "CAP-QA-02", topic: "quality", title: "Named account manager", text: "Every trade account has a named account manager who reviews usage, pricing and service with the customer." },
  { id: "CAP-QA-03", topic: "quality", title: "Complaints handling", text: "Complaints are logged, acknowledged within one working day (demo figure) and tracked to closure with a root-cause note." },
  { id: "CAP-QA-04", topic: "quality", title: "Product trials", text: "We offer product trials and samples so a site can test a product before switching." },
  // delivery
  { id: "CAP-DEL-01", topic: "delivery", title: "Own fleet, daily", text: "Our own vans deliver across London and the Home Counties every day, with nationwide delivery for the rest of the UK." },
  { id: "CAP-DEL-02", topic: "delivery", title: "Next-day and scheduled delivery", text: "We offer next-day delivery and scheduled deliveries on a fixed day for each site, so stock arrives when cleaning teams expect it." },
  { id: "CAP-DEL-03", topic: "delivery", title: "Many ways to order", text: "Customers can order through an online trade account, the Quick Order page, email, phone, purchase orders or PDF orders." },
  { id: "CAP-DEL-04", topic: "delivery", title: "Multi-site ordering", text: "Multi-site customers can order for each site from one account, with site-level delivery days and site-level reporting." },
  { id: "CAP-DEL-05", topic: "delivery", title: "On-time delivery", text: "We track on-time delivery for every drop. In this demo the on-time figure is 98% (demo figure)." },
  // social value
  { id: "CAP-SV-01", topic: "social-value", title: "Local employment", text: "We recruit locally around our Surrey site and invest in staff training and progression." },
  { id: "CAP-SV-02", topic: "social-value", title: "Community support", text: "Our team supports local community and school projects through volunteering and donated products (demo detail)." },
  { id: "CAP-SV-03", topic: "social-value", title: "Responsible supply chain", text: "We favour suppliers with credible environmental and ethical standards and prefer UK-made products where they perform as well." },
  // pricing
  { id: "CAP-PR-01", topic: "pricing", title: "Contract pricing", text: "Trade accounts get contract pricing agreed with their account manager. Prices are quoted excluding VAT." },
  { id: "CAP-PR-02", topic: "pricing", title: "Cost in use", text: "We price on cost in use, not cost per pack: a concentrate often costs more per pack but far less per litre in use." },
  // training
  { id: "CAP-TR-01", topic: "training", title: "Product training", text: "We train cleaning teams on site in how to dose, use and store the products they buy, in short toolbox talks." },
  { id: "CAP-TR-02", topic: "training", title: "Machine training", text: "Customers buying scrubber driers and other machines get operator training at handover." },
  { id: "CAP-TR-03", topic: "training", title: "COSHH video library", text: "Our COSHH video library supports induction of new cleaning staff across many sites." },
  // washroom
  { id: "CAP-WR-01", topic: "washroom", title: "Source Washroom services", text: "Source Washroom provides feminine hygiene units, nappy waste units, air care, hand dryers and consumable refills. Visits are planned on a job-scheduling platform, and our promise is simple: we always turn up." },
  { id: "CAP-WR-02", topic: "washroom", title: "Specifying sanitary units", text: "Rule of thumb: one sanitary unit in every female and accessible cubicle, and one in each staff female toilet. Busy sites with more than 40 female staff or pupils per washroom move from a 28-day to a 14-day visit cycle (demo rule)." },
  { id: "CAP-WR-03", topic: "washroom", title: "Specifying nappy and baby change", text: "Rule of thumb: one nappy waste unit in every baby change room, exchanged on the same visit cycle as sanitary units (demo rule)." },
  { id: "CAP-WR-04", topic: "washroom", title: "Air care and dryers", text: "Rule of thumb: one passive air care unit per washroom, and a high-speed low-energy hand dryer for every two basins where paper towels are not used (demo rule)." },
  { id: "CAP-WR-05", topic: "washroom", title: "Duty of care", text: "Sanitary and nappy waste is collected under duty of care, with waste transfer notes kept for every site." },
];

const BUYERS: Record<Exclude<TenderSector, "care" | "office">, string> = {
  education: "a multi-academy trust in Surrey",
  "higher-education": "a university in London",
  healthcare: "an NHS trust in the South East",
  public: "a borough council in Berkshire",
};

const PAST: Array<[Topic, string, (buyer: string) => string]> = [
  ["sustainability", "Carbon reduction plan", (b) => `Past answer for ${b}: we described our B Corp certification, ISO 14001, electric vans for deliveries inside the M25 and our solar-powered site, and committed to a quarterly CO2e report per site. We proposed swapping ready-to-use sprays for concentrates in year one.`],
  ["health-safety", "Health and safety and COSHH", (b) => `Past answer for ${b}: we set out our ISO 45001 system, the safety data sheet on every product page, on-site COSHH assessment support and COSHH training videos for new cleaning staff.`],
  ["quality", "Quality assurance", (b) => `Past answer for ${b}: we described ISO 9001, a named account manager, quarterly service reviews and our complaints process with root-cause notes.`],
  ["delivery", "Delivery and service levels", (b) => `Past answer for ${b}: we offered a fixed delivery day per site from our own fleet, next-day delivery for urgent orders, and ordering by trade account, Quick Order, email, phone or purchase order.`],
  ["social-value", "Social value", (b) => `Past answer for ${b}: we described local recruitment around our Surrey site, staff training, and volunteering and donated products for community projects (demo detail).`],
  ["training", "Product and COSHH training", (b) => `Past answer for ${b}: we offered on-site toolbox talks on dosing and storage, machine training at handover and our COSHH video library for inductions.`],
];

interface KbEntry {
  id: string;
  kind: "capability" | "past-answer";
  topic: Topic;
  cert?: string;
  sector: string;
  won?: boolean;
  title: string;
  text: string;
}

const PAST_ENTRIES: KbEntry[] = (Object.keys(BUYERS) as Array<keyof typeof BUYERS>).flatMap((sector, si) =>
  PAST.map(([topic, title, fn], ti) => ({
    id: `PAST-${sector.toUpperCase().replace("-", "")}-${ti + 1}`,
    kind: "past-answer" as const,
    topic,
    sector,
    // Most past answers were part of a winning bid; a few were not.
    won: (si + ti) % 4 !== 3,
    title,
    text: fn(BUYERS[sector]),
  }))
);

export const KB: KbEntry[] = [
  ...CAPABILITIES.map((c) => ({ ...c, kind: "capability" as const, sector: "all" })),
  ...PAST_ENTRIES,
];

export const TENDER_DOCS: SeedDoc[] = KB.map((k) => {
  const meta: Record<string, unknown> = { kb_id: k.id, kind: k.kind, topic: k.topic, sector: k.sector, title: k.title };
  if (k.cert) meta.cert = k.cert;
  if (k.won !== undefined) meta.won = k.won;
  return {
    id: `ss-kb-${k.id}`,
    text: `${k.title} (${k.kind}, ${k.topic}${k.cert ? `, ${k.cert}` : ""}). ${k.text}`,
    metadata: meta,
  };
});
