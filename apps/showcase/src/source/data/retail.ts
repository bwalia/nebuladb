/**
 * `ss_retail_inbox` — one demo day of the nationwide-retail loop from the
 * Source Supplies discovery call (a national retailer orders through its
 * facilities portal, Source keys it into Merlin, ships by courier, then
 * attaches TWO proofs of delivery back to the portal).
 *
 * DEMO DATA. "ValueMax Stores" and "ParcelLine" are invented; store
 * numbers, PO numbers, consignments and signatories are made up.
 *
 * Three document kinds, all in one bucket so NebulaDB can search across
 * the whole mailbox the way a person would:
 *   - portal_order    new work orders arriving today (to key into Merlin)
 *   - merlin_despatch Merlin delivery tickets for orders shipped on Wed
 *   - courier_pod     courier proof-of-delivery emails for those orders
 *
 * The messy cases are deliberate and fixed (see EXCEPTIONS below) so the
 * demo and its tests always tell the same story.
 */
import { B, rng, type SeedDoc } from "./common.ts";

export const SEED_VERSION = "ss-retail-v1";
export const BUCKET = B.retailInbox;
export const RULES_BUCKET = B.retailRules;

export const RETAILER = "ValueMax Stores";
export const COURIER = "ParcelLine";
/** Today on the demo calendar (a Friday). Despatches went out on Wednesday. */
export const RETAIL_DAY = "2026-10-30";
export const DESPATCH_DAY = "2026-10-28";

export interface Store {
  no: string;
  name: string;
  postcode: string;
}

const STORE_LIST: [string, string, string][] = [
  ["0112", "Camberley", "GU15 3SY"],
  ["0118", "Reading Broad St", "RG1 2BH"],
  ["0124", "Guildford", "GU1 3DE"],
  ["0131", "Slough", "SL1 1DQ"],
  ["0137", "Woking", "GU21 6GB"],
  ["0142", "Basingstoke", "RG21 7BE"],
  ["0156", "Croydon Whitgift", "CR0 1LP"],
  ["0163", "Kingston", "KT1 1JU"],
  ["0171", "Bristol Cribbs", "BS34 5DG"],
  ["0178", "Swindon", "SN1 1LF"],
  ["0185", "Oxford Westgate", "OX1 1PE"],
  ["0192", "Milton Keynes", "MK9 3ES"],
  ["0204", "Cambridge Grafton", "CB1 1PS"],
  ["0211", "Southampton", "SO15 1QD"],
  ["0219", "Portsmouth Gunwharf", "PO1 3TZ"],
  ["0226", "Brighton Churchill Sq", "BN1 2RG"],
  ["0233", "Stratford", "E20 1EJ"],
  ["0240", "Wembley", "HA9 0FD"],
  ["0247", "Bluewater", "DA9 9ST"],
  ["0254", "Watford", "WD17 2TB"],
  ["0301", "Manchester Arndale", "M4 3AQ"],
  ["0308", "Trafford", "M17 8AA"],
  ["0315", "Liverpool One", "L1 8JQ"],
  ["0322", "Sheffield Meadowhall", "S9 1EP"],
  ["0329", "Leeds White Rose", "LS11 8LU"],
  ["0336", "Bradford Broadway", "BD1 1US"],
  ["0343", "Stockport", "SK1 1RS"],
  ["0350", "Warrington", "WA1 2QQ"],
  ["0357", "Bolton", "BL1 1DF"],
  ["0364", "Preston", "PR1 2HH"],
];

export const STORES: Store[] = STORE_LIST.map(([no, town, postcode]) => ({
  no,
  name: `${RETAILER} ${town}`,
  postcode,
}));
export const STORE_BY_NO: Record<string, Store> = Object.fromEntries(STORES.map((s) => [s.no, s]));

/** The retailer's own item codes, as they appear on portal work orders. */
export const RETAILER_ITEMS: { code: string; desc: string; sku: string }[] = [
  { code: "VM-11873", desc: "Toilet tissue jumbo 2ply (case)", sku: "JR-400" },
  { code: "VM-11874", desc: "Hand towel C-fold white (case)", sku: "HT-300" },
  { code: "VM-11880", desc: "Refuse sacks heavy duty 120L", sku: "BG-625" },
  { code: "VM-11881", desc: "Bin liners black recycled 90L", sku: "BG-610" },
  { code: "VM-11890", desc: "Hand soap liquid 5L", sku: "HS-060" },
  { code: "VM-11902", desc: "Multi surface spray RTU", sku: "MP-050" },
  { code: "VM-11905", desc: "Floor cleaner RTU 5L", sku: "FC-010" },
  { code: "VM-11910", desc: "Nitrile gloves blue L (box)", sku: "GV-800" },
  { code: "VM-11915", desc: "Microfibre cloths colour coded", sku: "CL-915" },
  { code: "VM-11920", desc: "Washroom cleaner RTU", sku: "WR-020" },
];
const ITEM_BY_CODE = Object.fromEntries(RETAILER_ITEMS.map((i) => [i.code, i]));

/** Codes the portal sends that Source has never mapped: Jordan must find them. */
export const UNMAPPED_ITEMS: { code: string; desc: string; expectSku: string }[] = [
  { code: "VM-12044", desc: "Blue roll centrefeed 2ply x6", expectSku: "CR-350" },
  { code: "VM-12051", desc: "Wet floor warning sign yellow", expectSku: "WF-935" },
];

export interface PortalLine {
  code: string;
  desc: string;
  qty: number;
}

export interface PortalOrder {
  kind: "portal_order";
  wo: string;
  po: string;
  store_no: string;
  store: string;
  postcode: string;
  received: string;
  lines: PortalLine[];
}

export interface MerlinDespatch {
  kind: "merlin_despatch";
  so: string;
  po: string;
  store_no: string;
  store: string;
  postcode: string;
  consignment: string;
  despatched: string;
  lines: { sku: string; qty: number; owed: number }[];
  scan_pod: string;
}

export interface CourierPod {
  kind: "courier_pod";
  consignment: string;
  customer_ref: string;
  status: "Delivered" | "Failed" | "In transit";
  delivered_at: string;
  signed_by: string;
  photo: string;
  postcode: string;
  address: string;
  note: string;
}

export type InboxItem = PortalOrder | MerlinDespatch | CourierPod;

/**
 * Deliberate messy cases in Wednesday's 30 despatches (index → case).
 * Everything else is a clean straight-through match.
 */
export const EXCEPTIONS = {
  /** Courier email has no PO and the consignment digits are transposed. */
  fuzzy: 4,
  /** Merlin ticket marks one line owed (short-shipped). */
  owed: 9,
  /** Courier could not deliver. */
  failed: 13,
  /** Courier has a different postcode for the store (store relocated in the mall). */
  address: 24,
  /** Left in a safe place: photo, no signature. */
  safePlace: 18,
  /** Courier POD not back yet. */
  inTransit: 27,
} as const;

/** The alias a person confirms for the address case; learning it clears the exception. */
export const ADDRESS_ALIAS = { store_no: "0329", postcode: "LS11 8LW" };

const SIGNERS = ["J. Patel", "S. Okafor", "M. Byrne", "K. Nowak", "A. Hussain", "L. Evans", "R. Kaur", "D. Walsh", "T. Green", "C. Ahmed"];

function consignmentFor(i: number): string {
  return `PL${String(482_100_000 + i * 1379).padStart(9, "0")}`;
}

function swapTwo(s: string): string {
  const a = s.split("");
  [a[6], a[7]] = [a[7], a[6]];
  return a.join("");
}

function buildDay() {
  const rand = rng(20261030);
  const pick = <T,>(arr: T[]) => arr[Math.floor(rand() * arr.length)];

  const despatches: MerlinDespatch[] = [];
  const pods: CourierPod[] = [];
  STORES.forEach((st, i) => {
    const po = String(410_250_000 + i * 7919);
    const nLines = 1 + Math.floor(rand() * 3);
    const items = [...RETAILER_ITEMS].sort(() => rand() - 0.5).slice(0, nLines);
    const lines = items.map((it) => ({ sku: it.sku, qty: 1 + Math.floor(rand() * 4), owed: 0 }));
    if (i === EXCEPTIONS.owed) lines[0] = { ...lines[0], owed: 1 };
    const consignment = consignmentFor(i);
    const so = `SO-${88_400 + i}`;
    despatches.push({
      kind: "merlin_despatch",
      so,
      po,
      store_no: st.no,
      store: st.name,
      postcode: st.postcode,
      consignment,
      despatched: DESPATCH_DAY,
      lines,
      scan_pod: `merlin/pod/${so}.pdf`,
    });

    if (i === EXCEPTIONS.inTransit) return; // no courier email yet
    const hour = 8 + Math.floor(rand() * 9);
    const min = Math.floor(rand() * 60);
    const signer = pick(SIGNERS);
    let pod: CourierPod = {
      kind: "courier_pod",
      consignment,
      customer_ref: po,
      status: "Delivered",
      delivered_at: `2026-10-29T${String(hour).padStart(2, "0")}:${String(min).padStart(2, "0")}`,
      signed_by: signer,
      photo: `parcelline/photo/${consignment}.jpg`,
      postcode: st.postcode,
      address: `${st.name}, goods-in, ${st.postcode}`,
      note: "Delivered to goods-in.",
    };
    if (i === EXCEPTIONS.fuzzy) pod = { ...pod, consignment: swapTwo(consignment), customer_ref: "" };
    if (i === EXCEPTIONS.failed)
      pod = { ...pod, status: "Failed", signed_by: "", photo: "", note: "Store closed for refit — card left. Rebook required." };
    if (i === EXCEPTIONS.safePlace) pod = { ...pod, signed_by: "", note: "Left in safe place: rear service door, photo taken." };
    if (i === EXCEPTIONS.address)
      pod = { ...pod, postcode: ADDRESS_ALIAS.postcode, address: `${st.name} (Unit 4 service yard), ${ADDRESS_ALIAS.postcode}` };
    pods.push(pod);
  });

  // Today's 12 new portal work orders (to key into Merlin).
  const portal: PortalOrder[] = [];
  for (let k = 0; k < 12; k++) {
    const st = STORES[(k * 7 + 3) % STORES.length];
    const nLines = 1 + Math.floor(rand() * 3);
    const lines: PortalLine[] = [...RETAILER_ITEMS]
      .sort(() => rand() - 0.5)
      .slice(0, nLines)
      .map((it) => ({ code: it.code, desc: it.desc, qty: 1 + Math.floor(rand() * 5) }));
    if (k === 2) lines.push({ code: UNMAPPED_ITEMS[0].code, desc: UNMAPPED_ITEMS[0].desc, qty: 2 });
    if (k === 7) lines.push({ code: UNMAPPED_ITEMS[1].code, desc: UNMAPPED_ITEMS[1].desc, qty: 1 });
    portal.push({
      kind: "portal_order",
      wo: `WO-${7_731_200 + k * 17}`,
      po: String(410_400_000 + k * 6113),
      store_no: st.no,
      store: st.name,
      postcode: st.postcode,
      received: `${RETAIL_DAY}T0${7 + Math.floor(k / 4)}:${String((k * 13) % 60).padStart(2, "0")}`,
      lines,
    });
  }
  // The portal re-sends one order (a duplicate PO) — must not be keyed twice.
  portal.push({ ...portal[5], wo: `WO-${7_731_200 + 5 * 17}-R`, received: `${RETAIL_DAY}T10:41` });

  return { despatches, pods, portal };
}

export const DAY = buildDay();

export function itemByCode(code: string) {
  return ITEM_BY_CODE[code];
}

function portalText(o: PortalOrder): string {
  const lines = o.lines.map((l) => `${l.qty} x ${l.code} ${l.desc}`).join("; ");
  return `${RETAILER} portal work order ${o.wo}, PO ${o.po}, store ${o.store_no} ${o.store} ${o.postcode}. Lines: ${lines}.`;
}
function despatchText(d: MerlinDespatch): string {
  const lines = d.lines.map((l) => `${l.qty} x ${l.sku}${l.owed ? ` (${l.owed} owed)` : ""}`).join("; ");
  return `Merlin delivery ticket ${d.so} for PO ${d.po}, ${d.store} ${d.postcode}. ${COURIER} consignment ${d.consignment}, despatched ${d.despatched}. ${lines}. Scanned POD on file.`;
}
function podText(p: CourierPod): string {
  return (
    `${COURIER} proof of delivery, consignment ${p.consignment}, customer ref ${p.customer_ref || "(none)"}. ` +
    `Status ${p.status} ${p.delivered_at}. Signed by ${p.signed_by || "(no signature)"}. Address ${p.address}. ${p.note}`
  );
}

export const RETAIL_DOCS: SeedDoc[] = [
  ...DAY.despatches.map(({ lines, ...d }) => ({ id: `ss-rt-merlin-${d.so}`, text: despatchText({ ...d, lines }), metadata: { ...d, lines_json: JSON.stringify(lines) } })),
  ...DAY.pods.map((p) => ({ id: `ss-rt-pod-${p.consignment}`, text: podText(p), metadata: { ...p } })),
  ...DAY.portal.map(({ lines, ...o }) => ({ id: `ss-rt-portal-${o.wo}`, text: portalText({ ...o, lines }), metadata: { ...o, lines_json: JSON.stringify(lines) } })),
];
