/**
 * `ss_sds` — DEMO safety data sheets for ten made-up products, one doc
 * per section (16 sections each).
 *
 * Every word here is FICTIONAL. Nothing is copied from a real SDS, there
 * are no real hazard codes, exposure limits or transport numbers, and
 * none of these products carries a real brand name. Every section starts
 * with a "DEMO SDS" banner so nobody mistakes it for real safety advice.
 */
import { B, type SeedDoc } from "./common.ts";

export const SEED_VERSION = "ss-sds-v1";
export const BUCKET = B.sds;

export const DEMO_BANNER = "DEMO SDS — fictional product for a software demo, not real safety advice.";

export const SECTION_TITLES: Record<number, string> = {
  1: "Identification",
  2: "Hazards identification",
  3: "Composition and ingredients",
  4: "First-aid measures",
  5: "Fire-fighting measures",
  6: "Accidental release (spills)",
  7: "Handling and storage",
  8: "Exposure controls / PPE",
  9: "Physical and chemical properties",
  10: "Stability and reactivity",
  11: "Toxicological information",
  12: "Ecological information",
  13: "Disposal",
  14: "Transport",
  15: "Regulatory information",
  16: "Other information",
};

type HazardClass = "corrosive" | "irritant" | "not classified";

interface SdsProfile {
  sku: string;
  name: string;
  use: string;
  hazard_class: HazardClass;
  signal_word: "Danger" | "Warning" | "None";
  ppe: string[];
  hazards: string;
  ingredients: string;
  eyes: string;
  skin: string;
  swallowed: string;
  inhaled: string;
  storage: string;
  incompatible: string;
  spill: string;
  appearance: string;
  ph: string;
  ecology: string;
  disposal: string;
}

const RINSE_EYES =
  "Rinse the eye gently with clean running water for several minutes, holding the eyelid open. Remove contact lenses if easy to do. Get medical advice if irritation continues.";
const MILD_SKIN = "Wash skin with plenty of water. Take off splashed clothing. Get advice if irritation continues.";
const MILD_SWALLOW =
  "Rinse the mouth with water. Do not make the person sick. Give a small glass of water to sip. Get medical advice and show this sheet.";
const FRESH_AIR = "Move the person to fresh air and keep them comfortable. Get medical advice if they feel unwell.";

export const SDS_PROFILES: SdsProfile[] = [
  {
    sku: "DS-200",
    name: "DS-200 Heavy-Duty Descaler (demo)",
    use: "Removing heavy limescale from toilets, urinals and showers. Professional use only.",
    hazard_class: "corrosive",
    signal_word: "Danger",
    ppe: ["chemical-resistant gauntlets", "splash goggles", "apron"],
    hazards:
      "Demo classification: corrosive. Can cause serious burns to skin and serious eye damage. Contact with chlorine products can release a harmful gas.",
    ingredients: "Fictional blend: demo acid A (10–20%), demo wetting agent (under 5%), water. Not a real formulation.",
    eyes: "Rinse the eye at once with clean running water for at least 15 minutes, holding the eyelid open. Remove contact lenses if easy to do. Get medical help urgently.",
    skin: "Take off splashed clothing straight away. Rinse skin with plenty of running water for at least 15 minutes. Get medical help if the skin is burnt or blistered.",
    swallowed: "Rinse the mouth with water. Do not make the person sick. Get medical help urgently and show this sheet.",
    inhaled: FRESH_AIR,
    storage:
      "Store upright in the closed original container in a locked, cool, ventilated store. Keep away from chlorine bleach and alkaline products. Do not store above shoulder height.",
    incompatible:
      "Do not mix with bleach or any chlorine product: the mixture releases a harmful gas. Do not mix with alkaline cleaners or other descalers. Reacts with some metals; do not use on marble or limestone.",
    spill: "Ventilate the area. Contain with absorbent granules or sand. Do not wash undiluted product into drains. Neutralise small residues with plenty of water.",
    appearance: "Clear red liquid, sharp odour",
    ph: "about 1 (demo value)",
    ecology: "Demo note: dilute acid is neutralised in sewage treatment. Do not release undiluted product to surface water.",
    disposal: "Rinse empty containers three times and recycle. Dispose of unwanted concentrate through a licensed waste contractor.",
  },
  {
    sku: "SA-190",
    name: "SA-190 Hard Surface Sanitiser (demo)",
    use: "Sanitising touch points and hard surfaces. Ready to use.",
    hazard_class: "irritant",
    signal_word: "Warning",
    ppe: ["nitrile gloves"],
    hazards: "Demo classification: irritant. May irritate eyes. Prolonged skin contact may cause dryness.",
    ingredients: "Fictional blend: demo quaternary agent (under 1%), demo solvent (under 5%), water.",
    eyes: RINSE_EYES,
    skin: MILD_SKIN,
    swallowed: MILD_SWALLOW,
    inhaled: FRESH_AIR,
    storage: "Store in the closed original bottle below 30 °C, away from food and drink. Keep out of reach of children.",
    incompatible: "Do not mix with other cleaning products, especially bleach or anionic detergents. Mixing can stop it working and may release irritating vapour.",
    spill: "Wipe up with cloths and rinse the area with water.",
    appearance: "Clear colourless liquid, light odour",
    ph: "about 8 (demo value)",
    ecology: "Demo note: harmful to aquatic life in large amounts. Use the dose stated on the label.",
    disposal: "Empty bottles can be rinsed and recycled. Small amounts of diluted product can go to foul drain with plenty of water.",
  },
  {
    sku: "KD-130",
    name: "KD-130 Kitchen Degreaser (demo)",
    use: "Removing grease from extraction canopies, hobs and kitchen walls. Dilute before use.",
    hazard_class: "corrosive",
    signal_word: "Danger",
    ppe: ["chemical-resistant gauntlets", "splash goggles", "apron"],
    hazards: "Demo classification: corrosive as a concentrate. Can cause burns to skin and serious eye damage. Diluted solution is an irritant.",
    ingredients: "Fictional blend: demo alkali B (5–10%), demo surfactant (5–10%), water.",
    eyes: "Rinse the eye at once with clean running water for at least 15 minutes, holding the eyelid open. Get medical help urgently.",
    skin: "Take off splashed clothing. Rinse skin with plenty of running water for at least 15 minutes. Get medical help if the skin is burnt.",
    swallowed: "Rinse the mouth with water. Do not make the person sick. Get medical help urgently and show this sheet.",
    inhaled: FRESH_AIR,
    storage: "Store in the closed original container in a cool, ventilated chemical store. Keep away from acids and descalers. Keep away from food.",
    incompatible: "Do not mix with acids or descalers: the reaction gives off heat and may spatter. Do not use on aluminium or painted surfaces.",
    spill: "Contain with absorbent granules. Do not let concentrate enter drains. Rinse the area with water once picked up.",
    appearance: "Clear yellow liquid, faint odour",
    ph: "about 13 (demo value)",
    ecology: "Demo note: the surfactants are readily biodegradable. Do not release concentrate to watercourses.",
    disposal: "Rinse containers three times and recycle. Dispose of unwanted concentrate through a licensed waste contractor.",
  },
  {
    sku: "GL-140",
    name: "GL-140 Streak-Free Glass Concentrate (demo)",
    use: "Cleaning glass and mirrors after dilution in a trigger bottle.",
    hazard_class: "irritant",
    signal_word: "Warning",
    ppe: ["nitrile gloves", "safety glasses when decanting"],
    hazards: "Demo classification: irritant as a concentrate. May cause eye irritation. Diluted solution is not classified.",
    ingredients: "Fictional blend: demo alcohol (5–10%), demo surfactant (under 1%), water.",
    eyes: RINSE_EYES,
    skin: MILD_SKIN,
    swallowed: MILD_SWALLOW,
    inhaled: FRESH_AIR,
    storage: "Store in the closed original container away from heat and naked flames. Keep from freezing.",
    incompatible: "Do not mix with other products. Keep away from strong oxidisers.",
    spill: "Wipe up with cloths or absorbent and rinse with water.",
    appearance: "Clear blue liquid, light alcohol odour",
    ph: "about 7 (demo value)",
    ecology: "Demo note: readily biodegradable ingredients.",
    disposal: "Rinse and recycle containers. Diluted product can go to foul drain.",
  },
  {
    sku: "CP-150",
    name: "CP-150 Carpet Extraction Cleaner (demo)",
    use: "Low-foam cleaner for carpet extraction machines. Dilute before use.",
    hazard_class: "irritant",
    signal_word: "Warning",
    ppe: ["nitrile gloves", "safety glasses when decanting"],
    hazards: "Demo classification: irritant. May irritate eyes and skin as a concentrate.",
    ingredients: "Fictional blend: demo builder C (under 5%), demo surfactant (under 5%), water.",
    eyes: RINSE_EYES,
    skin: MILD_SKIN,
    swallowed: MILD_SWALLOW,
    inhaled: FRESH_AIR,
    storage: "Store in the closed original container between 5 and 30 °C.",
    incompatible: "Do not mix with acids, descalers or bleach. Do not use with foaming shampoos in the same tank.",
    spill: "Soak up with absorbent and rinse with water.",
    appearance: "Pale green liquid, mild odour",
    ph: "about 10 (demo value)",
    ecology: "Demo note: readily biodegradable surfactants.",
    disposal: "Extraction waste water can go to foul drain. Rinse and recycle containers.",
  },
  {
    sku: "MR-160",
    name: "MR-160 Mould and Mildew Remover (demo)",
    use: "Removing black mould from silicone, grout and painted walls. Ready to use.",
    hazard_class: "corrosive",
    signal_word: "Danger",
    ppe: ["chemical-resistant gauntlets", "splash goggles", "FFP2 mask in small rooms"],
    hazards:
      "Demo classification: corrosive. Contains a demo chlorine agent. Can cause burns and serious eye damage. Mixing with acids releases a harmful gas.",
    ingredients: "Fictional blend: demo chlorine agent D (under 5%), demo alkali (under 1%), water.",
    eyes: "Rinse the eye at once with clean running water for at least 15 minutes, holding the eyelid open. Get medical help urgently.",
    skin: "Take off splashed clothing. Rinse skin with plenty of running water. Get medical help if the skin is burnt.",
    swallowed: "Rinse the mouth with water. Do not make the person sick. Get medical help urgently and show this sheet.",
    inhaled: "Move the person to fresh air at once. Get medical help if they have trouble breathing.",
    storage: "Store upright in the original bottle out of direct sunlight, below 25 °C. Keep away from acids, descalers and ammonia products.",
    incompatible:
      "Never mix with acids, descalers, toilet cleaners or ammonia: the mixture releases a harmful gas. Use alone and rinse surfaces before using another product.",
    spill: "Ventilate the room. Soak up with absorbent and rinse with plenty of water.",
    appearance: "Pale yellow liquid, chlorine odour",
    ph: "about 12 (demo value)",
    ecology: "Demo note: toxic to aquatic life in concentrate. Use only the amount needed.",
    disposal: "Rinse and recycle bottles. Do not pour unused product into surface water drains.",
  },
  {
    sku: "GR-170",
    name: "GR-170 Gum Remover Gel (demo)",
    use: "Softening chewing gum on hard floors, paving and carpet so it can be scraped off.",
    hazard_class: "irritant",
    signal_word: "Warning",
    ppe: ["nitrile gloves", "safety glasses"],
    hazards: "Demo classification: irritant. May irritate skin and eyes. Vapour may cause drowsiness in small rooms.",
    ingredients: "Fictional blend: demo citrus solvent (30–50%), demo thickener (under 5%).",
    eyes: RINSE_EYES,
    skin: MILD_SKIN,
    swallowed: MILD_SWALLOW,
    inhaled: FRESH_AIR,
    storage: "Store in the closed original container away from heat, sparks and naked flames. Keep in a ventilated store.",
    incompatible: "Do not mix with other products. Keep away from strong oxidisers and naked flames.",
    spill: "Scrape up gel and place in a closed container. Wipe the area with detergent solution.",
    appearance: "Orange gel, citrus odour",
    ph: "not applicable (demo value)",
    ecology: "Demo note: do not let gel enter drains or watercourses.",
    disposal: "Dispose of scraped gum and gel with general waste in a closed bag. Recycle empty containers.",
  },
  {
    sku: "EN-180",
    name: "EN-180 Enzyme Odour Digester (demo)",
    use: "Digesting the organic matter that causes smells in drains, laundries, urinals and carpets.",
    hazard_class: "not classified",
    signal_word: "None",
    ppe: ["nitrile gloves"],
    hazards: "Demo classification: not classified. Enzymes may cause an allergic reaction in sensitive people if mist is breathed in.",
    ingredients: "Fictional blend: demo enzyme culture (under 1%), demo fragrance (under 1%), water.",
    eyes: RINSE_EYES,
    skin: MILD_SKIN,
    swallowed: MILD_SWALLOW,
    inhaled: FRESH_AIR,
    storage: "Store between 5 and 35 °C out of direct sunlight. Do not freeze. Use within 12 months of opening.",
    incompatible:
      "Do not mix with bleach, disinfectants or descalers: they kill the enzymes and stop the product working. Leave at least 2 hours after disinfecting before dosing.",
    spill: "Rinse away with water.",
    appearance: "Cloudy liquid, fresh odour",
    ph: "about 7 (demo value)",
    ecology: "Demo note: readily biodegradable.",
    disposal: "Diluted product can go to foul drain. Recycle containers.",
  },
  {
    sku: "FC-110",
    name: "EcoFloor Neutral Concentrate (demo SDS)",
    use: "Daily floor cleaning by mop or scrubber drier after dilution.",
    hazard_class: "not classified",
    signal_word: "None",
    ppe: ["nitrile gloves when handling concentrate"],
    hazards: "Demo classification: not classified. May cause mild eye irritation as a concentrate.",
    ingredients: "Fictional blend: demo plant-based surfactant (under 5%), water.",
    eyes: RINSE_EYES,
    skin: MILD_SKIN,
    swallowed: MILD_SWALLOW,
    inhaled: FRESH_AIR,
    storage: "Store in the closed original container between 5 and 30 °C.",
    incompatible: "Do not mix with other products. Mixing with descalers or bleach can stop it working.",
    spill: "Rinse away with water. Floors may be slippery: put out wet floor signs.",
    appearance: "Clear colourless liquid, very low odour",
    ph: "about 7 (demo value)",
    ecology: "Demo note: readily biodegradable.",
    disposal: "Diluted product can go to foul drain. Recycle containers.",
  },
  {
    sku: "WR-220",
    name: "EcoWash Washroom Concentrate (demo SDS)",
    use: "Daily washroom cleaning after dilution in a trigger bottle or bucket.",
    hazard_class: "irritant",
    signal_word: "Warning",
    ppe: ["nitrile gloves", "safety glasses when decanting"],
    hazards: "Demo classification: irritant as a concentrate. May cause eye irritation.",
    ingredients: "Fictional blend: demo mild acid (under 5%), demo surfactant (under 5%), water.",
    eyes: RINSE_EYES,
    skin: MILD_SKIN,
    swallowed: MILD_SWALLOW,
    inhaled: FRESH_AIR,
    storage: "Store in the closed original container away from bleach and alkaline products.",
    incompatible: "Do not mix with bleach or chlorine products. Do not mix with other cleaners.",
    spill: "Rinse away with plenty of water.",
    appearance: "Clear green liquid, light fresh odour",
    ph: "about 3 (demo value)",
    ecology: "Demo note: readily biodegradable.",
    disposal: "Diluted product can go to foul drain. Recycle containers.",
  },
];

function sectionBody(p: SdsProfile, n: number): string {
  switch (n) {
    case 1:
      return `Product: ${p.name}. Code ${p.sku}. Use: ${p.use} Supplier: demo supplier record. Emergency number: in this demo, call 999 in an emergency or NHS 111 for advice.`;
    case 2:
      return `${p.hazards} Signal word: ${p.signal_word}.`;
    case 3:
      return p.ingredients;
    case 4:
      return `If in eyes: ${p.eyes} If on skin: ${p.skin} If swallowed: ${p.swallowed} If breathed in: ${p.inhaled}`;
    case 5:
      return "The product is water based and does not burn easily. Use extinguishers suited to the surrounding fire. Containers may burst if heated.";
    case 6:
      return `${p.spill} Wear the PPE in section 8 while cleaning up.`;
    case 7:
      return `Handling: dilute as the label says, never above the stated strength. Decant in a ventilated area. Wash hands after use. Storage: ${p.storage}`;
    case 8:
      return `PPE: ${p.ppe.join(", ")}. Use in a ventilated area. No demo exposure limit is set for this fictional product.`;
    case 9:
      return `Appearance: ${p.appearance}. pH: ${p.ph}.`;
    case 10:
      return `Stable in normal storage. ${p.incompatible}`;
    case 11:
      return `Demo toxicology summary for a fictional product: effects as described in section 2 (${p.hazard_class}). No real test data.`;
    case 12:
      return p.ecology;
    case 13:
      return p.disposal;
    case 14:
      return p.hazard_class === "corrosive"
        ? "Demo transport note: treat as a regulated corrosive for transport. Real transport classification is not given in this demo."
        : "Demo transport note: not regulated for transport in this demo.";
    case 15:
      return "Demo regulatory note: a real product would list its regulatory status here. This fictional product has none.";
    default:
      return "Demo SDS written for a software demonstration. Not a real safety data sheet. Revision: demo 1.";
  }
}

export interface SdsMeta {
  sku: string;
  product_name: string;
  section: number;
  section_title: string;
  hazard_class: HazardClass;
  ppe: string[];
  signal_word: string;
}

export const SDS_DOCS: SeedDoc[] = SDS_PROFILES.flatMap((p) =>
  Array.from({ length: 16 }, (_, i) => {
    const n = i + 1;
    const meta: SdsMeta = {
      sku: p.sku,
      product_name: p.name,
      section: n,
      section_title: SECTION_TITLES[n],
      hazard_class: p.hazard_class,
      ppe: p.ppe,
      signal_word: p.signal_word,
    };
    return {
      id: `ss-sds-${p.sku}-s${String(n).padStart(2, "0")}`,
      text: `${DEMO_BANNER}\n${p.name} (${p.sku}) — Section ${n}: ${SECTION_TITLES[n]}.\n${sectionBody(p, n)}`,
      metadata: { ...meta },
    };
  })
);

export const SDS_SKUS = SDS_PROFILES.map((p) => p.sku);
