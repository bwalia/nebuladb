/**
 * `ss_products` — about 120 made-up product lines for the Source
 * Supplies demo. Names, SKUs, prices, stock and CO2e figures are all
 * invented. Real partner brands appear only as `brand_family` on paper
 * and dispenser lines, and those lines carry no eco badges, no swap
 * claims and no safety data.
 *
 * Prices are GBP excluding VAT. `uses_per_pack` is the unit cost-per-use
 * is worked out on: for chemicals it is litres of in-use solution
 * (a 1:100 concentrate in a 10 L pack makes 1,010 L), for everything
 * else it is the countable unit (rolls, sheets, gloves, items).
 */
import { B, type Badge, type Category, type SeedDoc, type Sector, BADGES, SECTORS } from "./common.ts";

export const SEED_VERSION = "ss-products-v1";
export const BUCKET = B.products;

export interface Product {
  sku: string;
  name: string;
  category: Category;
  use_areas: string[];
  sectors: Sector[];
  badges: Badge[];
  concentrate: boolean;
  dilution: string;
  pack: string;
  price_gbp: number;
  in_stock: boolean;
  stock_qty: number;
  lead_days: number;
  co2e_kg_per_unit: number;
  replaces_sku?: string;
  brand_family?: string;
  uses_per_pack: number;
  use_unit: string;
  summary: string;
}

const ALL: Sector[] = [...SECTORS];

interface Extra {
  replaces?: string;
  brand?: string;
}

/** Litres of in-use solution one pack makes: RTU = as supplied; 1:N = litres × (N + 1). */
export function inUseLitres(dilution: string, litres: number): number {
  const m = /^1:(\d+)$/.exec(dilution);
  return Math.round(litres * (m ? Number(m[1]) + 1 : 1) * 10) / 10;
}

function chem(
  sku: string,
  name: string,
  use_areas: string[],
  sectors: Sector[],
  badges: Badge[],
  dilution: string,
  pack: string,
  litres: number,
  price_gbp: number,
  stock_qty: number,
  lead_days: number,
  co2e: number,
  summary: string,
  x: Extra = {}
): Product {
  return {
    sku,
    name,
    category: "chemicals",
    use_areas,
    sectors,
    badges,
    concentrate: dilution !== "RTU",
    dilution: dilution === "RTU" ? "ready to use" : dilution,
    pack,
    price_gbp,
    in_stock: stock_qty > 0,
    stock_qty,
    lead_days,
    co2e_kg_per_unit: co2e,
    replaces_sku: x.replaces,
    uses_per_pack: inUseLitres(dilution, litres),
    use_unit: "litre in use",
    summary,
  };
}

function item(
  sku: string,
  name: string,
  category: Category,
  use_areas: string[],
  sectors: Sector[],
  badges: Badge[],
  pack: string,
  units: number,
  use_unit: string,
  price_gbp: number,
  stock_qty: number,
  lead_days: number,
  co2e: number,
  summary: string,
  x: Extra = {}
): Product {
  return {
    sku,
    name,
    category,
    use_areas,
    sectors,
    badges,
    concentrate: false,
    dilution: "n/a",
    pack,
    price_gbp,
    in_stock: stock_qty > 0,
    stock_qty,
    lead_days,
    co2e_kg_per_unit: co2e,
    replaces_sku: x.replaces,
    brand_family: x.brand,
    uses_per_pack: units,
    use_unit,
    summary,
  };
}

const TRIGGER = "6 × 750 ml";

export const PRODUCTS: Product[] = [
  // ---- Chemicals: floors -------------------------------------------------
  chem("FC-010", "Standard Floor Cleaner (ready to use)", ["floors"], ALL, [], "RTU", "2 × 5 L", 10, 14.9, 140, 1, 6.2,
    "General floor cleaner supplied ready to use. Strong pine scent. Heavy to carry and ship because it is mostly water."),
  chem("FC-110", "EcoFloor Neutral Concentrate", ["floors"], ALL, ["nordic-swan", "eu-ecolabel", "vegan", "made-in-britain"], "1:100", "2 × 5 L", 10, 26.5, 80, 1, 2.1,
    "Low-odour, pH-neutral floor cleaner for daily mopping and scrubber driers. Safe on vinyl, safety flooring and sealed wood. Low foam. Good for school canteens and corridors.",
    { replaces: "FC-010" }),
  chem("FC-115", "EcoFloor Neutral Concentrate (1 L dosing bottles)", ["floors"], ALL, ["nordic-swan", "eu-ecolabel", "vegan"], "1:100", "6 × 1 L", 6, 21.0, 0, 3, 1.6,
    "The same low-odour neutral floor cleaner in measured 1 L dosing bottles for small sites without a dosing pump."),
  chem("FC-120", "Citrus Floor Maintainer", ["floors"], ["education", "office", "leisure", "higher-education"], ["eu-ecolabel"], "1:80", "2 × 5 L", 10, 24.0, 60, 2, 2.6,
    "Daily floor maintainer with a strong fresh citrus fragrance. Leaves a light sheen on polished floors."),
  chem("FC-130", "Kitchen Floor Degreasing Cleaner", ["floors", "kitchens"], ["education", "care", "healthcare", "industrial", "leisure"], ["eu-ecolabel"], "1:50", "2 × 5 L", 10, 29.8, 45, 1, 2.9,
    "Cuts fat and grease from canteen and kitchen floors. Low odour. Rinse-free at normal dilution."),
  chem("FC-140", "Probiotic Floor and Grout Cleaner", ["floors", "washrooms"], ["education", "care", "office", "higher-education", "leisure"], ["ecocert", "vegan", "cruelty-free"], "1:40", "2 × 5 L", 10, 34.0, 30, 2, 2.4,
    "Probiotic cleaner that keeps breaking down dirt in grout lines and floor drains after mopping. Low odour."),
  chem("FC-150", "Scrubber Drier Low-Foam Cleaner", ["floors"], ["industrial", "leisure", "education", "office", "healthcare"], ["nordic-swan"], "1:200", "1 × 20 L", 20, 48.0, 22, 2, 3.8,
    "Very low foam cleaner made for scrubber drier tanks. Large sites, warehouses and sports halls."),
  chem("FP-160", "Low-Odour Floor Stripper", ["floors"], ALL, ["eu-ecolabel"], "1:10", "1 × 5 L", 5, 22.5, 35, 2, 2.0,
    "Removes old polish before a floor is re-sealed. Low odour, so it suits occupied buildings."),
  chem("FP-165", "Hard-Wearing Floor Polish", ["floors"], ["education", "office", "care", "healthcare"], [], "RTU", "1 × 5 L", 5, 27.0, 40, 2, 3.4,
    "Ready-to-use polish for vinyl and linoleum. Two to three coats after stripping."),
  chem("FP-170", "Sports Floor Restorer", ["floors", "sports floors"], ["leisure", "education", "higher-education"], ["made-in-britain"], "1:20", "1 × 5 L", 5, 39.0, 12, 4, 2.2,
    "Deep-cleans and restores worn acrylic and polyurethane sports courts when used with a scrubber drier and a soft pad. Restores grip."),

  // ---- Chemicals: washrooms ---------------------------------------------
  chem("WR-020", "Standard Washroom Cleaner (ready to use)", ["washrooms"], ALL, [], "RTU", TRIGGER, 4.5, 13.8, 200, 1, 3.9,
    "Ready-to-use washroom spray in single-use plastic trigger bottles."),
  chem("WR-220", "EcoWash Washroom Concentrate", ["washrooms"], ALL, ["nordic-swan", "eu-ecolabel", "vegan"], "1:50", "2 × 5 L", 10, 27.5, 90, 1, 1.9,
    "Daily washroom cleaner concentrate for basins, toilets, urinals and tiles. Refill your trigger bottles instead of buying new ones. Light fresh scent.",
    { replaces: "WR-020" }),
  chem("WR-225", "EcoWash Washroom Concentrate (dosing pouches)", ["washrooms"], ALL, ["nordic-swan", "vegan"], "1:50", "4 × 2 L pouches", 8, 25.0, 0, 5, 1.2,
    "Washroom concentrate in flat pouches for a wall dosing unit. Less plastic than drums."),
  chem("DS-200", "DS-200 Heavy-Duty Descaler (demo)", ["washrooms", "limescale"], ALL, [], "1:10", "2 × 5 L", 10, 31.0, 55, 1, 2.8,
    "Strong acid descaler for heavy limescale on toilets, urinals and showers. Demo product: see its demo safety data sheet before use."),
  chem("DS-210", "Gentle Daily Descaler", ["washrooms", "kitchens", "limescale"], ALL, ["eu-ecolabel", "vegan"], "1:20", "2 × 5 L", 10, 28.0, 70, 1, 2.0,
    "Mild daily descaler that stops limescale building up on taps and shower screens. Use the heavy-duty descaler only for heavy build-up.",
    { replaces: "DS-200" }),
  chem("UR-230", "Urinal Enzyme Cleaner", ["washrooms", "odour"], ["office", "education", "leisure", "industrial"], ["vegan"], "RTU", "1 × 5 L", 5, 19.5, 40, 2, 1.8,
    "Enzyme cleaner poured into urinal traps at the end of the day to stop smells and scale."),
  chem("SA-190", "SA-190 Hard Surface Sanitiser (demo)", ["washrooms", "kitchens"], ALL, [], "RTU", TRIGGER, 4.5, 16.2, 120, 1, 3.5,
    "Ready-to-use sanitiser spray for touch points. Demo product: see its demo safety data sheet before use."),
  chem("SA-195", "Food-Safe Sanitiser Concentrate", ["kitchens"], ["education", "care", "healthcare", "leisure", "industrial"], ["eu-ecolabel"], "1:100", "2 × 5 L", 10, 30.0, 50, 1, 2.2,
    "Concentrated sanitiser for food prep surfaces. Refill trigger bottles on site.",
    { replaces: "SA-190" }),
  chem("SA-197", "Chlorine-Free Disinfectant Cleaner", ["washrooms", "desks"], ["healthcare", "care"], ["vegan", "cruelty-free"], "1:40", "2 × 5 L", 10, 36.0, 25, 2, 2.3,
    "Chlorine-free cleaner and disinfectant for care and healthcare settings."),

  // ---- Chemicals: kitchens -----------------------------------------------
  chem("KD-130", "KD-130 Kitchen Degreaser (demo)", ["kitchens"], ALL, [], "1:20", "2 × 5 L", 10, 27.0, 60, 1, 3.1,
    "Heavy-duty degreaser for extraction canopies, hobs and kitchen walls. Demo product: see its demo safety data sheet before use."),
  chem("KD-135", "Plant-Based Kitchen Degreaser", ["kitchens"], ALL, ["ecocert", "vegan", "cruelty-free"], "1:20", "2 × 5 L", 10, 29.5, 45, 2, 1.9,
    "Plant-based degreaser for kitchen surfaces and floors. Low odour.",
    { replaces: "KD-130" }),
  chem("DW-240", "Hand Dishwash Liquid", ["kitchens"], ALL, ["eu-ecolabel", "vegan"], "1:200", "2 × 5 L", 10, 16.0, 150, 1, 2.0,
    "Concentrated washing-up liquid for sinks and pot wash."),
  chem("DW-245", "Machine Dishwash Detergent", ["kitchens"], ["education", "care", "healthcare", "leisure"], ["nordic-swan"], "1:250", "1 × 20 L", 20, 44.0, 18, 3, 4.4,
    "Detergent for commercial dishwashers and glasswashers. Use with a dosing pump."),
  chem("DW-250", "Machine Rinse Aid", ["kitchens"], ["education", "care", "healthcare", "leisure"], ["nordic-swan"], "1:500", "1 × 5 L", 5, 21.0, 26, 3, 1.1,
    "Rinse aid for commercial dishwashers. Spot-free drying."),
  chem("OV-255", "Oven and Grill Cleaner", ["kitchens"], ["education", "care", "leisure", "healthcare"], [], "RTU", TRIGGER, 4.5, 24.0, 33, 2, 2.7,
    "Ready-to-use gel for baked-on carbon in ovens and grills."),

  // ---- Chemicals: glass, desks, carpets ------------------------------------
  chem("GL-040", "Standard Glass Cleaner (ready to use)", ["glass"], ALL, [], "RTU", TRIGGER, 4.5, 12.5, 160, 1, 3.6,
    "Ready-to-use glass spray in single-use trigger bottles."),
  chem("GL-140", "GL-140 Streak-Free Glass Concentrate (demo)", ["glass"], ALL, ["nordic-swan", "vegan"], "1:40", "2 × 5 L", 10, 22.0, 75, 1, 1.4,
    "Streak-free glass and mirror cleaner concentrate. Refill trigger bottles on site.",
    { replaces: "GL-040" }),
  chem("MP-050", "Standard Multi-Surface Spray", ["desks"], ALL, [], "RTU", TRIGGER, 4.5, 12.0, 220, 1, 3.6,
    "Ready-to-use spray for desks, tables and touch points."),
  chem("MP-150", "EcoMulti Surface Concentrate", ["desks", "glass"], ALL, ["nordic-swan", "eu-ecolabel", "vegan"], "1:100", "2 × 5 L", 10, 24.5, 110, 1, 1.5,
    "One concentrate for desks, tables, glass and touch points. Refill your trigger bottles. Low odour.",
    { replaces: "MP-050" }),
  chem("MP-155", "EcoMulti Refill Pods", ["desks"], ALL, ["eu-ecolabel", "vegan", "planet-mark"], "1:150", "10 × 50 ml pods", 0.5, 18.0, 0, 6, 0.4,
    "Drop one pod into a 750 ml trigger bottle and top up with water. Almost no shipping weight."),
  chem("MP-165", "Antiviral Surface Cleaner", ["desks", "washrooms"], ["healthcare", "care", "education", "office"], [], "RTU", TRIGGER, 4.5, 19.8, 90, 1, 3.4,
    "Ready-to-use cleaner for touch points during outbreaks."),
  chem("CP-150", "CP-150 Carpet Extraction Cleaner (demo)", ["carpets"], ["office", "education", "higher-education", "care", "leisure"], ["eu-ecolabel"], "1:50", "1 × 5 L", 5, 23.0, 38, 2, 1.7,
    "Low-foam cleaner for carpet extraction machines. Demo product: see its demo safety data sheet before use."),
  chem("CP-155", "Carpet Spot and Stain Remover", ["carpets"], ALL, ["vegan"], "RTU", TRIGGER, 4.5, 21.0, 44, 2, 2.6,
    "Ready-to-use spotter for coffee, food and ink on carpets and upholstery."),
  chem("CP-160", "Encapsulation Carpet Shampoo", ["carpets"], ["office", "higher-education", "education"], ["nordic-swan"], "1:40", "1 × 5 L", 5, 26.0, 0, 4, 1.6,
    "Low-moisture carpet shampoo. Carpets are dry and back in use within an hour."),

  // ---- Chemicals: problem solvers -----------------------------------------
  chem("MR-160", "MR-160 Mould and Mildew Remover (demo)", ["mould", "washrooms", "walls"], ["higher-education", "care", "education", "healthcare", "office"], [], "RTU", TRIGGER, 4.5, 27.6, 48, 1, 3.0,
    "Ready-to-use spray for black mould on silicone, grout and painted walls. Demo product: see its demo safety data sheet before use."),
  chem("MR-165", "Peroxide Mould Treatment", ["mould", "walls"], ["higher-education", "care", "education"], ["eu-ecolabel", "vegan"], "RTU", TRIGGER, 4.5, 31.0, 20, 2, 2.4,
    "Chlorine-free mould treatment for student rooms and bathrooms. Lower odour than chlorine products.",
    { replaces: "MR-160" }),
  chem("MR-170", "Anti-Mould Protective Coating", ["mould", "walls"], ["higher-education", "care", "education"], ["made-in-britain"], "RTU", "1 × 2.5 L", 2.5, 34.0, 14, 3, 1.9,
    "Clear coating applied after mould removal to slow regrowth on painted walls and ceilings."),
  chem("GR-170", "GR-170 Gum Remover Gel (demo)", ["gum", "floors"], ["education", "leisure", "industrial", "office"], [], "RTU", "6 × 500 ml", 3, 29.0, 30, 2, 2.0,
    "Gel that softens chewing gum on hard floors, paving and carpets so it can be scraped off. Demo product: see its demo safety data sheet before use."),
  chem("GR-175", "Steam Gum Removal Solution", ["gum", "outdoor paving"], ["education", "leisure", "office"], ["vegan"], "1:10", "1 × 5 L", 5, 33.0, 16, 3, 1.4,
    "Solution for steam gum removal machines on playgrounds, paving and concourses."),
  chem("EN-180", "EN-180 Enzyme Odour Digester (demo)", ["odour", "washrooms", "laundry", "drains"], ALL, ["vegan", "cruelty-free", "ecocert"], "1:20", "2 × 5 L", 10, 35.0, 52, 1, 1.6,
    "Enzyme product that digests the organic matter causing smells in drains, laundries, urinals and carpets. Demo product: see its demo safety data sheet before use."),
  chem("EN-185", "Enzyme Drain Maintainer", ["drains", "odour"], ["care", "healthcare", "education", "leisure", "industrial"], ["vegan", "ecocert"], "RTU", "1 × 5 L", 5, 26.0, 28, 2, 1.5,
    "Poured into drains and gullies at night to stop odour coming back."),
  chem("EN-190", "Odour Neutraliser Spray", ["odour", "carpets"], ALL, ["vegan"], "RTU", TRIGGER, 4.5, 17.5, 64, 1, 2.5,
    "Ready-to-use spray that neutralises smells on soft furnishings and in bin stores."),
  chem("LD-260", "Low-Temperature Laundry Liquid", ["laundry"], ["care", "healthcare", "leisure"], ["eu-ecolabel"], "1:100", "1 × 20 L", 20, 52.0, 15, 3, 4.0,
    "Laundry liquid that cleans at 30–40 °C, saving energy in on-site laundries."),
  chem("LD-265", "Laundry Odour Booster", ["laundry", "odour"], ["care", "healthcare", "leisure"], ["vegan", "ecocert"], "1:100", "1 × 5 L", 5, 28.0, 21, 2, 1.2,
    "Enzyme booster added to the wash to remove stale smells from towels and bedding."),
  chem("HS-060", "Standard Liquid Hand Soap", ["hand hygiene"], ALL, [], "RTU", "1 × 5 L", 5, 11.0, 140, 1, 3.2,
    "Pink liquid hand soap for refillable dispensers."),
  chem("HS-270", "Foaming Hand Soap Cartridges", ["hand hygiene"], ALL, ["eu-ecolabel", "vegan", "cruelty-free"], "RTU", "6 × 1 L", 6, 30.0, 130, 1, 2.0,
    "Foam soap in sealed cartridges. Each push uses less soap than liquid soap.",
    { replaces: "HS-060" }),
  chem("HS-275", "Alcohol-Free Hand Sanitiser Foam", ["hand hygiene"], ["education", "care", "healthcare", "office"], ["vegan"], "RTU", "6 × 1 L", 6, 33.0, 80, 1, 2.1,
    "Alcohol-free foam sanitiser for schools and care settings."),

  // ---- Paper -------------------------------------------------------------
  item("JR-400", "Jumbo Toilet Roll 2-ply (virgin fibre)", "paper", ["washrooms"], ALL, [], "case of 6 rolls", 6, "roll", 24.0, 300, 1, 9.0,
    "Jumbo toilet rolls from virgin fibre for jumbo dispensers."),
  item("JR-410", "Recycled Jumbo Toilet Roll 2-ply", "paper", ["washrooms"], ALL, ["eu-ecolabel", "planet-mark"], "case of 6 rolls", 6, "roll", 23.5, 260, 1, 5.4,
    "Jumbo toilet rolls made from recycled fibre. Fits standard jumbo dispensers.", { replaces: "JR-400" }),
  item("TR-425", "Standard Toilet Roll 2-ply (virgin fibre)", "paper", ["washrooms"], ALL, [], "case of 36 rolls", 36, "roll", 27.0, 180, 1, 8.6,
    "Standard household-size toilet rolls."),
  item("TR-420", "Recycled Coreless Toilet Roll", "paper", ["washrooms"], ALL, ["eu-ecolabel", "made-in-britain"], "case of 36 rolls", 36, "roll", 31.0, 95, 2, 6.0,
    "Coreless recycled rolls that last longer per roll and leave no cardboard core waste.", { replaces: "TR-425" }),
  item("TR-430", "Mini Jumbo Toilet Roll", "paper", ["washrooms"], ALL, [], "case of 12 rolls", 12, "roll", 29.0, 120, 1, 7.2,
    "Mini jumbo rolls for compact dispensers.", { brand: "Katrin" }),
  item("TR-390", "Bulk Pack Toilet Tissue", "paper", ["washrooms"], ALL, [], "case of 36 packs", 36, "pack", 38.0, 60, 2, 7.9,
    "Interleaved bulk pack toilet tissue for bulk pack dispensers.", { brand: "Kimberly-Clark" }),
  item("HT-300", "C-Fold Hand Towels (virgin fibre, white)", "paper", ["washrooms"], ALL, [], "case of 2,400 sheets", 2400, "sheet", 19.0, 240, 1, 7.8,
    "White C-fold paper hand towels from virgin fibre."),
  item("HT-310", "Recycled C-Fold Hand Towels", "paper", ["washrooms"], ALL, ["eu-ecolabel", "fsc", "planet-mark"], "case of 2,400 sheets", 2400, "sheet", 18.5, 210, 1, 4.6,
    "Recycled C-fold hand towels. Same fold and size, so they fit the dispensers you already have.", { replaces: "HT-300" }),
  item("HT-345", "Recycled Z-Fold Hand Towels", "paper", ["washrooms"], ALL, ["eu-ecolabel", "planet-mark"], "case of 3,000 sheets", 3000, "sheet", 24.0, 0, 5, 4.9,
    "Recycled Z-fold towels that dispense one at a time to cut waste."),
  item("HT-320", "Z-Fold Hand Towels 2-ply", "paper", ["washrooms"], ALL, [], "case of 3,000 sheets", 3000, "sheet", 26.0, 140, 1, 6.9,
    "Soft 2-ply Z-fold towels for interfold dispensers.", { brand: "Lucart" }),
  item("HT-330", "Auto-Cut Hand Towel Roll", "paper", ["washrooms"], ALL, [], "case of 6 rolls", 6, "roll", 34.0, 90, 1, 8.4,
    "Hand towel rolls for automatic roll towel dispensers.", { brand: "Katrin" }),
  item("HT-340", "Hand Towel Roll (virgin fibre)", "paper", ["washrooms"], ALL, [], "case of 6 rolls", 6, "roll", 30.0, 100, 1, 8.0,
    "Standard hand towel rolls for roll dispensers."),
  item("HT-335", "Recycled Hand Towel Roll", "paper", ["washrooms"], ALL, ["eu-ecolabel", "fsc"], "case of 6 rolls", 6, "roll", 32.0, 70, 2, 5.6,
    "Recycled hand towel rolls for roll dispensers.", { replaces: "HT-340" }),
  item("CR-350", "Blue Centrefeed Roll 2-ply", "paper", ["kitchens", "workshops"], ALL, [], "case of 6 rolls", 6, "roll", 21.0, 150, 1, 6.5,
    "Blue centrefeed rolls for kitchens and workshops."),
  item("CR-355", "Recycled White Centrefeed Roll", "paper", ["kitchens", "workshops"], ALL, ["eu-ecolabel", "fsc", "made-in-britain"], "case of 6 rolls", 6, "roll", 22.0, 85, 1, 4.1,
    "Recycled centrefeed rolls. Same core size as the blue rolls.", { replaces: "CR-350" }),
  item("BR-360", "Industrial Wiper Roll", "paper", ["workshops"], ["industrial"], [], "case of 2 rolls", 2, "roll", 28.0, 40, 2, 7.0,
    "Strong wiper roll for oil and grease in workshops.", { brand: "Northwood" }),
  item("NP-370", "Napkins 2-ply 33 cm", "paper", ["kitchens"], ALL, ["fsc"], "case of 2,000", 2000, "napkin", 18.0, 70, 2, 3.0,
    "White 2-ply napkins for canteens and cafes."),
  item("NP-375", "Dispenser Napkins", "paper", ["kitchens"], ALL, [], "case of 6,000", 6000, "napkin", 39.0, 30, 2, 5.0,
    "Napkins for counter-top napkin dispensers.", { brand: "Lucart" }),
  item("FT-380", "Facial Tissues Cube Box", "paper", ["offices"], ALL, ["fsc", "pefc"], "case of 24 boxes", 24, "box", 22.0, 55, 2, 3.2,
    "Cube boxes of facial tissues for receptions and meeting rooms."),
  item("CO-385", "Couch Roll 2-ply", "paper", ["treatment rooms"], ["healthcare", "care"], ["pefc"], "case of 9 rolls", 9, "roll", 26.0, 40, 2, 5.8,
    "Couch roll for treatment couches and changing tables."),
  item("SC-395", "Toilet Seat Cover Refills", "paper", ["washrooms"], ALL, ["fsc"], "case of 20 packs", 20, "pack", 17.0, 25, 3, 1.5,
    "Paper seat covers for seat cover dispensers."),

  // ---- Washroom: dispensers, units and services ------------------------------
  item("DP-500", "Jumbo Toilet Roll Dispenser", "washroom", ["washrooms"], ALL, [], "1 dispenser", 1, "item", 32.0, 45, 2, 6.0,
    "Lockable dispenser for jumbo toilet rolls.", { brand: "Katrin" }),
  item("DP-505", "C-Fold Hand Towel Dispenser", "washroom", ["washrooms"], ALL, [], "1 dispenser", 1, "item", 27.0, 50, 2, 5.0,
    "Lockable dispenser for C-fold hand towels.", { brand: "Lucart" }),
  item("DP-510", "Automatic Roll Towel Dispenser", "washroom", ["washrooms"], ALL, [], "1 dispenser", 1, "item", 68.0, 18, 3, 9.5,
    "Battery automatic towel dispenser. One sheet at a time cuts waste.", { brand: "Katrin" }),
  item("DP-525", "Centrefeed Roll Dispenser", "washroom", ["kitchens", "workshops"], ALL, [], "1 dispenser", 1, "item", 24.0, 30, 2, 4.5,
    "Wall dispenser for centrefeed rolls.", { brand: "Northwood" }),
  item("DP-515", "Foam Soap Cartridge Dispenser", "washroom", ["washrooms", "hand hygiene"], ALL, [], "1 dispenser", 1, "item", 18.0, 70, 1, 3.2,
    "Manual dispenser for foam soap cartridges."),
  item("DP-520", "Recycled-Plastic Soap Dispenser", "washroom", ["washrooms", "hand hygiene"], ALL, ["made-in-britain", "planet-mark"], "1 dispenser", 1, "item", 21.0, 40, 3, 1.9,
    "Soap dispenser moulded from recycled plastic."),
  item("UR-530", "Enzyme Urinal Screens", "washroom", ["washrooms", "odour"], ["office", "education", "leisure", "industrial"], ["vegan"], "box of 10", 10, "screen", 22.0, 90, 1, 0.9,
    "Urinal screens with slow-release enzymes. Each lasts about 30 days."),
  item("UR-535", "Waterless Urinal Cartridge", "washroom", ["washrooms"], ["office", "education", "leisure", "industrial"], ["planet-mark"], "1 cartridge", 1, "item", 39.0, 22, 3, 1.4,
    "Cartridge for waterless urinals. Saves flush water."),
  item("AF-540", "Passive Air Care Unit Refill", "washroom", ["washrooms", "odour"], ALL, ["vegan"], "box of 6", 6, "refill", 14.0, 60, 1, 0.8,
    "Battery-free gel refill for passive air care units. Lasts about 60 days."),
  item("AF-545", "Metered Air Freshener Unit (aerosol-free)", "washroom", ["washrooms", "odour"], ALL, ["made-in-britain"], "1 unit", 1, "item", 45.0, 20, 4, 3.0,
    "Wall unit that releases fragrance on a timer without aerosols."),
  item("FH-560", "Sanitary Disposal Unit Exchange", "washroom", ["washrooms"], ALL, [], "per unit, per exchange", 1, "exchange", 4.5, 999, 0, 0.6,
    "Source Washroom service: a clean 23 L sanitary unit swapped on every visit."),
  item("NB-565", "Nappy Waste Unit Exchange", "washroom", ["washrooms"], ALL, [], "per unit, per exchange", 1, "exchange", 6.0, 999, 0, 0.8,
    "Source Washroom service: nappy waste unit for baby change rooms, swapped on every visit."),
  item("HD-575", "Standard Warm-Air Hand Dryer", "washroom", ["washrooms"], ALL, [], "1 dryer", 1, "item", 149.0, 12, 3, 55.0,
    "Warm-air hand dryer. Slow drying and higher energy use."),
  item("HD-570", "High-Speed Low-Energy Hand Dryer", "washroom", ["washrooms"], ALL, ["made-in-britain"], "1 dryer", 1, "item", 329.0, 8, 7, 42.0,
    "High-speed dryer that dries hands in about 10 seconds and uses far less power.", { replaces: "HD-575" }),
  item("SB-580", "Wall-Mounted Baby Changing Station", "washroom", ["washrooms"], ALL, [], "1 station", 1, "item", 189.0, 6, 10, 18.0,
    "Fold-down baby changing station."),

  // ---- Waste -------------------------------------------------------------
  item("BG-600", "Clear Bin Liners 90 L", "waste", ["bins"], ALL, [], "case of 200", 200, "liner", 16.0, 160, 1, 7.4,
    "Clear bin liners from virgin plastic."),
  item("BG-610", "Recycled Black Bin Liners 90 L", "waste", ["bins"], ALL, ["made-in-britain", "planet-mark"], "case of 200", 200, "liner", 15.5, 140, 1, 4.2,
    "Bin liners made from recycled plastic.", { replaces: "BG-600" }),
  item("BG-615", "Compostable Food Waste Liners 30 L", "waste", ["bins", "kitchens"], ALL, ["global-greentag"], "case of 500", 500, "liner", 28.0, 50, 2, 2.0,
    "Compostable liners for food waste caddies."),
  item("BG-620", "Recycled Pedal Bin Liners 15 L", "waste", ["bins", "washrooms"], ALL, ["made-in-britain"], "case of 1,000", 1000, "liner", 12.0, 90, 1, 2.1,
    "Small recycled liners for washroom and office pedal bins."),
  item("BG-625", "Heavy-Duty Refuse Sacks 120 L", "waste", ["bins"], ALL, [], "case of 100", 100, "sack", 19.5, 70, 1, 6.0,
    "Thick sacks for heavy and sharp waste."),
  item("RB-630", "Three-Way Recycling Station", "waste", ["bins"], ALL, ["made-in-britain"], "1 station", 1, "item", 145.0, 7, 6, 25.0,
    "Recycling station with three colour-coded openings."),
  item("RB-635", "Indoor Recycling Bin 60 L", "waste", ["bins"], ALL, ["planet-mark"], "1 bin", 1, "item", 38.0, 20, 4, 8.0,
    "Indoor bin with a recycling lid."),
  item("RB-640", "Sanitary Bin Liners", "waste", ["washrooms"], ALL, [], "case of 500", 500, "liner", 14.0, 60, 1, 1.8,
    "Liners for sanitary and nappy units."),
  item("CB-645", "Clinical Waste Bags 90 L (yellow)", "waste", ["bins"], ["healthcare", "care"], [], "case of 250", 250, "bag", 24.0, 45, 2, 6.2,
    "Yellow clinical waste bags."),
  item("SB-650", "Sharps Container 5 L", "waste", ["treatment rooms"], ["healthcare", "care"], [], "box of 20", 20, "container", 36.0, 30, 2, 5.0,
    "Sharps container with a temporary closure."),

  // ---- Machines ------------------------------------------------------------
  item("SD-700", "Compact Battery Scrubber Drier 43 cm", "machines", ["floors", "sports floors"], ["education", "office", "leisure", "healthcare", "care"], [], "1 machine", 1, "item", 3450.0, 4, 5, 180.0,
    "Walk-behind battery scrubber drier for corridors, canteens and sports halls. Leaves floors dry and safe to walk on in minutes."),
  item("SD-705", "Ride-On Scrubber Drier 65 cm", "machines", ["floors"], ["industrial", "leisure"], [], "1 machine", 1, "item", 8900.0, 1, 14, 410.0,
    "Ride-on scrubber drier for warehouses and large concourses."),
  item("VC-710", "Low-Noise Tub Vacuum 9 L", "machines", ["carpets", "floors"], ALL, [], "1 vacuum", 1, "item", 169.0, 25, 2, 28.0,
    "Quiet tub vacuum for daytime cleaning in offices and schools."),
  item("VC-715", "Battery Backpack Vacuum", "machines", ["carpets", "floors"], ["office", "leisure", "education"], [], "1 vacuum", 1, "item", 449.0, 9, 3, 35.0,
    "Cordless backpack vacuum for stairs, cinemas and stadium seating."),
  item("RM-720", "Rotary Floor Machine 17 inch", "machines", ["floors"], ALL, [], "1 machine", 1, "item", 899.0, 6, 4, 75.0,
    "Rotary machine for scrubbing, stripping and buffing hard floors."),
  item("CE-725", "Carpet Extractor 30 L", "machines", ["carpets"], ["office", "education", "higher-education", "care", "leisure"], [], "1 machine", 1, "item", 1190.0, 3, 6, 90.0,
    "Hot-water carpet extractor for deep cleaning carpets."),
  item("SG-730", "Steam Gum Removal Machine", "machines", ["gum", "outdoor paving"], ["education", "leisure", "office"], [], "1 machine", 1, "item", 2450.0, 2, 7, 120.0,
    "Steam machine that lifts chewing gum from paving, playgrounds and concourses without chemicals."),
  item("PW-735", "Cold-Water Pressure Washer", "machines", ["outdoor paving"], ALL, [], "1 machine", 1, "item", 699.0, 5, 3, 60.0,
    "Pressure washer for bin stores, paving and car parks."),

  // ---- PPE -----------------------------------------------------------------
  item("GV-800", "Nitrile Gloves (blue, powder-free)", "PPE", ["all areas"], ALL, [], "box of 100", 100, "glove", 5.6, 400, 1, 1.4,
    "General-purpose disposable nitrile gloves."),
  item("GV-805", "Biodegradable Nitrile Gloves", "PPE", ["all areas"], ALL, ["planet-mark"], "box of 100", 100, "glove", 7.2, 180, 1, 0.9,
    "Disposable nitrile gloves designed to break down faster in landfill.", { replaces: "GV-800" }),
  item("GV-810", "Chemical-Resistant Gauntlets (long cuff)", "PPE", ["all areas"], ALL, [], "12 pairs", 12, "pair", 28.0, 60, 1, 1.8,
    "Reusable long-cuff gauntlets for diluting and decanting chemicals."),
  item("GG-820", "Safety Goggles (indirect vent)", "PPE", ["all areas"], ALL, [], "pack of 10", 10, "item", 36.0, 40, 1, 1.5,
    "Splash goggles for decanting and using strong chemicals."),
  item("FS-825", "Full Face Shield", "PPE", ["all areas"], ALL, [], "pack of 5", 5, "item", 24.0, 25, 2, 1.2,
    "Face shield worn over goggles for heavy-duty jobs."),
  item("AP-830", "Disposable Aprons (colour-coded)", "PPE", ["all areas"], ALL, [], "roll of 200", 200, "apron", 9.5, 120, 1, 1.6,
    "Disposable plastic aprons in colour-coded rolls."),
  item("AP-835", "Compostable Aprons", "PPE", ["all areas"], ALL, ["global-greentag"], "roll of 200", 200, "apron", 13.0, 60, 2, 0.7,
    "Compostable aprons for care and food settings.", { replaces: "AP-830" }),
  item("MK-840", "FFP2 Disposable Masks", "PPE", ["all areas"], ALL, [], "box of 20", 20, "mask", 18.0, 50, 1, 0.6,
    "Disposable masks for dusty jobs and mould removal."),
  item("HV-845", "Hi-Vis Vests", "PPE", ["outdoor paving"], ALL, [], "pack of 10", 10, "item", 22.0, 35, 2, 2.0,
    "Hi-vis vests for outdoor and car park work."),
  item("FT-850", "Anti-Slip Overshoes", "PPE", ["floors"], ALL, [], "50 pairs", 50, "pair", 30.0, 20, 2, 1.0,
    "Overshoes with grip for wet floors and floor stripping."),

  // ---- Equipment -----------------------------------------------------------
  item("MO-900", "Microfibre Flat Mop Pads (colour-coded)", "equipment", ["floors"], ALL, ["made-in-britain"], "pack of 10", 10, "pad", 29.0, 85, 1, 1.9,
    "Washable flat mop pads. Use with a measured dose of concentrate to cut water and chemical use."),
  item("MO-905", "Cotton Kentucky Mop Heads", "equipment", ["floors"], ALL, [], "pack of 10", 10, "item", 34.0, 60, 1, 3.0,
    "Traditional cotton mop heads."),
  item("BU-910", "Colour-Coded Mop Bucket and Wringer 20 L", "equipment", ["floors"], ALL, [], "1 bucket", 1, "item", 44.0, 30, 2, 6.0,
    "Mop bucket with wringer and colour-coded handle."),
  item("CL-915", "Microfibre Cloths (colour-coded)", "equipment", ["desks", "washrooms", "glass", "kitchens"], ALL, ["made-in-britain"], "pack of 50", 50, "cloth", 22.0, 140, 1, 1.4,
    "Colour-coded microfibre cloths: red washrooms, blue general, green kitchens, yellow clinical."),
  item("CL-920", "Recycled Microfibre Cloths", "equipment", ["desks", "washrooms", "glass", "kitchens"], ALL, ["planet-mark", "made-in-britain"], "pack of 50", 50, "cloth", 24.0, 70, 2, 0.8,
    "Colour-coded cloths made from recycled bottles.", { replaces: "CL-915" }),
  item("TT-925", "Recycled-Plastic Cleaning Trolley", "equipment", ["all areas"], ALL, ["planet-mark"], "1 trolley", 1, "item", 189.0, 9, 5, 22.0,
    "Cleaning trolley with lockable chemical store."),
  item("WF-935", "Wet Floor Signs", "equipment", ["floors"], ALL, [], "pack of 5", 5, "item", 36.0, 40, 1, 2.5,
    "Yellow A-frame wet floor signs."),
  item("PD-940", "Floor Machine Pads (red, buffing)", "equipment", ["floors"], ALL, [], "pack of 5", 5, "pad", 18.0, 50, 1, 1.0,
    "Red pads for daily buffing and light scrubbing."),
  item("PD-945", "Floor Machine Pads (black, stripping)", "equipment", ["floors"], ALL, [], "pack of 5", 5, "pad", 20.0, 45, 1, 1.0,
    "Black pads for stripping old polish."),
  item("PD-950", "Diamond Restoration Pads", "equipment", ["floors", "sports floors"], ALL, [], "pack of 2", 2, "pad", 62.0, 10, 3, 0.8,
    "Diamond pads that restore dull stone, concrete and sports floors with water only."),
  item("TB-955", "Toilet Brush and Holder", "equipment", ["washrooms"], ALL, [], "pack of 6", 6, "item", 21.0, 50, 1, 2.4,
    "Closed toilet brush and holder."),
  item("DB-960", "Wall-Mounted Dosing Unit (4 products)", "equipment", ["all areas"], ALL, [], "1 unit", 1, "item", 249.0, 5, 7, 12.0,
    "Measures concentrate into bottles and buckets so staff never over-dose."),
];

export const PRODUCT_BY_SKU: Record<string, Product> = Object.fromEntries(PRODUCTS.map((p) => [p.sku, p]));

/** Cost per use (GBP per litre in use, per roll, per item). */
export function costPerUse(p: Pick<Product, "price_gbp" | "uses_per_pack">): number {
  return p.uses_per_pack > 0 ? p.price_gbp / p.uses_per_pack : p.price_gbp;
}

function productText(p: Product): string {
  const badges = p.badges.length ? p.badges.map((b) => BADGES[b]).join(", ") : "none";
  return [
    `${p.name} (${p.sku}). Category: ${p.category}.`,
    p.summary,
    `Use on: ${p.use_areas.join(", ")}. Sectors: ${p.sectors.join(", ")}.`,
    `Dilution: ${p.dilution}. Pack: ${p.pack}. Price £${p.price_gbp.toFixed(2)} per pack ex VAT.`,
    `Eco badges: ${badges}.`,
    p.replaces_sku ? `Greener swap for ${p.replaces_sku}.` : "",
    p.brand_family ? `Brand family: ${p.brand_family}.` : "",
    "Demo product record.",
  ]
    .filter(Boolean)
    .join(" ");
}

export const PRODUCT_DOCS: SeedDoc[] = PRODUCTS.map((p) => {
  const meta: Record<string, unknown> = { ...p };
  delete meta.summary;
  // Missing values never match in SQL, so leave optional keys off rather than null.
  if (!p.replaces_sku) delete meta.replaces_sku;
  if (!p.brand_family) delete meta.brand_family;
  return { id: `ss-prod-${p.sku}`, text: productText(p), metadata: meta };
});
