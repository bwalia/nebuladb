/**
 * `ss_guides` — made-up how-to and problem-solving article chunks, in
 * the spirit of a supplier's "Problem Solving" blog. Every SKU named in
 * `skus` exists in `ss_products`. Methods are generic cleaning practice
 * written for the demo; always follow the product label and its SDS.
 */
import { B, type SeedDoc, type Sector } from "./common.ts";

export const SEED_VERSION = "ss-guides-v1";
export const BUCKET = B.guides;

export const PROBLEMS = ["mould", "gum", "odour", "limescale", "floor-restore", "washroom", "kitchen", "carpet", "switch-green"] as const;
export type Problem = (typeof PROBLEMS)[number];

interface Chunk {
  heading: string;
  body: string;
  skus: string[];
}

interface Guide {
  id: string;
  title: string;
  problem: Problem;
  surface: string;
  sector: Sector;
  chunks: Chunk[];
}

export const GUIDES: Guide[] = [
  {
    id: "G-MOULD-01",
    title: "Black mould in student bedrooms",
    problem: "mould",
    surface: "painted walls",
    sector: "higher-education",
    chunks: [
      {
        heading: "Before you start",
        body: "Black mould in student rooms is almost always caused by condensation: drying clothes indoors, closed trickle vents and cold outside walls. Treat the mould and the cause, or it comes back within weeks. Open the window, take soft items out of the room and photograph the area for the accommodation team.",
        skus: ["MK-840", "GV-810"],
      },
      {
        heading: "Removing the mould",
        body: "1. Wear gauntlets, goggles and an FFP2 mask in small rooms. 2. Spray Peroxide Mould Treatment (MR-165) onto the mould and leave for 10 minutes. For stubborn patches on silicone use MR-160, and never use it in the same session as an acid descaler. 3. Wipe with a red microfibre cloth, rinse with clean water and dry. 4. Bag cloths after use.",
        skus: ["MR-165", "MR-160", "GV-810", "GG-820", "CL-915"],
      },
      {
        heading: "Stopping it coming back",
        body: "Once dry, roll on Anti-Mould Protective Coating (MR-170) on painted walls and ceilings. The room can be used again once surfaces are dry, usually about 2 hours after treatment; allow 24 hours after the coating before hanging posters or furniture against the wall. Ask the accommodation team to check trickle vents and extractor fans.",
        skus: ["MR-170"],
      },
    ],
  },
  {
    id: "G-MOULD-02",
    title: "Mould on shower silicone and grout",
    problem: "mould",
    surface: "silicone sealant",
    sector: "higher-education",
    chunks: [
      {
        heading: "Treating silicone and grout",
        body: "1. Ventilate the shower room. 2. Spray MR-160 Mould and Mildew Remover onto the silicone, leave 15 minutes and rinse well. 3. Scrub grout lines with a stiff brush. 4. If staining stays in the silicone after two treatments, the sealant needs replacing; cleaning cannot fix it.",
        skus: ["MR-160", "GV-810", "GG-820"],
      },
      {
        heading: "Weekly prevention",
        body: "Clean showers with EcoWash Washroom Concentrate (WR-220) and dry screens and silicone with a cloth. Probiotic Floor and Grout Cleaner (FC-140) on shower floors keeps working in grout between cleans.",
        skus: ["WR-220", "FC-140", "CL-915"],
      },
    ],
  },
  {
    id: "G-MOULD-03",
    title: "Mould in care home en-suites",
    problem: "mould",
    surface: "tiles",
    sector: "care",
    chunks: [
      {
        heading: "Working around residents",
        body: "Plan mould treatment while the resident is out of the room. Use the lower-odour Peroxide Mould Treatment (MR-165) rather than chlorine products. Leave the window open and the door shut during treatment.",
        skus: ["MR-165", "GV-810"],
      },
      {
        heading: "Method and re-entry",
        body: "1. Spray, leave 10 minutes, wipe with a red cloth. 2. Rinse and dry. 3. The en-suite can be used again once dry, usually within an hour. Record the treatment in the room log so the care team can see it.",
        skus: ["MR-165", "CL-915"],
      },
    ],
  },
  {
    id: "G-GUM-01",
    title: "Chewing gum on school playgrounds",
    problem: "gum",
    surface: "tarmac",
    sector: "education",
    chunks: [
      {
        heading: "Plan the job",
        body: "Remove gum from playgrounds in the holidays or after school when pupils are off site. Cone off the area and wear hi-vis. Count the gum spots first so you can report the before-and-after to the school.",
        skus: ["HV-845"],
      },
      {
        heading: "Steam method for large areas",
        body: "1. Use the Steam Gum Removal Machine (SG-730) with Steam Gum Removal Solution (GR-175) in the tank. 2. Hold the nozzle on each spot for 3–5 seconds until the gum lifts. 3. Brush loose residue away. 4. A trained operator covers roughly 300–400 spots an hour on tarmac. The area can be used as soon as it is dry, usually within 15 minutes.",
        skus: ["SG-730", "GR-175"],
      },
      {
        heading: "Small patches and prevention",
        body: "For a handful of spots, apply GR-170 Gum Remover Gel, leave 2 minutes and scrape. Add bins near the gates and sports areas, and power wash the playground each term with the Cold-Water Pressure Washer (PW-735).",
        skus: ["GR-170", "PW-735", "BG-610"],
      },
    ],
  },
  {
    id: "G-GUM-02",
    title: "Gum on airport and stadium concourses",
    problem: "gum",
    surface: "sealed concrete",
    sector: "leisure",
    chunks: [
      {
        heading: "Night-time removal",
        body: "Work in overnight shifts when the concourse is closed. Use the Steam Gum Removal Machine (SG-730) on fresh gum and GR-170 gel on old, flattened gum. Follow with a ride-on scrubber drier pass so the floor is dry before opening.",
        skus: ["SG-730", "GR-170", "SD-705", "FC-150"],
      },
      {
        heading: "Keeping on top of it",
        body: "Weekly spot-treatment is cheaper than a big clean twice a year. Log the zones with the most gum (usually near food outlets and exits) and put extra bins there.",
        skus: ["RB-635"],
      },
    ],
  },
  {
    id: "G-GUM-03",
    title: "Gum on office carpet",
    problem: "gum",
    surface: "carpet",
    sector: "office",
    chunks: [
      {
        heading: "Method",
        body: "1. Apply a small amount of GR-170 Gum Remover Gel to the gum. 2. Wait 2 minutes and lift with a blunt scraper, working from the edge. 3. Treat any stain with Carpet Spot and Stain Remover (CP-155). 4. Vacuum once dry. The area can be walked on straight away.",
        skus: ["GR-170", "CP-155", "VC-710"],
      },
    ],
  },
  {
    id: "G-ODOUR-01",
    title: "Smells in care-home laundries",
    problem: "odour",
    surface: "laundry drains",
    sector: "care",
    chunks: [
      {
        heading: "Find the source",
        body: "Laundry smells usually come from three places: floor drains and gullies with lint and fat in them, damp towels left in machines, and washing at too low a temperature without a booster. Check drains first; lift the gully cover and look for build-up.",
        skus: [],
      },
      {
        heading: "Enzyme treatment",
        body: "1. Clear lint from the drain by hand, wearing gloves. 2. Pour EN-180 Enzyme Odour Digester, diluted 1:20, into each drain and gully at the end of the day so it has the night to work. 3. Do not use bleach in the same drains for 2 hours either side, because it kills the enzymes. 4. Repeat nightly for a week, then weekly with Enzyme Drain Maintainer (EN-185).",
        skus: ["EN-180", "EN-185", "GV-800"],
      },
      {
        heading: "In the wash",
        body: "Add Laundry Odour Booster (LD-265) to towels and bedding and use Low-Temperature Laundry Liquid (LD-260) at 40 °C. Empty machines straight away and leave doors open to dry. Most homes see the smell go within 5–7 days.",
        skus: ["LD-265", "LD-260"],
      },
    ],
  },
  {
    id: "G-ODOUR-02",
    title: "Urinal odour in school washrooms",
    problem: "odour",
    surface: "urinals",
    sector: "education",
    chunks: [
      {
        heading: "Daily routine",
        body: "Clean urinals with EcoWash Washroom Concentrate (WR-220). Fit Enzyme Urinal Screens (UR-530) and change them every 30 days. Pour Urinal Enzyme Cleaner (UR-230) into traps at the end of the day.",
        skus: ["WR-220", "UR-530", "UR-230"],
      },
      {
        heading: "When smells persist",
        body: "Persistent smells mean scale in the traps and pipes. Descale with Gentle Daily Descaler (DS-210), or DS-200 for heavy scale with full PPE, then restart the enzyme routine after rinsing. Do not mix descaler with enzyme or bleach products.",
        skus: ["DS-210", "DS-200", "EN-180"],
      },
    ],
  },
  {
    id: "G-ODOUR-03",
    title: "Bin store smells",
    problem: "odour",
    surface: "bin store",
    sector: "industrial",
    chunks: [
      {
        heading: "Method",
        body: "1. Empty and pressure wash the bin store with the Cold-Water Pressure Washer (PW-735). 2. Spray EN-180 Enzyme Odour Digester, diluted, onto floors and bin bases. 3. Use recycled heavy liners (BG-610) so bags do not split. 4. Spray Odour Neutraliser (EN-190) on bin lids weekly.",
        skus: ["PW-735", "EN-180", "BG-610", "EN-190"],
      },
      {
        heading: "Keeping it fresh",
        body: "Wash bins monthly and keep lids shut. Put food waste in compostable liners (BG-615) so it can go to food waste collection rather than general waste.",
        skus: ["BG-615"],
      },
    ],
  },
  {
    id: "G-LIME-01",
    title: "Heavy limescale on toilets and urinals",
    problem: "limescale",
    surface: "ceramic",
    sector: "office",
    chunks: [
      {
        heading: "Heavy build-up",
        body: "1. Put on gauntlets, goggles and an apron. 2. Dilute DS-200 Heavy-Duty Descaler 1:10. 3. Apply under the rim and to scaled areas, leave 10 minutes and brush. 4. Flush twice and rinse. Never use DS-200 with bleach or mould remover: check its demo SDS section 10.",
        skus: ["DS-200", "GV-810", "GG-820", "AP-830", "TB-955"],
      },
      {
        heading: "Stop it coming back",
        body: "Once descaled, keep scale away with Gentle Daily Descaler (DS-210) twice a week. It is milder and needs only gloves.",
        skus: ["DS-210", "GV-800"],
      },
    ],
  },
  {
    id: "G-LIME-02",
    title: "Limescale on shower screens and taps",
    problem: "limescale",
    surface: "glass and chrome",
    sector: "leisure",
    chunks: [
      {
        heading: "Method",
        body: "Spray Gentle Daily Descaler (DS-210) diluted 1:20 onto screens and taps, leave 5 minutes, wipe and rinse. Finish glass with GL-140 Streak-Free Glass Concentrate and a dry microfibre cloth. Showers can be used straight after rinsing.",
        skus: ["DS-210", "GL-140", "CL-915"],
      },
    ],
  },
  {
    id: "G-FLOOR-01",
    title: "Restoring a worn acrylic sports court",
    problem: "floor-restore",
    surface: "sports floor",
    sector: "leisure",
    chunks: [
      {
        heading: "Check the floor first",
        body: "Worn acrylic sports courts look dull and lose grip because of built-up dirt and rubber marks, not because the coating has gone. Test a small area first. If the coating is worn through to the base, the court needs re-coating by a specialist, not cleaning.",
        skus: [],
      },
      {
        heading: "Deep clean with a scrubber drier",
        body: "1. Sweep or dust mop the court. 2. Fill the Compact Battery Scrubber Drier (SD-700) with Sports Floor Restorer (FP-170) at 1:20. 3. Scrub with a red pad (PD-940); use diamond restoration pads (PD-950) with water only on heavy marks. 4. Make two passes at right angles. 5. The court is dry and safe to play on in about 30 minutes.",
        skus: ["SD-700", "FP-170", "PD-940", "PD-950"],
      },
      {
        heading: "Keep the grip",
        body: "After restoration, clean daily with Scrubber Drier Low-Foam Cleaner (FC-150) at 1:200. Do not use polish or shine products on sports floors, as they make them slippery.",
        skus: ["FC-150", "SD-700"],
      },
    ],
  },
  {
    id: "G-FLOOR-02",
    title: "Floor restoration with a scrubber drier",
    problem: "floor-restore",
    surface: "vinyl",
    sector: "education",
    chunks: [
      {
        heading: "Strip",
        body: "1. Close the area and put out wet floor signs. 2. Apply Low-Odour Floor Stripper (FP-160) at 1:10 and leave 10 minutes. 3. Scrub with a black pad (PD-945) on a Rotary Floor Machine (RM-720). 4. Pick up slurry with a scrubber drier and rinse twice.",
        skus: ["FP-160", "PD-945", "RM-720", "WF-935", "FT-850"],
      },
      {
        heading: "Seal",
        body: "Apply two or three thin coats of Hard-Wearing Floor Polish (FP-165) with a flat mop, letting each coat dry for 30 minutes. Allow light foot traffic after 1 hour and full use after 24 hours.",
        skus: ["FP-165", "MO-900"],
      },
      {
        heading: "Maintain",
        body: "Clean daily with EcoFloor Neutral Concentrate (FC-110) in the scrubber drier at 1:100. A neutral cleaner protects the polish, so you strip less often.",
        skus: ["FC-110", "SD-700"],
      },
    ],
  },
  {
    id: "G-FLOOR-03",
    title: "Canteen floors after lunch service",
    problem: "floor-restore",
    surface: "safety flooring",
    sector: "education",
    chunks: [
      {
        heading: "Method",
        body: "1. Put out wet floor signs. 2. Sweep up food. 3. Mop with Kitchen Floor Degreasing Cleaner (FC-130) at 1:50 near serveries and EcoFloor Neutral Concentrate (FC-110) elsewhere. 4. Use flat microfibre mop pads (MO-900), which use less water so floors dry faster.",
        skus: ["FC-130", "FC-110", "MO-900", "WF-935"],
      },
      {
        heading: "Low odour for occupied schools",
        body: "Both cleaners are low odour, so canteens can be cleaned between sittings. Safety flooring should never be polished.",
        skus: ["FC-110", "FC-130"],
      },
    ],
  },
  {
    id: "G-WASH-01",
    title: "School washroom deep clean",
    problem: "washroom",
    surface: "washroom tiles",
    sector: "education",
    chunks: [
      {
        heading: "Method",
        body: "1. Restock paper and soap first. 2. Spray EcoWash Washroom Concentrate (WR-220) on basins, toilets and urinals and leave 2 minutes. 3. Brush toilets (TB-955). 4. Wipe with a red cloth, high to low. 5. Mop floors with Probiotic Floor and Grout Cleaner (FC-140).",
        skus: ["WR-220", "TB-955", "CL-915", "FC-140"],
      },
      {
        heading: "Termly jobs",
        body: "Descale taps and urinals with DS-210 each term. Check sanitary and nappy units are on the service schedule and that hand dryers work.",
        skus: ["DS-210", "FH-560"],
      },
    ],
  },
  {
    id: "G-WASH-02",
    title: "Keeping washrooms stocked across many sites",
    problem: "washroom",
    surface: "washroom",
    sector: "office",
    chunks: [
      {
        heading: "Standard kit per site",
        body: "Multi-site contract cleaners save time with one standard washroom kit: recycled jumbo toilet roll (JR-410), recycled C-fold hand towels (HT-310), foam soap cartridges (HS-270) and EcoWash concentrate (WR-220). Recycled lines fit the same dispensers, so a switch needs no refit.",
        skus: ["JR-410", "HT-310", "HS-270", "WR-220"],
      },
      {
        heading: "Fewer emergencies",
        body: "Order on the site's delivery day, using last month's usage as a guide, and keep one spare case per site. Foam soap cartridges (HS-270) in cartridge dispensers (DP-515) cut soap use per wash.",
        skus: ["HS-270", "DP-515"],
      },
    ],
  },
  {
    id: "G-KITCH-01",
    title: "Degreasing an extraction canopy",
    problem: "kitchen",
    surface: "stainless steel",
    sector: "care",
    chunks: [
      {
        heading: "Method",
        body: "1. Make sure the hob is cold and the extraction is off. 2. Wear gauntlets and goggles. 3. Apply KD-130 Kitchen Degreaser at 1:20, leave 5 minutes, wipe with a green cloth and rinse. 4. Never use KD-130 with descaler.",
        skus: ["KD-130", "GV-810", "GG-820", "CL-915"],
      },
      {
        heading: "Daily alternative",
        body: "For daily wipe-downs use Plant-Based Kitchen Degreaser (KD-135), which needs only gloves, and finish food surfaces with Food-Safe Sanitiser Concentrate (SA-195).",
        skus: ["KD-135", "SA-195", "GV-800"],
      },
    ],
  },
  {
    id: "G-CARPET-01",
    title: "Coffee and food stains on office carpet",
    problem: "carpet",
    surface: "carpet",
    sector: "office",
    chunks: [
      {
        heading: "Spot cleaning",
        body: "Blot fresh spills, then spray Carpet Spot and Stain Remover (CP-155), leave 2 minutes and blot from the outside in. Do not rub, as it spreads the stain.",
        skus: ["CP-155"],
      },
      {
        heading: "Deep clean",
        body: "Twice a year, extract with the Carpet Extractor (CE-725) and CP-150 at 1:50. Carpets are dry in 2–4 hours with good airflow. Encapsulation shampoo (CP-160) is an option where carpets must be back in use within an hour.",
        skus: ["CE-725", "CP-150", "CP-160"],
      },
    ],
  },
  {
    id: "G-SWITCH-01",
    title: "Switching a site to concentrates",
    problem: "switch-green",
    surface: "all surfaces",
    sector: "office",
    chunks: [
      {
        heading: "Why switch",
        body: "Ready-to-use sprays are mostly water. Shipping concentrates instead cuts delivery weight, plastic and CO2e. One 10 L pack of EcoMulti (MP-150) at 1:100 makes about 1,000 litres of in-use solution.",
        skus: ["MP-150", "MP-050"],
      },
      {
        heading: "How to switch",
        body: "1. Fit a wall dosing unit (DB-960) so staff always dose correctly. 2. Swap ready-to-use sprays for MP-150, WR-220 and GL-140 concentrates. 3. Swap the floor cleaner for FC-110. 4. Re-use trigger bottles and label them by colour. 5. Train staff in a 20-minute toolbox talk.",
        skus: ["DB-960", "MP-150", "WR-220", "GL-140", "FC-110", "CL-920"],
      },
    ],
  },
];

export const GUIDE_DOCS: SeedDoc[] = GUIDES.flatMap((g) =>
  g.chunks.map((c, i) => ({
    id: `ss-guide-${g.id}-${i + 1}`,
    text: `${g.title} — ${c.heading}. ${c.body}`,
    metadata: {
      guide_id: g.id,
      title: g.title,
      heading: c.heading,
      chunk: i + 1,
      problem: g.problem,
      surface: g.surface,
      sector: g.sector,
      skus: c.skus,
    },
  }))
);

export const SURFACES = [...new Set(GUIDES.map((g) => g.surface))].sort();
