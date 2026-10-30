/**
 * Property Finder — "describe the home you want" semantic search with
 * hard numeric filters (max price, min bedrooms) in the same SQL, then a
 * chat that compares the shortlist. Listings are invented.
 */
import type { SeedDoc } from "../../rag/seed";
import type { UseCaseSpec } from "../UseCaseDemo";

const AREAS = ["Bristol", "Leeds", "Brighton", "Manchester"] as const;

function l(id: string, area: string, price_gbp: number, bedrooms: number, type: string, epc: string, body: string): SeedDoc {
  return {
    id,
    text: `${id} · ${bedrooms}-bed ${type} · ${area} · £${price_gbp.toLocaleString("en-GB")} · EPC ${epc}\n${body}`,
    metadata: {
      listing_id: id,
      area,
      price_gbp,
      bedrooms,
      type,
      epc,
      headline: `${bedrooms}-bed ${type}, ${area}`,
      price: `£${price_gbp.toLocaleString("en-GB")}`,
      epc_rating: `EPC ${epc}`,
    },
  };
}

const DOCS: SeedDoc[] = [
  l("LST-101", "Bristol", 465000, 3, "terrace", "C",
    "Victorian terrace in Bishopston, 6 minutes' walk to Montpelier station. South-facing garden with a studio shed set up as a home office. Original fireplaces, new boiler 2025."),
  l("LST-102", "Bristol", 395000, 2, "flat", "B",
    "Harbourside flat with balcony over the water, lift, secure bike store. Five minutes to Temple Meads. Service charge £2,100 per year."),
  l("LST-103", "Bristol", 610000, 4, "semi-detached", "D",
    "Family semi near outstanding-rated primary school, large garden, driveway with EV charger, needs kitchen update."),
  l("LST-201", "Leeds", 285000, 3, "semi-detached", "C",
    "Headingley semi near parks and the cricket ground, off-street parking, loft converted into a quiet office with skylights. Bus to city centre 15 minutes."),
  l("LST-202", "Leeds", 189000, 2, "flat", "B",
    "New-build flat in Holbeck, gym in building, walk to Leeds station in 8 minutes. Ideal first purchase; 999-year lease."),
  l("LST-203", "Leeds", 420000, 4, "detached", "A",
    "Eco detached home with heat pump, solar panels and battery, triple glazing. Near Roundhay Park. Very low running costs."),
  l("LST-301", "Brighton", 525000, 2, "flat", "C",
    "Regency seafront flat with bay window and sea view, high ceilings. No parking. Short walk to the station for London commute."),
  l("LST-302", "Brighton", 690000, 3, "terrace", "D",
    "Hanover terrace on a quiet street, dog-friendly walk to Queen's Park, garden room for working from home."),
  l("LST-401", "Manchester", 245000, 2, "flat", "B",
    "Ancoats warehouse conversion, exposed brick, 10 minutes' walk to Piccadilly. Co-working space downstairs."),
  l("LST-402", "Manchester", 365000, 3, "semi-detached", "C",
    "Chorlton semi with garden, near the tram stop and nature reserve, great for families with dogs. Dedicated study."),
  l("LST-403", "Manchester", 515000, 4, "detached", "B",
    "Didsbury detached with garage, heat pump installed 2024, near good secondary schools and the Mersey trail."),
];

export const propertySpec: UseCaseSpec = {
  bucket: "property_listings",
  blueprint: "property",
  seedVersion: "property-v1",
  docs: DOCS,
  noun: "listings",
  title: "Property Finder",
  subtitle: "Describe the home in plain English → semantic match AND hard price / bedroom / area filters in one SQL query → compare the shortlist",
  queryLabel: "Describe what you're looking for",
  placeholder: "Quiet street, room for an office, near a station…",
  scenarios: [
    { label: "WFH near station", q: "Somewhere with a proper home office space and a short walk to a train station", filters: { price_gbp: "500000", bedrooms: "2" } },
    { label: "Low energy bills", q: "Energy efficient home with heat pump or solar, cheap to run", filters: { bedrooms: "3" } },
    { label: "Family + dog", q: "Garden for a dog, close to parks and good schools", filters: { price_gbp: "450000", bedrooms: "3" } },
    { label: "First flat, Leeds", q: "First-time buyer, modern flat, walk to the station", filters: { area: "Leeds", price_gbp: "250000" } },
  ],
  filters: [
    { key: "area", label: "Area", kind: "select", options: AREAS },
    { key: "price_gbp", label: "Max price £", kind: "max", step: 5000 },
    { key: "bedrooms", label: "Min bedrooms", kind: "min" },
  ],
  idKey: "listing_id",
  titleKey: "headline",
  subKeys: ["price", "epc_rating"],
  resultsTitle: "Matching listings",
  chat: {
    title: "Property adviser",
    subtitle: "Compares only the shortlisted listings",
    system:
      "You help a home buyer compare a shortlist. Use ONLY the cited listings [n]; quote prices and features exactly. " +
      "Rank them against the buyer's stated needs, point out trade-offs and missing information worth asking the agent about. No mortgage or legal advice.",
    suggestions: ["Rank these against my brief.", "What should I ask the agent about each?", "Which is best value per bedroom?"],
    placeholder: "Ask about the shortlist…",
  },
};
