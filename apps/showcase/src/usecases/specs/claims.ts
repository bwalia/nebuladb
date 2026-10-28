/**
 * Claims Assist — first-notice-of-loss narrative → policy wording plus
 * similar settled claims, filtered by line of business → grounded
 * coverage view for the handler. Fictional insurer and claims.
 */
import type { SeedDoc } from "../../rag/seed";
import type { UseCaseSpec } from "../UseCaseDemo";

const LINES = ["home", "motor", "travel"] as const;
const KINDS = ["wording", "settled-claim"] as const;

function doc(id: string, line: string, kind: string, title: string, body: string): SeedDoc {
  return {
    id,
    text: `${id} · ${line} · ${kind} · ${title}\n${body}`,
    metadata: { doc_id: id, line, kind, title },
  };
}

const DOCS: SeedDoc[] = [
  doc("HW-4.1", "home", "wording", "Escape of water",
    "We cover damage caused by water escaping from a fixed water or heating installation. We do not cover the cost of repairing the item the water escaped from, damage from gradual leaks the policyholder knew about, or the first £350 (escape-of-water excess). Trace and access costs are covered up to £5,000."),
  doc("HW-4.3", "home", "wording", "Storm",
    "Storm means winds of at least 55mph, heavy rain of 25mm per hour or snow of 30cm in 24 hours. We do not cover damage to fences, gates or hedges, or damage caused by wear and tear made worse by a storm."),
  doc("HW-6.2", "home", "wording", "Accidental damage (optional)",
    "If accidental damage cover is shown on the schedule, we cover sudden, unexpected physical damage to buildings or contents. Excludes damage by pets, chewing, scratching, and damage while the home is let."),
  doc("HW-9.1", "home", "wording", "Unoccupancy",
    "If the home is unoccupied for more than 60 consecutive days, cover for escape of water, theft and malicious damage is suspended until the home is occupied again."),
  doc("MW-2.1", "motor", "wording", "Windscreen",
    "Windscreen and window glass damage is covered with a £90 excess when replaced, or £10 when repaired, using our approved supplier. Claims for glass only do not affect the no-claims discount."),
  doc("MW-3.4", "motor", "wording", "Theft and keys",
    "We do not cover theft if the keys or keyless entry device were left in or on the car, or the car was left unlocked. Replacement of lost keys is covered up to £1,000."),
  doc("MW-5.0", "motor", "wording", "Use of vehicle",
    "Cover applies to social, domestic and pleasure use and commuting. Business use and delivering goods for payment are excluded unless shown on the certificate."),
  doc("TW-2.2", "travel", "wording", "Cancellation",
    "We cover irrecoverable costs if the trip is cancelled because of the illness of the insured, a close relative or travel companion, supported by a doctor's certificate. Pre-existing conditions must have been declared."),
  doc("TW-3.1", "travel", "wording", "Delayed baggage",
    "If baggage is delayed by more than 12 hours on the outward journey, we pay £100 for essential purchases, with receipts and a property irregularity report from the airline."),
  doc("CLM-2208", "home", "settled-claim", "Leaking shower tray — partially paid",
    "Water damage to kitchen ceiling below bathroom. Leak from failed shower seal over months. Ceiling and redecoration paid; resealing of shower excluded; gradual damage partially declined for the bathroom floor. Paid £2,840 less £350 excess."),
  doc("CLM-2311", "home", "settled-claim", "Burst pipe in loft — paid in full",
    "Pipe froze and burst in the loft during January cold snap while occupants were home. Trace and access £1,200, drying and repairs £7,900. Paid in full less excess."),
  doc("CLM-2402", "home", "settled-claim", "Fence blown down — declined",
    "Garden fence panels blown down in 60mph winds. Storm definition met but fences are excluded under storm. Declined; policyholder advised."),
  doc("CLM-3107", "motor", "settled-claim", "Keyless relay theft — paid",
    "Car stolen from driveway overnight using keyless relay attack; keys were inside the house. Police crime reference provided. Paid market value less excess."),
  doc("CLM-3150", "motor", "settled-claim", "Delivery driver collision — declined",
    "Collision while delivering takeaway food for an app. Policy shows social, domestic and pleasure only. Declined under use of vehicle; third-party liability handled under statutory obligations with recovery."),
  doc("CLM-4021", "travel", "settled-claim", "Cancellation, undeclared condition — declined",
    "Trip cancelled after heart condition flare-up. Condition diagnosed 2 years earlier and not declared. Declined; premium for declared cover offered for future trips."),
];

export const claimsSpec: UseCaseSpec = {
  bucket: "insurance_claims",
  seedVersion: "claims-v1",
  docs: DOCS,
  noun: "documents",
  title: "Claims Assist",
  subtitle: "First notice of loss → policy wording + similar settled claims → grounded coverage view. Fictional insurer.",
  queryLabel: "Claim narrative (first notice of loss)",
  placeholder: "Describe what happened, as the customer reported it…",
  scenarios: [
    { label: "Ceiling leak", q: "Water coming through the living room ceiling, plumber says the bath seal has been leaking for a while.", filters: { line: "home" } },
    { label: "Keyless car theft", q: "Car taken from outside house overnight, keys were on the hall table, no broken glass.", filters: { line: "motor" } },
    { label: "Gig delivery crash", q: "I hit a parked car while dropping off a parcel for Evri on Saturday.", filters: { line: "motor" } },
    { label: "Fence in storm", q: "Last night's storm flattened our garden fence and cracked two roof tiles.", filters: { line: "home" } },
    { label: "Trip cancelled", q: "Had to cancel Portugal holiday because my mother was admitted to hospital.", filters: { line: "travel" } },
  ],
  filters: [
    { key: "line", label: "Line of business", kind: "select", options: LINES },
    { key: "kind", label: "Document type", kind: "select", options: KINDS },
  ],
  idKey: "doc_id",
  titleKey: "title",
  subKeys: ["line", "kind"],
  resultsTitle: "Wording + precedent claims",
  chat: {
    title: "Coverage assistant",
    subtitle: "Cites policy clauses and precedent claims",
    system:
      "You are a claims handler's assistant. Using ONLY the cited wording and precedent claims [n], give: likely coverage position (covered / partial / likely excluded), the clauses that drive it, the excess, the evidence to request from the customer, and precedent claims that support the view. " +
      "The final decision rests with the handler; flag any facts that must be confirmed.",
    suggestions: [
      "What evidence should I request?",
      "Draft a plain-English update to the customer.",
      "Which precedent claim is closest and why?",
    ],
    placeholder: "Ask about coverage, excess or evidence…",
  },
};
