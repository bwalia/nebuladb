/**
 * Payment transactions for Fraud Neighbours — nearest-neighbour risk vote
 * (not a generative model).
 */
import type { SeedDoc } from "../../rag/seed";

export const SEED_VERSION = "fraud-v1";
export const TX_BUCKET = "fraud_transactions";

export type Label = "legit" | "fraud" | "review";

function tx(
  id: string,
  label: Label,
  amount_gbp: number,
  mcc: string,
  channel: string,
  country: string,
  device: string,
  narrative: string
): SeedDoc {
  return {
    id,
    text: `${id} · ${label} · £${amount_gbp} · MCC ${mcc} · ${channel} · ${country} · ${device}\n${narrative}`,
    metadata: { tx_id: id, label, amount_gbp, mcc, channel, country, device },
  };
}

export const TRANSACTIONS: SeedDoc[] = [
  tx("TX-9001", "fraud", 1840, "5732", "card-not-present", "NG", "new-device",
    "Electronics purchase, shipping to a freight forwarder, first transaction on a 2-hour-old account, VPN ASN."),
  tx("TX-9002", "fraud", 2100, "5732", "card-not-present", "GH", "new-device",
    "Similar electronics CNP burst — 3 attempts in 12 minutes, mismatched billing/shipping country."),
  tx("TX-9003", "fraud", 990, "5812", "card-not-present", "RO", "emulator",
    "Restaurant MCC but digital goods URL in descriptor; emulator fingerprint."),
  tx("TX-8801", "legit", 42, "5411", "contactless", "GB", "known-device",
    "Grocery near home postcode, known device, daytime."),
  tx("TX-8802", "legit", 1200, "3000", "card-present", "GB", "known-device",
    "Airline ticket at Heathrow, chip & PIN, travel history matches."),
  tx("TX-8803", "legit", 68, "5812", "contactless", "GB", "known-device",
    "Lunch near office, repeated merchant."),
  tx("TX-8701", "review", 450, "5732", "card-not-present", "GB", "known-device",
    "Laptop from UK retailer, known device but amount 4× monthly average."),
  tx("TX-8702", "review", 320, "7995", "card-not-present", "MT", "known-device",
    "Gambling MCC, sudden geography change, otherwise clean history."),
  tx("TX-9004", "fraud", 75, "6011", "atm", "ES", "unknown-atm",
    "ATM cash-out 40 minutes after a CNP electronics fraud on same PAN."),
  tx("TX-9005", "fraud", 1600, "5732", "card-not-present", "NG", "new-device",
    "Repeat of TX-9001 pattern — different merchant, same shipping MULE address token."),
  tx("TX-8804", "legit", 15, "4111", "contactless", "GB", "known-device",
    "TfL transit."),
  tx("TX-8805", "legit", 220, "5651", "card-present", "GB", "known-device",
    "Clothing store on high street, chip."),
  tx("TX-8703", "review", 890, "5722", "card-not-present", "DE", "known-device",
    "Household appliance, EU shipping, first DE purchase."),
  tx("TX-9006", "fraud", 50, "4816", "card-not-present", "RU", "new-device",
    "Digital goods / telecom, TOR exit, BIN-country mismatch."),
];

/** Live alerts the analyst picks — not yet labelled in the bucket. */
export const ALERTS = [
  {
    id: "ALERT-1",
    title: "CNP electronics → freight forwarder",
    text: "£1,760 card-not-present electronics, shipping to a Lagos freight forwarder, account age 3 hours, new device, MCC 5732.",
  },
  {
    id: "ALERT-2",
    title: "Travel-sized legit?",
    text: "£1,150 airline purchase at LHR, chip & PIN, known device, customer has two prior trips this year.",
  },
  {
    id: "ALERT-3",
    title: "MCC mismatch digital goods",
    text: "£80 charge coded as restaurant (5812) but descriptor looks like a VPN subscription, emulator UA, Romania IP.",
  },
];
