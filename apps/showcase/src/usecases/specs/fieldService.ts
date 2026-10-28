/**
 * Field Service — a technician describes a fault on site; NebulaDB
 * pulls manual sections, fault-code entries and past job notes for that
 * equipment model; the chat gives a step-by-step fix. Fictional heat
 * pump and chiller models.
 */
import type { SeedDoc } from "../../rag/seed";
import type { UseCaseSpec } from "../UseCaseDemo";

const MODELS = ["AeroHeat 12", "AeroHeat 16", "ChillMax 400"] as const;
const SOURCES = ["manual", "fault-code", "job-note"] as const;

function d(id: string, model: string, source: string, title: string, body: string): SeedDoc {
  return {
    id,
    text: `${id} · ${model} · ${source} · ${title}\n${body}`,
    metadata: { doc_id: id, model, source, title },
  };
}

const DOCS: SeedDoc[] = [
  d("AH12-E07", "AeroHeat 12", "fault-code", "E07 — high discharge temperature",
    "Discharge temperature above 105°C. Common causes: low refrigerant charge, blocked outdoor coil, faulty discharge sensor. Check sensor resistance (10kΩ at 25°C) before checking pressures."),
  d("AH12-E11", "AeroHeat 12", "fault-code", "E11 — low water flow",
    "Flow switch open for more than 15 seconds while the pump runs. Check strainer, purge air from the system, confirm pump speed setting 3, check the flow switch paddle orientation."),
  d("AH12-M4.2", "AeroHeat 12", "manual", "Defrost cycle",
    "Defrost starts when coil temperature is below -3°C for 40 minutes. Steam from the outdoor unit during defrost is normal. Repeated defrost every 20 minutes indicates a coil sensor fault or poor airflow."),
  d("AH12-M6.1", "AeroHeat 12", "manual", "Isolating the unit safely",
    "Isolate at the rotary isolator and the consumer unit. Wait 5 minutes for inverter capacitors to discharge; confirm under 50V DC on the DC bus test points before touching the inverter board."),
  d("AH16-E07", "AeroHeat 16", "fault-code", "E07 — high discharge temperature",
    "As AeroHeat 12, but discharge sensor is on the compressor top; firmware below 3.4 can raise false E07 in ambient above 32°C — update firmware first."),
  d("AH16-E21", "AeroHeat 16", "fault-code", "E21 — inverter communication lost",
    "Indoor controller cannot reach the outdoor inverter board. Check the 3-core comms cable polarity, termination resistor DIP switch 4 on, and that cable is not run alongside mains."),
  d("AH16-M3.3", "AeroHeat 16", "manual", "Commissioning water pressure",
    "System pressure cold should be 1.2–1.5 bar. Expansion vessel pre-charge 1.0 bar. Below 0.8 bar the unit locks out with E11/E12 and requires a manual reset."),
  d("CM400-A31", "ChillMax 400", "fault-code", "A31 — low suction pressure",
    "Suction below 2.1 bar for 60 seconds. Check for a refrigerant leak, iced evaporator from low chilled-water flow, or a sticking electronic expansion valve. Log the event in the F-gas record."),
  d("CM400-M8.0", "ChillMax 400", "manual", "F-gas leak check requirement",
    "Systems above 5 tonnes CO2e need periodic leak checks by a certified engineer and records kept for 5 years. Never vent refrigerant; recover into a labelled cylinder."),
  d("JOB-55120", "AeroHeat 12", "job-note", "Recurring E11 after radiator upgrade",
    "Customer had new radiators fitted; magnetic filter was full of debris. Cleaned filter and strainer, bled air at the top radiator, raised pump to speed 3. No recurrence in 30 days."),
  d("JOB-55488", "AeroHeat 16", "job-note", "E21 every evening",
    "Comms cable shared a trunking with an EV charger feed; interference at peak charging. Rerouted cable in separate conduit, termination DIP 4 was off — switched on. Fixed."),
  d("JOB-56002", "AeroHeat 12", "job-note", "E07 in summer, coil clogged",
    "Outdoor coil choked with cottonwood seeds. Washed coil with low-pressure water, discharge temp dropped 18°C. Advised customer to keep 300mm clearance."),
  d("JOB-56331", "ChillMax 400", "job-note", "A31 on start-up in winter",
    "Glycol mix too weak after a top-up with mains water; evaporator iced. Corrected glycol to 30%, reset. Leak check clean, recorded in F-gas log."),
];

export const fieldServiceSpec: UseCaseSpec = {
  bucket: "field_service",
  seedVersion: "field-v1",
  docs: DOCS,
  noun: "manual entries",
  title: "Field Service Assistant",
  subtitle: "Technician fault description → manuals, fault codes and past job notes for the model → step-by-step fix",
  queryLabel: "What are you seeing on site?",
  placeholder: "Fault code, symptoms, what you've already tried…",
  scenarios: [
    { label: "E11 after new radiators", q: "Unit keeps showing E11, customer just had new radiators fitted, pressure looks fine.", filters: { model: "AeroHeat 12" } },
    { label: "E21 at night", q: "E21 comms fault appears most evenings around 6pm then clears.", filters: { model: "AeroHeat 16" } },
    { label: "Hot day E07", q: "E07 high discharge on a 34 degree afternoon, outdoor fan is spinning.", filters: { model: "AeroHeat 16" } },
    { label: "Chiller low suction", q: "Chiller trips on A31 low suction after the client topped up the system.", filters: { model: "ChillMax 400" } },
  ],
  filters: [
    { key: "model", label: "Equipment model", kind: "select", options: MODELS },
    { key: "source", label: "Source", kind: "select", options: SOURCES },
  ],
  idKey: "doc_id",
  titleKey: "title",
  subKeys: ["model", "source"],
  resultsTitle: "Manuals, codes and job notes",
  chat: {
    title: "Fix guide",
    subtitle: "Step-by-step, cited to manuals and past jobs",
    system:
      "You are a senior heat-pump and chiller engineer coaching a technician on site. Using ONLY the cited excerpts [n], give a numbered diagnostic sequence from quickest/cheapest check to most invasive, with expected readings. " +
      "Put any safety isolation step first when electrical work is involved. Mention regulatory record-keeping when refrigerant is involved.",
    suggestions: ["What should I check first?", "Write the job note for this visit.", "Which past job matches best?"],
    placeholder: "Ask about readings, parts or next checks…",
  },
};
