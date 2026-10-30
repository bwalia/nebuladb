/**
 * Clinical Guidelines — ward staff ask a bedside question; NebulaDB
 * retrieves the trust's pathway sections, filtered by specialty; the
 * chat answers with citations. Fictional "Northline Health" pathways,
 * illustrative only — not clinical advice.
 */
import type { SeedDoc } from "../../rag/seed";
import type { UseCaseSpec } from "../UseCaseDemo";

const SPECIALTIES = ["acute", "cardiology", "respiratory", "paediatrics", "pharmacy", "infection"] as const;

function g(id: string, specialty: string, title: string, version: string, body: string): SeedDoc {
  return {
    id,
    text: `${id} · ${title}\nSpecialty: ${specialty} · Version ${version}\n\n${body}`,
    metadata: { guideline_id: id, specialty, title, version },
  };
}

const DOCS: SeedDoc[] = [
  g("CG-ACU-01", "acute", "Deteriorating adult: early warning score escalation", "4.2",
    "Score 1–4: increase observations to 4-hourly and inform the nurse in charge. Score 5–6 or any single parameter scoring 3: urgent review by ward doctor within 30 minutes. Score 7 or more: emergency response — call the critical care outreach team and consider transfer to a higher level of care. Document the escalation and the plan in the observation chart."),
  g("CG-ACU-02", "acute", "Suspected sepsis: first-hour bundle", "3.0",
    "Suspect sepsis when an infection is likely and the early warning score is 5 or more. Within one hour: take blood cultures before antibiotics where it does not delay treatment, give broad-spectrum antibiotics per the local antimicrobial guide, measure lactate, start fluid resuscitation if hypotensive or lactate is raised, and monitor urine output. Senior review is required within the hour."),
  g("CG-ACU-03", "acute", "Falls in hospital: post-fall assessment", "2.1",
    "After any inpatient fall, do not move the patient until checked for injury. Check for head injury and anticoagulant use; patients on anticoagulants with a suspected head injury need medical review and imaging consideration. Repeat neurological observations per the head-injury chart. Complete an incident report before end of shift."),
  g("CG-CAR-01", "cardiology", "Chest pain: initial assessment pathway", "5.0",
    "Record a 12-lead ECG within 10 minutes of arrival and have it reviewed by a clinician. Check troponin per the high-sensitivity protocol at 0 and 1 hour. Any ST elevation activates the primary PCI pathway — call the cardiology registrar immediately. Give pain relief and do not delay the ECG for history taking."),
  g("CG-CAR-02", "cardiology", "New atrial fibrillation on the ward", "2.4",
    "Assess haemodynamic stability first; unstable patients need emergency review. For stable patients, rate control is first line. Assess stroke and bleeding risk before starting anticoagulation and record both scores. Refer to cardiology if onset is under 48 hours and rhythm control is being considered."),
  g("CG-RES-01", "respiratory", "Oxygen prescribing and target saturations", "3.3",
    "Oxygen must be prescribed with a target range. Most adults: target 94–98%. Patients at risk of hypercapnic respiratory failure (for example known COPD with previous hypercapnia): target 88–92% until blood gases are known. Review targets daily and wean when within range."),
  g("CG-RES-02", "respiratory", "COPD exacerbation: ward management", "2.0",
    "Use controlled oxygen to the 88–92% target. Give nebulised bronchodilators and a short course of oral steroids per the formulary. Obtain an arterial or venous blood gas if drowsy or if oxygen requirements rise. Consider non-invasive ventilation referral for persistent respiratory acidosis despite treatment."),
  g("CG-PAE-01", "paediatrics", "Febrile child under 5: traffic-light assessment", "4.0",
    "Assess colour, activity, respiratory, circulation/hydration and other features against the traffic-light table. Any red feature requires same-day paediatric assessment. Infants under 3 months with a temperature of 38°C or above are treated as high risk. Give safety-netting advice in writing to parents on discharge."),
  g("CG-PAE-02", "paediatrics", "Paediatric dosing: weight-based checks", "1.8",
    "All paediatric medicines are dosed by weight; record the weight in kilograms on the chart before prescribing. A second registered practitioner must independently check any calculated dose for intravenous drugs. Never exceed the adult maximum dose regardless of weight."),
  g("CG-PHA-01", "pharmacy", "Anticoagulant interruption before procedures", "2.6",
    "Decisions to pause anticoagulants must consider procedure bleeding risk and the patient's thrombotic risk. Pharmacy and the procedural team agree a written plan, including when to restart. Do not stop anticoagulation in patients with mechanical heart valves without specialist advice."),
  g("CG-PHA-02", "pharmacy", "Penicillin allergy: de-labelling and alternatives", "1.5",
    "Clarify the allergy history: rash versus anaphylaxis versus intolerance such as nausea. Intolerances are not allergies and should be relabelled. Low-risk histories can be referred to the de-labelling service. Use the antimicrobial guide's non-penicillin alternatives while the label is in place."),
  g("CG-INF-01", "infection", "C. difficile: isolation and stool sampling", "3.1",
    "Isolate any patient with diarrhoea of potentially infectious cause in a side room with en-suite or dedicated commode within 2 hours. Send a stool sample. Use gloves and aprons; clean hands with soap and water, not alcohol gel. Review antibiotics and stop proton pump inhibitors where possible."),
  g("CG-INF-02", "infection", "Needlestick injury: first steps", "2.2",
    "Encourage bleeding, wash with soap and running water, do not scrub. Report immediately to the occupational health line or, out of hours, the emergency department for risk assessment. Post-exposure prophylaxis, if indicated, should start as soon as possible. Record the source patient details for risk assessment."),
];

export const clinicalSpec: UseCaseSpec = {
  bucket: "clinical_guidelines",
  blueprint: "clinical",
  seedVersion: "clinical-v1",
  docs: DOCS,
  noun: "guidelines",
  title: "Clinical Guidelines",
  subtitle: "Bedside question → trust pathways filtered by specialty → cited answer. Illustrative pathways, not clinical advice.",
  queryLabel: "Clinical question",
  placeholder: "Ask about escalation, sepsis, oxygen targets…",
  scenarios: [
    { label: "NEWS 6 on night shift", q: "Patient scored 6 on early warning at 2am, what's the escalation and timeframe?", filters: { specialty: "acute" } },
    { label: "Sepsis first hour", q: "Suspected urinary sepsis with low blood pressure — what must happen in the first hour?" },
    { label: "COPD oxygen target", q: "COPD patient sats 97% on 4 litres, is that right?", filters: { specialty: "respiratory" } },
    { label: "Fall on apixaban", q: "Elderly patient fell out of bed and is on apixaban, bumped head. Next steps?" },
    { label: "Feverish 2-month-old", q: "Two month old baby with temperature 38.4, mum says feeding less." },
  ],
  filters: [{ key: "specialty", label: "Specialty", kind: "select", options: SPECIALTIES }],
  idKey: "guideline_id",
  titleKey: "title",
  subKeys: ["specialty", "version"],
  resultsTitle: "Matched pathway sections",
  chat: {
    title: "Pathway assistant",
    subtitle: "Answers only from the retrieved trust pathways",
    system:
      "You are a clinical decision-support assistant for a hospital trust. Answer ONLY from the cited pathway excerpts and cite them as [n]. " +
      "Give actions in order with timeframes. If the excerpts do not cover the situation, say so and advise escalation to a senior clinician. " +
      "Always end with: 'Decision support only — follow senior clinical judgement.'",
    suggestions: ["Turn this into a checklist with timings.", "Who do I escalate to?", "What must be documented?"],
    placeholder: "Ask a follow-up about these pathways…",
  },
};
