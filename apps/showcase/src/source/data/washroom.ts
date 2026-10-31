/**
 * `ss_washroom_jobs` — invented Source Washroom service history: planned
 * sanitary visits (completed or missed) and resolved customer tickets.
 * Sites and accounts match `ss_accounts`. Dates sit on the fixed demo
 * calendar (DEMO_TODAY).
 */
import { B, DEMO_TODAY, slug, type SeedDoc } from "./common.ts";
import { SITES, type Service, type Site } from "./accounts.ts";

export const SEED_VERSION = "ss-washroom-v1";
export const BUCKET = B.washroom;

export type Priority = "P1" | "P2" | "P3" | "P4";
export const PRIORITIES: Priority[] = ["P1", "P2", "P3", "P4"];

export interface WashroomJob {
  job_id: string;
  kind: "visit" | "ticket";
  account_id: string;
  account_name: string;
  site: string;
  service: Service;
  status: "completed" | "missed" | "resolved";
  visit_date: string;
  issue: string;
  priority: Priority;
  fix: string;
}

/** ISO date arithmetic on UTC days. */
export function addDays(iso: string, days: number): string {
  const d = new Date(`${iso}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

export function daysBetween(a: string, b: string): number {
  return Math.round((Date.parse(`${b}T00:00:00Z`) - Date.parse(`${a}T00:00:00Z`)) / 86_400_000);
}

/** Sites whose latest planned sanitary visit was missed (the demo story). */
export const MISSED: Record<string, string> = {
  "KINGSMEAD/South site": "2026-10-13",
  "MERIDIAN/Park Halls": "2026-10-20",
  "CASTLEGATE/Westbrook Pool": "2026-10-07",
  "HARBOUR/Elm Lodge": "2026-10-22",
};

function visits(s: Site, idx: number): WashroomJob[] {
  const key = `${s.account_id}/${s.site}`;
  const missed = MISSED[key];
  // Last completed visit: one cycle before the missed one, or recently.
  const last = missed ? addDays(missed, -s.washroom_cycle_days) : addDays(DEMO_TODAY, -((idx * 3) % (s.washroom_cycle_days - 2)) - 2);
  const prev = addDays(last, -s.washroom_cycle_days);
  const mk = (date: string, status: WashroomJob["status"], n: number): WashroomJob => ({
    job_id: `V-${s.account_id.slice(0, 3)}-${slug(s.site)}-${n}`,
    kind: "visit",
    account_id: s.account_id,
    account_name: s.account_name,
    site: s.site,
    service: "sanitary",
    status,
    visit_date: date,
    issue: status === "missed" ? "Planned sanitary visit not completed." : "Planned sanitary visit.",
    priority: status === "missed" ? "P2" : "P4",
    fix:
      status === "missed"
        ? "Visit not completed: technician could not get access and the job was not rebooked."
        : `All sanitary units exchanged. Next visit due in ${s.washroom_cycle_days} days.`,
  });
  const out = [mk(prev, "completed", 1), mk(last, "completed", 2)];
  if (missed) out.push(mk(missed, "missed", 3));
  return out;
}

const TICKET_TEMPLATES: Array<[Service, Priority, string, string]> = [
  ["sanitary", "P2", "Sanitary bin in the ladies' toilet on floor 2 is overflowing and smells.", "Emergency exchange within 24 hours. Added one extra unit and moved the site to a 14-day cycle."],
  ["sanitary", "P1", "Sanitary units not changed for 3 weeks, strong smell in the staff toilets.", "Missed visit confirmed on the job platform. Same-day emergency exchange, apology sent and the missed visit credited."],
  ["sanitary", "P3", "Lid broken on the sanitary unit in Block B.", "Unit swapped on the next planned visit at no charge."],
  ["sanitary", "P4", "We need a sanitary unit in the new accessible toilet.", "Unit added to the contract from the next visit. Quote sent to the site manager."],
  ["sanitary", "P2", "Bins in the girls' toilets are full before the visit is due.", "Visit cycle changed from 28 to 14 days during term time."],
  ["nappy", "P2", "Nappy bin in the baby change room is full and smells.", "Exchange visit the next day. Added a second nappy unit."],
  ["nappy", "P3", "Pedal on the nappy unit is broken.", "Unit swapped on the next visit."],
  ["air-care", "P3", "Air freshener in the reception toilets has stopped working.", "Refill replaced and a battery-free passive unit fitted."],
  ["air-care", "P3", "Smell in the men's toilets even with the air freshener.", "Air care cannot hide urinal scale. Recommended descaling and enzyme urinal screens; added to the cleaning routine."],
  ["air-care", "P4", "Fragrance is too strong and staff are complaining.", "Changed to a low-intensity refill."],
  ["dryer", "P2", "Hand dryer in the first-floor toilets is not working.", "Engineer visit within 48 hours. Thermal cut-out reset and filter replaced."],
  ["dryer", "P4", "Hand dryer is very loud and slow.", "Quoted a high-speed low-energy dryer (HD-570)."],
  ["dryer", "P1", "Hand dryer keeps tripping the electrics.", "Dryer isolated the same day. Fault found and unit replaced."],
  ["refills", "P2", "Ran out of hand towels on Friday, no spare stock on site.", "Next-day delivery of recycled towels. Added one spare case to the standing order."],
  ["refills", "P2", "Soap dispensers are empty in the sports hall.", "Refill sent on the next van. Foam soap added to the weekly refills."],
  ["refills", "P3", "Wrong size toilet rolls delivered for our dispensers.", "Collected and swapped for the correct jumbo rolls on the next delivery."],
];

function tickets(): WashroomJob[] {
  const out: WashroomJob[] = [];
  let n = 0;
  // Two passes over the templates, spread across sites that have that service.
  for (let pass = 0; pass < 2; pass++) {
    TICKET_TEMPLATES.forEach(([service, priority, issue, fix], i) => {
      const eligible = SITES.filter((s) => s.washroom_services.includes(service) || service === "refills");
      const s = eligible[(i * 5 + pass * 7) % eligible.length];
      n++;
      out.push({
        job_id: `T-${String(4100 + n)}`,
        kind: "ticket",
        account_id: s.account_id,
        account_name: s.account_name,
        site: s.site,
        service,
        status: "resolved",
        visit_date: addDays(DEMO_TODAY, -(9 + ((i * 11 + pass * 37) % 160))),
        issue,
        priority,
        fix,
      });
    });
  }
  return out;
}

export const JOBS: WashroomJob[] = [...SITES.flatMap(visits), ...tickets()];

function jobText(j: WashroomJob): string {
  return j.kind === "visit"
    ? `Service visit ${j.job_id}: ${j.service} at ${j.account_name}, ${j.site} on ${j.visit_date}. Status: ${j.status}. ${j.fix} Demo service record.`
    : `Ticket ${j.job_id} (${j.priority}, ${j.service}) from ${j.account_name}, ${j.site} on ${j.visit_date}: ${j.issue} Fix: ${j.fix} Demo service record.`;
}

export const JOB_DOCS: SeedDoc[] = JOBS.map((j) => ({
  id: `ss-job-${j.job_id}`,
  text: jobText(j),
  metadata: { ...j },
}));
