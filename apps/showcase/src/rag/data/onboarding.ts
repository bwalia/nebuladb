/**
 * Onboarding Concierge — handbook, IT setup and benefits for fictional
 * employer "Northline". Role-tagged so SQL can filter.
 */
import type { SeedDoc } from "../seed";

export const SEED_VERSION = "onboard-v1";
export const ONBOARD_BUCKET = "onboarding_docs";

export const ROLES = ["all", "engineering", "sales", "people-manager"] as const;
export type Role = (typeof ROLES)[number];

export const DAY_ONE: Array<{ id: string; title: string; role?: Role }> = [
  { id: "OB-IT-01", title: "Collect laptop & enrol MDM" },
  { id: "OB-IT-02", title: "Okta + MFA" },
  { id: "OB-HR-01", title: "Complete Right to Work & payroll" },
  { id: "OB-HR-03", title: "Benefits enrolment window" },
  { id: "OB-ENG-01", title: "Dev environment (engineers)", role: "engineering" },
  { id: "OB-SAL-01", title: "CRM & territory briefing (sales)", role: "sales" },
  { id: "OB-MGR-01", title: "Manager toolkit (people managers)", role: "people-manager" },
];

export const SAMPLE_QUESTIONS: Array<{ label: string; q: string; role?: Role }> = [
  { label: "First-day VPN", q: "How do I get on the corporate VPN on day one?" },
  { label: "Pension", q: "When can I join the pension and what is the default contribution?" },
  {
    label: "Repo access",
    role: "engineering",
    q: "How do I get GitHub access and clone the platform monorepo?",
  },
  {
    label: "Demo environment",
    role: "sales",
    q: "Where is the customer demo tenant and who resets it before a pitch?",
  },
  {
    label: "1:1s",
    role: "people-manager",
    q: "What is the expected cadence for 1:1s and performance check-ins?",
  },
];

function doc(id: string, role: Role, title: string, body: string): SeedDoc {
  return {
    id,
    text: `${id} · ${title}\nAudience: ${role}\n\n${body}`,
    metadata: { doc_id: id, role, title },
  };
}

export const DOCS: SeedDoc[] = [
  doc(
    "OB-IT-01",
    "all",
    "Laptop collection and MDM",
    "Collect your MacBook from Workplace on floor 3 (bring photo ID). Open Company Portal and enrol — encryption and FileVault are enforced automatically. Do not postpone enrolment; email and Slack stay locked until MDM reports healthy. Loaner process: IT Service Desk if your build is delayed."
  ),
  doc(
    "OB-IT-02",
    "all",
    "Okta, MFA and VPN",
    "Activate Okta from the welcome email (expires in 48 hours). Enrol a hardware key or platform authenticator; SMS is backup only. VPN: install the Company Portal 'Northline VPN' profile; connect before opening internal wikis. Split tunnel excludes public SaaS. If MFA loops, use a backup code then visit IT."
  ),
  doc(
    "OB-IT-03",
    "all",
    "Slack, email and calendar norms",
    "Slack workspace northline-hq. Channels: #announcements (read-only), #help-it, #help-people. Email signature template is in the wiki. Meetings default to 25/50 minutes. No-meeting Wednesday mornings UK for makers."
  ),
  doc(
    "OB-HR-01",
    "all",
    "Right to work, payroll and IR35",
    "Upload Right to Work documents in Workday before day 3. Payroll is monthly on the last working day; submit bank details by the 5th. Contractors: IR35 status is determined by People Ops — do not start without a clearance email."
  ),
  doc(
    "OB-HR-03",
    "all",
    "Benefits and pension",
    "Medical, dental and income protection open in the first 30 days via Benefex. Pension: auto-enrolment from the first full pay month; default 4% employee + 5% employer; change anytime in the pension portal. Cycle-to-work and learning budget (£1,000/year) start after probation."
  ),
  doc(
    "OB-HR-05",
    "all",
    "Probation and leave in the first 90 days",
    "Probation is 3 months with a 6-week check-in. Annual leave accrues from day one; taking more than 5 days in probation needs People Ops + manager approval. Sickness: notify your manager before 09:30 and log in Workday."
  ),
  doc(
    "OB-ENG-01",
    "engineering",
    "Engineering day-one setup",
    "Request GitHub org access via #help-it (template 'eng-github'). Clone nebula-platform with SSH certs from Teleport — personal PATs are rejected. Install mise + docker. Run `make doctor`. Staging kubeconfig is granted after the security awareness quiz. Pair with your onboarding buddy on the first service tour."
  ),
  doc(
    "OB-ENG-02",
    "engineering",
    "On-call and incident expectations",
    "New engineers join the shadow on-call rotation after month 2. Read Incident Copilot runbooks. Sev-1 pages always get a Slack huddle in #incident-active. Never invent a rollback without the service owner."
  ),
  doc(
    "OB-SAL-01",
    "sales",
    "CRM, demos and collateral",
    "Salesforce access lands after enablement day. Demo tenant: demo.northline.example — reset via #sales-ops at least 2 hours before a customer meeting. Battle cards and security questionnaires live in Highspot. Never promise roadmap dates not marked GA."
  ),
  doc(
    "OB-SAL-02",
    "sales",
    "Discount authority",
    "List pricing is in CPQ. Discounts: AE up to 10%, Regional Director to 20%, VP+ above that with deal desk. Marketplace private offers need 5 business days."
  ),
  doc(
    "OB-MGR-01",
    "people-manager",
    "Manager toolkit",
    "Weekly 1:1s (30 min minimum). Use Lattice for goals; set 30/60/90 for new hires. Budget and headcount requests go through the QBR pack. Escalations: People BP partner listed on the team page. Complete 'Hiring at Northline' before opening a req."
  ),
  doc(
    "OB-MGR-02",
    "people-manager",
    "Feedback and performance",
    "Continuous feedback in Lattice. Formal reviews twice yearly. Performance improvement plans require People Ops co-ownership before delivery. Never discuss performance in public Slack channels."
  ),
];
