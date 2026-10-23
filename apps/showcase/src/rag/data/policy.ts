/**
 * Policy Desk corpus — fictional UK SaaS "Northline" employee handbook,
 * security standards and finance rules. Invented; not legal advice.
 */
import type { SeedDoc } from "../seed";

export const SEED_VERSION = "policy-v1";
export const POLICY_BUCKET = "policy_docs";

export const DEPARTMENTS = ["hr", "security", "it", "finance", "travel", "legal"] as const;
export type Department = (typeof DEPARTMENTS)[number];

export const SAMPLE_QUESTIONS: Array<{ label: string; q: string; dept?: Department }> = [
  {
    label: "Remote work abroad",
    dept: "hr",
    q: "Can I work from Spain for three weeks next quarter, and do I need approval?",
  },
  {
    label: "Laptop lost",
    dept: "security",
    q: "My company laptop was stolen from a train. What do I do in the first hour?",
  },
  {
    label: "Expense over limit",
    dept: "finance",
    q: "Client dinner came to £95 per head — is that reclaimable and who approves?",
  },
  {
    label: "Bring your own device",
    dept: "it",
    q: "Can I use my personal Mac for accessing production AWS accounts?",
  },
  {
    label: "NDA with vendor",
    dept: "legal",
    q: "A vendor wants us to sign their NDA before a POC. Who reviews it?",
  },
];

function pol(
  id: string,
  department: Department,
  title: string,
  owner: string,
  updated: string,
  body: string
): SeedDoc {
  return {
    id,
    text: `${id} · ${title}\nOwner: ${owner}\nUpdated: ${updated}\n\n${body}`,
    metadata: { policy_id: id, department, title, owner, updated_at: updated },
  };
}

export const POLICIES: SeedDoc[] = [
  pol(
    "POL-HR-12",
    "hr",
    "Flexible and remote working",
    "People Ops",
    "2026-07-01",
    "UK-contracted employees may work remotely from their UK home address by default. Working from another UK location for up to 10 consecutive working days needs only manager notice in Workday. Working from outside the UK (including EU holiday lets) requires People Ops + Tax approval at least 15 working days before travel when the stay exceeds 5 working days or any work is performed. Stays over 30 calendar days in a tax year need a formal secondment letter. Managers must not approve foreign work informally on Slack. VPN and MFA remain mandatory. Core hours for synchronous meetings are 10:00–16:00 UK time unless the team charter says otherwise."
  ),
  pol(
    "POL-HR-18",
    "hr",
    "Annual leave and public holidays",
    "People Ops",
    "2026-01-15",
    "Full-time staff receive 25 days annual leave plus UK bank holidays. Leave requests over 10 consecutive days need director approval. Carry-over: up to 5 days into the next leave year, used by 31 March. Parental leave, sickness and jury service follow the separate statutory policies. Booking: Workday → Time Off; conflicting critical on-call weeks are blocked automatically for engineering."
  ),
  pol(
    "POL-SEC-03",
    "security",
    "Lost or stolen devices",
    "Security Operations",
    "2026-06-12",
    "Immediate steps (within 60 minutes): 1) Call Security on-call via PagerDuty 'device-loss'. 2) From any phone, ask IT to remote-wipe via MDM (Intune). 3) Change Okta password and revoke sessions. 4) File a police report if theft is suspected and attach the crime reference to the ticket. Do not attempt to track the device yourself. Report phishing or credential exposure on the same ticket. Customer data on the device is encrypted at rest; still treat as a reportable incident if the device was unlocked."
  ),
  pol(
    "POL-SEC-07",
    "security",
    "Access to production systems",
    "Security Architecture",
    "2026-08-20",
    "Production AWS/GCP access is break-glass only via CyberArk, time-boxed to 4 hours, with dual approval (manager + on-call security). Personal devices are never enrolled for production. SSH keys on laptops are forbidden; use short-lived certificates. All production actions are logged to the SIEM for 365 days. Contractors need a named Northline sponsor and expire automatically at contract end."
  ),
  pol(
    "POL-SEC-11",
    "security",
    "Acceptable use and data classification",
    "CISO office",
    "2026-03-01",
    "Data classes: Public, Internal, Confidential, Restricted. Customer PII and payment data are Restricted — store only in approved systems (not personal Drive, not Slack DMs). Screenshots of Restricted data in chat are a policy breach. Generative AI tools: Confidential and below may be pasted into the company-approved Copilot tenant only; Restricted data must never leave Northline systems. Violations are investigated under the disciplinary policy."
  ),
  pol(
    "POL-IT-04",
    "it",
    "Company hardware and BYOD",
    "Workplace IT",
    "2026-05-08",
    "Northline issues encrypted MacBooks (or Linux for platform roles). BYOD phones may enrol in Company Portal for email and MFA only — not for AWS console, VPN split to prod, or source code. Jailbroken/rooted devices are blocked. Loaner laptops: request via IT Service Desk; return within 5 days. Softphones use RingCentral; personal WhatsApp is not an official channel for customer data."
  ),
  pol(
    "POL-IT-09",
    "it",
    "Software installation and SaaS",
    "Workplace IT",
    "2026-04-18",
    "Install only from the Company Portal catalogue. New SaaS needs a Security review if it processes Confidential+ data or connects to Okta. Shadow IT discoveries are logged; repeated bypasses escalate to the manager. Open-source libraries in product code follow the OSS intake checklist (licence + vulnerability scan)."
  ),
  pol(
    "POL-FIN-02",
    "finance",
    "Expenses and entertaining",
    "Finance Ops",
    "2026-02-10",
    "Submit expenses in Expensify within 30 days with itemised receipts. UK meal caps (per person, incl. VAT): breakfast £15, lunch £25, dinner £55; alcohol only with client entertainment and capped at £20/person. Client entertainment over £75/person needs pre-approval from a director. Rail: standard class under 3 hours, advance fares preferred. Hotels: up to £180/night outside London, £250 in London. Personal credit cards preferred; company cards for frequent travellers only."
  ),
  pol(
    "POL-FIN-06",
    "finance",
    "Procurement and purchase orders",
    "Procurement",
    "2026-09-01",
    "Any vendor spend over £2,000 needs a PO before the invoice. SaaS renewals auto-create a PO 60 days out. Prefer vendors on the approved list. New vendors: complete the supplier form + DPIA if personal data is involved. Card payments under £500 for one-off tools are allowed with manager approval and must still be coded to the correct cost centre."
  ),
  pol(
    "POL-TRV-01",
    "travel",
    "Business travel booking",
    "Workplace",
    "2026-01-20",
    "Book through TravelPerk with the Northline profile. Flights: economy under 6 hours; premium economy needs VP approval; business class only for >8 hours overnight with VP+ approval. Carbon surcharge is applied to the cost centre. Travel insurance is automatic. Travel to higher-risk countries requires Security travel clearance."
  ),
  pol(
    "POL-LEG-05",
    "legal",
    "Contracts, NDAs and click-wrap",
    "Legal Counsel",
    "2026-06-30",
    "Customer MSAs and DPAs: Legal must review before signature. Mutual NDAs under the company template may be signed by directors and VPs; vendor-paper NDAs and any NDA with liability caps above £100k or unusual IP assignment go to Legal (sla: 3 business days). Click-wrap for internal tools is fine if Security has approved the vendor. Never paste unsigned customer contracts into public GenAI tools."
  ),
  pol(
    "POL-LEG-08",
    "legal",
    "Speak-up and whistleblowing",
    "Compliance",
    "2025-11-12",
    "Raise concerns via the Speak-Up portal or ethics@northline.example. Retaliation is prohibited. Reports can be anonymous. Criminal matters may be referred to authorities. This channel is not for HR grievances already covered by the grievance procedure."
  ),
];
