/**
 * Contract Review demo corpus: a UK buyer-side SaaS procurement
 * playbook (England & Wales law) plus two fictional vendor contracts.
 * Vendors, terms and figures are invented for the demo.
 */
import type { SeedDoc } from "../seed";

export const PLAYBOOK_BUCKET = "legal_playbook";
export const SEED_VERSION = "contracts-v1";

export type Risk = "high" | "medium" | "low";
export type Approver = "legal" | "finance" | "security";

interface Entry {
  clause_type: string;
  title: string;
  preferred: string;
  fallback: string;
  red_lines: string[];
  risk_if_missing: Risk;
  approver: Approver;
  rationale: string;
}

const ENTRIES: Entry[] = [
  {
    clause_type: "limitation_of_liability",
    title: "Limitation of liability",
    preferred:
      "Each party's aggregate liability is capped at 150% of the fees paid or payable in the 12 months before the claim. Uncapped: death or personal injury caused by negligence, fraud, breach of confidentiality, data protection breaches and IP indemnity.",
    fallback:
      "Cap at 100% of 12 months' fees, with a separate super-cap of 3x annual fees (minimum £2m) for data protection and confidentiality breaches.",
    red_lines: [
      "Vendor liability capped below 12 months' fees",
      "Customer liability uncapped while vendor liability is capped",
      "Exclusion of liability for loss or corruption of customer data",
    ],
    risk_if_missing: "high",
    approver: "legal",
    rationale:
      "Liability caps decide who carries the loss when the service fails. A cap tied to a short fee period leaves us under-compensated for a data incident.",
  },
  {
    clause_type: "ip_indemnity",
    title: "IP indemnity",
    preferred:
      "Vendor indemnifies the customer against all third-party claims that the service or deliverables infringe intellectual property rights, uncapped, with vendor conduct of claims and an obligation to procure a licence, modify or refund.",
    fallback: "IP indemnity subject to the data protection super-cap (3x annual fees).",
    red_lines: ["No IP indemnity from the vendor", "IP indemnity limited to the general cap"],
    risk_if_missing: "high",
    approver: "legal",
    rationale: "Infringement claims can stop us using a critical service; the vendor controls the code and should carry that risk.",
  },
  {
    clause_type: "data_protection",
    title: "Data protection (UK GDPR) and DPA",
    preferred:
      "Vendor acts as processor under a signed Article 28 UK GDPR data processing agreement, processes personal data only on documented instructions, and uses the UK IDTA or Addendum for any restricted transfer.",
    fallback: "Vendor's standard DPA accepted if it meets Article 28 and includes the UK Addendum to the EU SCCs.",
    red_lines: [
      "No data processing agreement",
      "Vendor may process personal data for its own purposes",
      "International transfers without a UK transfer mechanism",
    ],
    risk_if_missing: "high",
    approver: "security",
    rationale: "UK GDPR Article 28 makes a written processor contract mandatory; ICO fines land on the controller.",
  },
  {
    clause_type: "sub_processors",
    title: "Sub-processors",
    preferred:
      "Vendor publishes its sub-processor list, gives 30 days' prior written notice of any new sub-processor, and the customer may object on reasonable grounds and terminate without penalty if the objection is not resolved.",
    fallback: "14 days' notice with a right to object and terminate the affected service.",
    red_lines: ["Sub-processors added without notice", "No right to object to a new sub-processor"],
    risk_if_missing: "medium",
    approver: "security",
    rationale: "Every sub-processor extends our attack surface and transfer footprint.",
  },
  {
    clause_type: "breach_notification",
    title: "Personal data breach notification",
    preferred:
      "Vendor notifies the customer without undue delay and in any event within 24 hours of becoming aware of a personal data breach, with the information needed for the customer's ICO notification.",
    fallback: "Notification within 48 hours.",
    red_lines: ["Notification later than 72 hours", "Notification only 'as required by law'"],
    risk_if_missing: "high",
    approver: "security",
    rationale: "We must report qualifying breaches to the ICO within 72 hours, so the vendor's notice has to arrive well before that.",
  },
  {
    clause_type: "security_standards",
    title: "Information security",
    preferred:
      "Vendor maintains ISO 27001 certification or SOC 2 Type II, encrypts data in transit and at rest, runs annual penetration tests and shares summaries, and meets the security schedule.",
    fallback: "Equivalent controls evidenced by a completed security questionnaire and annual attestation.",
    red_lines: ["No contractual security commitments", "Vendor may reduce security measures at its discretion"],
    risk_if_missing: "medium",
    approver: "security",
    rationale: "Security obligations need to be contractual so they can be enforced.",
  },
  {
    clause_type: "sla_service_credits",
    title: "Service levels and credits",
    preferred:
      "99.9% monthly availability with service credits of 5–25% of monthly fees; credits are not the sole remedy; repeated failure (3 months in any 6) is a termination right.",
    fallback: "99.5% availability, credits as sole remedy except for chronic failure termination.",
    red_lines: ["No availability commitment", "Service credits as sole and exclusive remedy with no termination right"],
    risk_if_missing: "medium",
    approver: "finance",
    rationale: "Availability targets with teeth keep the vendor incentivised on operational quality.",
  },
  {
    clause_type: "termination_for_convenience",
    title: "Termination for convenience",
    preferred: "Customer may terminate for convenience on 90 days' written notice, with a pro-rata refund of prepaid fees.",
    fallback: "Termination for convenience at each anniversary on 60 days' notice.",
    red_lines: ["No customer exit right for the whole term beyond 12 months"],
    risk_if_missing: "medium",
    approver: "finance",
    rationale: "Multi-year commitments without an exit right lock in budget and prevent switching.",
  },
  {
    clause_type: "auto_renewal",
    title: "Renewal",
    preferred: "No automatic renewal. Any renewal is by mutual written agreement.",
    fallback: "Automatic renewal for successive 12-month periods unless either party gives 30 days' notice; vendor sends a renewal reminder 60 days before expiry.",
    red_lines: ["Automatic renewal for more than 12 months", "Non-renewal notice period longer than 60 days"],
    risk_if_missing: "low",
    approver: "finance",
    rationale: "Long auto-renewals with long notice windows are the most common cause of unwanted spend.",
  },
  {
    clause_type: "price_increases",
    title: "Fees and price increases",
    preferred: "Fees fixed for the initial term. Increases on renewal capped at the lower of UK CPI and 3%, with 90 days' written notice.",
    fallback: "Annual increase capped at CPI + 2%.",
    red_lines: ["Vendor may change fees unilaterally or at its sole discretion", "Uncapped price increases"],
    risk_if_missing: "medium",
    approver: "finance",
    rationale: "Unilateral pricing makes the commercial deal unenforceable in practice.",
  },
  {
    clause_type: "governing_law",
    title: "Governing law and jurisdiction",
    preferred: "Governed by the law of England and Wales; exclusive jurisdiction of the courts of England and Wales.",
    fallback: "Law of England and Wales with LCIA arbitration seated in London.",
    red_lines: ["Foreign governing law (e.g. Delaware, New York, California)", "Foreign exclusive jurisdiction"],
    risk_if_missing: "medium",
    approver: "legal",
    rationale: "Litigating under foreign law multiplies cost and uncertainty for a UK customer.",
  },
  {
    clause_type: "assignment_change_of_control",
    title: "Assignment and change of control",
    preferred:
      "Neither party may assign without the other's prior written consent (not unreasonably withheld); customer may terminate if the vendor undergoes a change of control to a competitor.",
    fallback: "Vendor may assign to an affiliate or successor on written notice, with a customer termination right.",
    red_lines: ["Vendor may assign freely without notice"],
    risk_if_missing: "low",
    approver: "legal",
    rationale: "We need control over who ends up holding our data and contract.",
  },
  {
    clause_type: "confidentiality",
    title: "Confidentiality",
    preferred: "Mutual confidentiality, surviving 5 years after termination (indefinitely for trade secrets and personal data), with standard carve-outs.",
    fallback: "Mutual confidentiality surviving 3 years.",
    red_lines: ["One-way confidentiality protecting only the vendor", "Confidentiality ending on termination"],
    risk_if_missing: "high",
    approver: "legal",
    rationale: "Our data and plans are disclosed to the vendor throughout the relationship.",
  },
  {
    clause_type: "customer_data_ownership",
    title: "Customer data ownership",
    preferred: "Customer owns all customer data and outputs. Vendor gets a limited licence to process customer data only to provide the service.",
    fallback: "Vendor may use aggregated, anonymised usage statistics that cannot identify the customer or any individual.",
    red_lines: ["Vendor claims ownership of or a perpetual licence to customer data", "Vendor may use customer data for any purpose"],
    risk_if_missing: "high",
    approver: "legal",
    rationale: "Data is the asset; ambiguity about ownership is a permanent leak.",
  },
  {
    clause_type: "ai_model_training",
    title: "AI and model training on customer data",
    preferred: "Vendor must not use customer data, prompts or outputs to train, fine-tune or improve any machine-learning model, for itself or third parties, without the customer's prior written opt-in.",
    fallback: "Training only on de-identified data with an opt-out the customer exercises at signature.",
    red_lines: ["Vendor may use customer data to train or improve its models by default", "Training rights that survive termination"],
    risk_if_missing: "medium",
    approver: "security",
    rationale: "Once data is in a model's weights it cannot be deleted, and it can leak to other customers.",
  },
  {
    clause_type: "audit_rights",
    title: "Audit rights",
    preferred: "Customer (or its auditor or regulator) may audit the vendor's compliance once a year on 30 days' notice, and at any time after a security incident.",
    fallback: "Vendor provides third-party audit reports (SOC 2 / ISO) annually, with on-site audit only after an incident.",
    red_lines: ["No audit or information rights"],
    risk_if_missing: "medium",
    approver: "security",
    rationale: "Regulated customers (FCA, PRA) must be able to evidence oversight of outsourced services.",
  },
  {
    clause_type: "insurance",
    title: "Insurance",
    preferred: "Vendor maintains professional indemnity £5m, cyber £5m, and public and employer's liability insurance as required by law, with evidence on request.",
    fallback: "Professional indemnity and cyber £2m each.",
    red_lines: ["No insurance obligations"],
    risk_if_missing: "low",
    approver: "finance",
    rationale: "Insurance makes the liability cap collectable.",
  },
  {
    clause_type: "warranties",
    title: "Warranties",
    preferred: "Vendor warrants the service will perform materially in line with the documentation, will be delivered with reasonable skill and care, and will be free of malicious code.",
    fallback: "Performance warranty for 90 days with re-performance or refund remedy.",
    red_lines: ["Service provided 'as is' with all warranties excluded"],
    risk_if_missing: "medium",
    approver: "legal",
    rationale: "Without warranties there is no contractual standard to measure failure against.",
  },
  {
    clause_type: "force_majeure",
    title: "Force majeure",
    preferred: "Narrow force majeure excluding events a reasonable business-continuity plan would mitigate (including cyber attacks and supplier failure); customer may terminate after 30 days.",
    fallback: "Standard force majeure with termination right after 60 days.",
    red_lines: ["Force majeure covering cyber attacks or the vendor's own subcontractors without a BCP obligation"],
    risk_if_missing: "low",
    approver: "legal",
    rationale: "Broad force majeure clauses quietly excuse the outages we most need protection from.",
  },
  {
    clause_type: "publicity",
    title: "Publicity",
    preferred: "No use of the customer's name, logo or the relationship in marketing without prior written consent.",
    fallback: "Vendor may list the customer name (no logo) in a customer list after written approval.",
    red_lines: ["Vendor may use customer name and logo freely"],
    risk_if_missing: "low",
    approver: "legal",
    rationale: "Brand use should always be opt-in.",
  },
  {
    clause_type: "exit_data_return",
    title: "Exit and data return",
    preferred: "On termination the vendor provides all customer data in a standard machine-readable format within 30 days at no extra cost, offers up to 6 months of transition assistance, then deletes the data and certifies deletion.",
    fallback: "Data export available for 30 days after termination; deletion within 90 days.",
    red_lines: ["Data deleted on termination with no export window", "Exit assistance charged at undefined rates"],
    risk_if_missing: "high",
    approver: "security",
    rationale: "Without a defined exit, switching vendors risks losing the data entirely.",
  },
  {
    clause_type: "non_solicitation",
    title: "Non-solicitation",
    preferred: "Mutual non-solicitation of key personnel for 12 months, excluding general advertising.",
    fallback: "Mutual non-solicitation for 6 months.",
    red_lines: ["One-way non-solicitation with a fee above 20% of salary"],
    risk_if_missing: "low",
    approver: "legal",
    rationale: "Keeps the clause balanced and proportionate.",
  },
];

export interface PlaybookMeta {
  clause_type: string;
  title: string;
  preferred: string;
  fallback: string;
  red_lines: string[];
  risk_if_missing: Risk;
  approver: Approver;
}

export const PLAYBOOK: SeedDoc[] = ENTRIES.map((e) => ({
  id: `pb-${e.clause_type}`,
  text: [
    `PLAYBOOK · ${e.title}`,
    `Preferred position: ${e.preferred}`,
    `Fallback position: ${e.fallback}`,
    `Red lines (escalate to ${e.approver}): ${e.red_lines.join("; ")}`,
    `Rationale: ${e.rationale}`,
  ].join("\n"),
  metadata: {
    clause_type: e.clause_type,
    title: e.title,
    preferred: e.preferred,
    fallback: e.fallback,
    red_lines: e.red_lines,
    risk_if_missing: e.risk_if_missing,
    approver: e.approver,
  },
}));

export const PLAYBOOK_BY_TYPE: Record<string, PlaybookMeta> = Object.fromEntries(
  PLAYBOOK.map((d) => [d.metadata.clause_type as string, d.metadata as unknown as PlaybookMeta])
);

export interface SampleContract {
  id: string;
  name: string;
  text: string;
}

export const SAMPLE_CONTRACTS: SampleContract[] = [
  {
    id: "northwind-msa",
    name: "Northwind Analytics — Master Subscription Agreement",
    text: `NORTHWIND ANALYTICS INC. — MASTER SUBSCRIPTION AGREEMENT

1. Definitions
"Customer Data" means all data uploaded to the Service by or on behalf of Customer. "Service" means the Northwind Insights analytics platform and related support. "Order Form" means each ordering document signed by the parties.

2. Subscription and Access
Northwind grants Customer a non-exclusive, non-transferable right to access the Service during the Subscription Term for its internal business purposes, subject to the user limits in the Order Form.

3. Fees and Payment
Customer shall pay the fees in the Order Form annually in advance within 30 days of invoice. Northwind may revise its fees at any time at its sole discretion by giving Customer 30 days' notice, and revised fees apply from the next invoice.

4. Term and Renewal
The initial Subscription Term is 12 months. Thereafter the Subscription Term shall automatically renew for successive periods of 36 months unless either party gives written notice of non-renewal at least 120 days before the end of the then-current term.

5. Customer Data
As between the parties Customer owns Customer Data. Customer grants Northwind a worldwide, perpetual, royalty-free licence to use, copy and process Customer Data to provide the Service and to train, develop and improve Northwind's machine learning models and products, including after termination.

6. Data Protection
Each party shall comply with applicable data protection laws. Northwind may engage sub-processors at its discretion to deliver the Service and will update the list on its website from time to time. Northwind shall notify Customer of a personal data breach affecting Customer Data within seven (7) days of confirming the breach.

7. Security
Northwind will use commercially reasonable measures to protect the Service and may modify its security practices from time to time provided they do not materially degrade overall protection.

8. Service Levels
Northwind targets 99% availability measured monthly. Service credits of 2% of the monthly fee per full hour below target are Customer's sole and exclusive remedy for any unavailability.

9. Warranties
Except as expressly stated, the Service is provided "as is" and Northwind disclaims all warranties, express or implied, including fitness for a particular purpose.

10. Limitation of Liability
Northwind's total aggregate liability arising out of or in connection with this Agreement shall not exceed the fees paid by Customer in the three (3) months preceding the event giving rise to the claim. Northwind shall not be liable for any loss or corruption of data. Customer's liability for breach of clause 2 and for payment of fees is unlimited.

11. Indemnities
Customer shall defend and indemnify Northwind against any claim arising from Customer Data or Customer's use of the Service.

12. Confidentiality
Each party shall keep the other's Confidential Information confidential during the term of this Agreement.

13. Termination
Either party may terminate for material breach not remedied within 30 days. On termination Customer's access ends immediately and Northwind may delete Customer Data without further notice after 7 days.

14. Assignment
Northwind may assign or transfer this Agreement, in whole or in part, without Customer's consent.

15. Governing Law
This Agreement is governed by the laws of the State of Delaware, and the parties submit to the exclusive jurisdiction of the state and federal courts located in Wilmington, Delaware.
`,
  },
  {
    id: "harbor-dpa",
    name: "Harbor CRM Ltd — Data Processing Agreement",
    text: `HARBOR CRM LTD — DATA PROCESSING AGREEMENT (UK GDPR)

1. Scope and Roles
This DPA forms part of the Harbor CRM subscription agreement. Customer is the controller and Harbor CRM Ltd is the processor of Customer Personal Data under Article 28 of the UK GDPR.

2. Processing Instructions
Harbor shall process Customer Personal Data only on Customer's documented instructions, including with regard to international transfers, and shall not process Customer Personal Data for its own purposes.

3. Sub-processors
Harbor maintains a list of sub-processors at harborcrm.example/subprocessors. Harbor shall give Customer at least 30 days' prior written notice of any new sub-processor. Customer may object on reasonable data protection grounds, and if the parties cannot resolve the objection Customer may terminate the affected services without penalty.

4. Security of Processing
Harbor maintains ISO 27001 certification, encrypts Customer Personal Data in transit and at rest using industry-standard encryption, and conducts an independent penetration test annually, sharing an executive summary on request.

5. Personal Data Breach
Harbor shall notify Customer without undue delay, and in any event within 48 hours, after becoming aware of a Personal Data Breach, and shall provide the information Customer reasonably needs to meet its notification obligations to the ICO.

6. International Transfers
Harbor shall not transfer Customer Personal Data outside the UK except under the UK International Data Transfer Agreement or the UK Addendum to the EU Standard Contractual Clauses.

7. Audit
Harbor shall make available its current ISO 27001 certificate and SOC 2 Type II report annually. Customer may conduct an on-site audit once per year on 30 days' notice, or at any time following a Personal Data Breach.

8. Machine Learning
Harbor shall not use Customer Personal Data, prompts or outputs to train or improve any machine learning model without Customer's prior written opt-in.

9. Return and Deletion
On termination Harbor shall, at Customer's choice, return all Customer Personal Data in CSV or JSON format within 30 days and then delete it, providing written certification of deletion.

10. Liability
Each party's liability under this DPA is subject to a cap equal to the greater of £2,000,000 and three times the annual fees paid under the subscription agreement.

11. Governing Law
This DPA is governed by the law of England and Wales, and the courts of England and Wales have exclusive jurisdiction.
`,
  },
];
