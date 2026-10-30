/**
 * Compliance Mapper — paste an internal control or policy statement;
 * NebulaDB finds the regulatory obligations it maps to across
 * frameworks; the chat writes a gap analysis. Obligations are short
 * plain-English paraphrases written for the demo, not regulatory text.
 */
import type { SeedDoc } from "../../rag/seed";
import type { UseCaseSpec } from "../UseCaseDemo";

const FRAMEWORKS = ["GDPR", "DORA", "EU AI Act", "NIS2", "PCI DSS", "SOC 2"] as const;

function ob(id: string, framework: string, ref: string, topic: string, body: string): SeedDoc {
  return {
    id,
    text: `${id} · ${framework} ${ref} · ${topic}\n${body}`,
    metadata: { obligation_id: id, framework, ref, topic },
  };
}

const DOCS: SeedDoc[] = [
  ob("OB-GDPR-01", "GDPR", "Art. 33", "Breach notification",
    "Notify the supervisory authority of a personal data breach without undue delay and, where feasible, within 72 hours of becoming aware, unless it is unlikely to result in a risk to individuals. Document every breach, including facts, effects and remedial action."),
  ob("OB-GDPR-02", "GDPR", "Art. 30", "Records of processing",
    "Maintain a record of processing activities: purposes, categories of data subjects and data, recipients, international transfers, retention periods and a general description of security measures."),
  ob("OB-GDPR-03", "GDPR", "Art. 35", "Impact assessment",
    "Carry out a data protection impact assessment before processing likely to result in high risk, such as large-scale profiling or systematic monitoring."),
  ob("OB-GDPR-04", "GDPR", "Art. 17", "Right to erasure",
    "Erase personal data without undue delay on valid request, including in backups on a reasonable schedule, and inform processors who hold copies."),
  ob("OB-DORA-01", "DORA", "Art. 19", "ICT incident reporting",
    "Financial entities classify ICT-related incidents and report major ones to the competent authority with an initial notification, an intermediate report and a final report."),
  ob("OB-DORA-02", "DORA", "Art. 28", "Third-party ICT risk",
    "Maintain a register of all contractual arrangements with ICT third-party providers, assess concentration risk, and ensure exit strategies for critical services."),
  ob("OB-DORA-03", "DORA", "Art. 24–25", "Resilience testing",
    "Run a risk-based digital operational resilience testing programme, including vulnerability assessments, scenario testing and, for significant entities, threat-led penetration testing."),
  ob("OB-AIA-01", "EU AI Act", "Art. 9", "Risk management (high-risk AI)",
    "Providers of high-risk AI systems establish a continuous risk management process across the lifecycle: identify foreseeable risks, evaluate them, and adopt mitigation measures."),
  ob("OB-AIA-02", "EU AI Act", "Art. 12", "Record-keeping and logging",
    "High-risk AI systems must automatically log events over their lifetime to enable traceability of operation and post-market monitoring."),
  ob("OB-AIA-03", "EU AI Act", "Art. 50", "Transparency to users",
    "People interacting with an AI system must be informed they are dealing with AI unless obvious; AI-generated synthetic content should be marked as such."),
  ob("OB-NIS2-01", "NIS2", "Art. 21", "Cybersecurity risk measures",
    "Essential and important entities implement measures including incident handling, business continuity and backups, supply-chain security, MFA, encryption and staff training."),
  ob("OB-NIS2-02", "NIS2", "Art. 23", "Incident early warning",
    "Give the CSIRT or authority an early warning within 24 hours of becoming aware of a significant incident, followed by a notification within 72 hours and a final report within one month."),
  ob("OB-PCI-01", "PCI DSS", "Req. 8", "Strong authentication",
    "Identify every user with a unique ID and require multi-factor authentication for all access into the cardholder data environment."),
  ob("OB-PCI-02", "PCI DSS", "Req. 10", "Audit logging",
    "Log all access to system components and cardholder data, protect logs from tampering, review them daily and retain at least one year of history."),
  ob("OB-SOC2-01", "SOC 2", "CC6.1", "Logical access",
    "Logical access to information assets is restricted through access provisioning, role-based permissions and periodic access reviews."),
  ob("OB-SOC2-02", "SOC 2", "CC7.3", "Incident evaluation",
    "Security events are evaluated to determine whether they are incidents, and incidents are responded to with a documented programme."),
];

export const complianceSpec: UseCaseSpec = {
  bucket: "compliance_obligations",
  blueprint: "compliance",
  seedVersion: "compliance-v1",
  docs: DOCS,
  noun: "obligations",
  title: "Compliance Mapper",
  subtitle: "Internal control → matching obligations across GDPR, DORA, EU AI Act, NIS2, PCI DSS, SOC 2 → gap analysis. Paraphrased, not legal text.",
  queryLabel: "Internal control or policy statement",
  placeholder: "Paste a control from your policy library…",
  scenarios: [
    { label: "Incident response control", q: "Security incidents are triaged by the on-call SRE and reported to the CISO within 48 hours; customers are told when the investigation closes." },
    { label: "Vendor register", q: "Procurement keeps a spreadsheet of SaaS vendors with contract renewal dates; no exit plan is documented." },
    { label: "Chatbot disclosure", q: "Our support assistant is an LLM; replies are sent under the agent's name.", filters: { framework: "EU AI Act" } },
    { label: "Admin MFA", q: "Engineers access production databases with SSO; MFA is optional for break-glass accounts." },
    { label: "Log retention", q: "Application logs are retained for 30 days in CloudWatch then deleted." },
  ],
  filters: [{ key: "framework", label: "Framework", kind: "select", options: FRAMEWORKS }],
  idKey: "obligation_id",
  titleKey: "topic",
  subKeys: ["framework", "ref"],
  resultsTitle: "Mapped obligations",
  chat: {
    title: "Gap analyst",
    subtitle: "Compares the control against the retrieved obligations",
    system:
      "You are a GRC analyst. Compare the user's internal control with each cited obligation [n]. " +
      "For each, state: covered / partially covered / gap, with one sentence of reasoning, and propose a concrete control change for every gap. " +
      "Use only the excerpts; the obligations are paraphrases, so recommend Legal confirm against the official text.",
    suggestions: [
      "Produce a gap table for the control I pasted.",
      "Which gap is highest priority and why?",
      "Rewrite the control so it covers all of these.",
    ],
    placeholder: "Ask about gaps or a rewrite…",
  },
};
