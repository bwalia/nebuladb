/**
 * Retail POD Desk — from the Source Supplies discovery call.
 *
 * Nationwide value-retail stores place ~30 small orders/day via their
 * facilities portal (Service Channel–style). Source accepts into Merlin,
 * ships by courier (DX / FedEx-style), then must attach TWO proofs of
 * delivery back to the portal: Merlin scan + courier photo. Dave estimated
 * ~£20k/year of staff time on that loop.
 *
 * Demo retailer is fictional "ValueMax Stores" — same workflow shape.
 * Nothing uploads for real; a person confirms each attach.
 */
import { useState } from "react";
import { Panel } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { Chips, DemoNote } from "./common";

type PodStatus = "awaiting_despatch" | "courier_out" | "ready_to_attach" | "attached";

interface WorkOrder {
  scRef: string;
  store: string;
  postcode: string;
  lines: string;
  merlinSo: string;
  courier: "DX" | "FedEx";
  consignment: string;
  merlinPod: boolean;
  courierPod: boolean;
  status: PodStatus;
}

const SEED_ORDERS: WorkOrder[] = [
  {
    scRef: "SC-884201",
    store: "ValueMax Camberley",
    postcode: "GU15",
    lines: "2× jumbo toilet roll, 1× hand towel",
    merlinSo: "SO-VM-10442",
    courier: "DX",
    consignment: "DX94822103",
    merlinPod: true,
    courierPod: true,
    status: "ready_to_attach",
  },
  {
    scRef: "SC-884218",
    store: "ValueMax Reading",
    postcode: "RG1",
    lines: "4× bin liners, 2× multi-purpose cleaner",
    merlinSo: "SO-VM-10451",
    courier: "FedEx",
    consignment: "FX77102944",
    merlinPod: true,
    courierPod: false,
    status: "courier_out",
  },
  {
    scRef: "SC-884233",
    store: "ValueMax Guildford",
    postcode: "GU1",
    lines: "1× floor cleaner, 3× soap refill",
    merlinSo: "SO-VM-10458",
    courier: "DX",
    consignment: "DX94823011",
    merlinPod: true,
    courierPod: true,
    status: "ready_to_attach",
  },
  {
    scRef: "SC-884240",
    store: "ValueMax Slough",
    postcode: "SL1",
    lines: "6× recycled hand towels",
    merlinSo: "—",
    courier: "DX",
    consignment: "—",
    merlinPod: false,
    courierPod: false,
    status: "awaiting_despatch",
  },
  {
    scRef: "SC-884155",
    store: "ValueMax Woking",
    postcode: "GU21",
    lines: "2× washroom spray, 1× gloves",
    merlinSo: "SO-VM-10401",
    courier: "FedEx",
    consignment: "FX77098812",
    merlinPod: true,
    courierPod: true,
    status: "attached",
  },
];

const SCENARIOS = [
  { label: "Today's dual-POD queue", focus: "ready" as const },
  { label: "New portal orders → Merlin", focus: "intake" as const },
  { label: "Full day's board", focus: "all" as const },
];

const STATUS_LABEL: Record<PodStatus, string> = {
  awaiting_despatch: "Accept → Merlin",
  courier_out: "Waiting courier POD",
  ready_to_attach: "Ready: attach both PODs",
  attached: "Done on portal",
};

export function ServiceChannelTab() {
  const [scenario, setScenario] = useState(SCENARIOS[0].label);
  const [focus, setFocus] = useState<"ready" | "intake" | "all">(SCENARIOS[0].focus);
  const [orders, setOrders] = useState(SEED_ORDERS);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [log, setLog] = useState<string[]>([]);

  const visible = orders.filter((o) => {
    if (focus === "ready") return o.status === "ready_to_attach";
    if (focus === "intake") return o.status === "awaiting_despatch";
    return true;
  });

  const runBoard = () => {
    const ready = orders.filter((o) => o.status === "ready_to_attach");
    const intake = orders.filter((o) => o.status === "awaiting_despatch");
    const waiting = orders.filter((o) => o.status === "courier_out");
    setSteps([
      {
        label: "Pull open work orders from retail portal",
        detail: `${orders.length} demo ValueMax / Service Portal tickets (≈30/day in production)`,
        rowsOut: orders.length,
        status: "ok",
      },
      {
        label: "Match Merlin sales orders + courier consignments",
        detail: "Consignment number links Merlin scan POD ↔ courier photo POD",
        rowsOut: orders.filter((o) => o.merlinSo !== "—").length,
        status: "ok",
      },
      {
        label: "Dual-POD attach queue",
        detail: `${ready.length} ready · ${waiting.length} waiting courier · ${intake.length} still to accept into Merlin`,
        rowsOut: ready.length,
        status: "ok",
      },
    ]);
    setLog([
      `Board refreshed. ${ready.length} work orders can have both PODs attached to the portal now.`,
      `Estimated staff time avoided if automated: ~£20k/year (discovery-call figure for this nationwide retail loop).`,
    ]);
  };

  const acceptIntoMerlin = (scRef: string) => {
    setOrders((prev) =>
      prev.map((o) =>
        o.scRef === scRef && o.status === "awaiting_despatch"
          ? {
              ...o,
              merlinSo: `SO-VM-${10000 + Math.floor(Math.random() * 900)}`,
              consignment: `${o.courier === "DX" ? "DX" : "FX"}${94820000 + Math.floor(Math.random() * 9000)}`,
              status: "courier_out",
            }
          : o
      )
    );
    setLog((l) => [`${scRef}: accepted → draft Merlin SO created (demo). Courier booked.`, ...l].slice(0, 8));
  };

  const fetchCourierPod = (scRef: string) => {
    setOrders((prev) =>
      prev.map((o) =>
        o.scRef === scRef && o.status === "courier_out"
          ? { ...o, courierPod: true, status: "ready_to_attach" }
          : o
      )
    );
    setLog((l) => [`${scRef}: courier POD image fetched by consignment (demo).`, ...l].slice(0, 8));
  };

  const attachBoth = (scRef: string) => {
    setOrders((prev) =>
      prev.map((o) =>
        o.scRef === scRef && o.status === "ready_to_attach"
          ? { ...o, status: "attached" }
          : o
      )
    );
    setLog((l) => [
      `${scRef}: Merlin scan + courier photo attached to Service Portal (demo — person confirmed).`,
      ...l,
    ].slice(0, 8));
  };

  const readyCount = orders.filter((o) => o.status === "ready_to_attach").length;

  return (
    <div className="space-y-4">
      <DemoNote extra="Models the nationwide retail portal + dual-POD attach workflow from the Source Supplies discovery call (~£20k/yr pain). Fictional ValueMax account — not a real retailer." />
      <ArchitecturePanel id="ss_retail_pod" />
      <Panel
        title="Retail POD Desk"
        subtitle="Portal order → Merlin → courier → attach Merlin scan + courier photo back to the portal."
      >
        <Chips
          items={SCENARIOS}
          active={scenario}
          onPick={(s) => {
            setScenario(s.label);
            setFocus(s.focus);
          }}
        />
        <button type="button" className="btn" onClick={runBoard}>
          Refresh dual-POD board
        </button>
        <p className="mt-2 text-xs text-gray-500 dark:text-muted">
          {readyCount} ready to attach · {orders.filter((o) => o.status !== "attached").length} still open
        </p>
      </Panel>

      {steps.length > 0 && <PipelineTrace steps={steps} />}

      <Panel title="Work orders" subtitle="Two PODs required: Merlin scan + courier delivery photo">
        <div className="overflow-x-auto -mx-1">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs uppercase tracking-wider text-gray-500 dark:text-muted border-b border-gray-200 dark:border-edge">
                <th className="py-2 pr-3">Portal</th>
                <th className="py-2 pr-3">Store</th>
                <th className="py-2 pr-3">Lines</th>
                <th className="py-2 pr-3">Merlin</th>
                <th className="py-2 pr-3">Courier</th>
                <th className="py-2 pr-3">PODs</th>
                <th className="py-2 pr-3">Status</th>
                <th className="py-2">Action</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((o) => (
                <tr key={o.scRef} className="border-b border-gray-100 dark:border-edge/60 align-top">
                  <td className="py-2 pr-3 font-mono text-xs">{o.scRef}</td>
                  <td className="py-2 pr-3">
                    {o.store}
                    <div className="text-[11px] text-gray-500 dark:text-muted">{o.postcode}</div>
                  </td>
                  <td className="py-2 pr-3 text-xs">{o.lines}</td>
                  <td className="py-2 pr-3 font-mono text-xs">{o.merlinSo}</td>
                  <td className="py-2 pr-3 text-xs">
                    {o.courier}
                    <div className="font-mono text-[11px]">{o.consignment}</div>
                  </td>
                  <td className="py-2 pr-3 text-xs">
                    <div>
                      <span className={`dot ${o.merlinPod ? "dot-ok" : "dot-idle"}`} /> Merlin
                    </div>
                    <div>
                      <span className={`dot ${o.courierPod ? "dot-ok" : "dot-idle"}`} /> Courier
                    </div>
                  </td>
                  <td className="py-2 pr-3 text-xs">
                    <span className={o.status === "ready_to_attach" ? "text-warn" : ""}>{STATUS_LABEL[o.status]}</span>
                  </td>
                  <td className="py-2">
                    {o.status === "awaiting_despatch" && (
                      <button type="button" className="btn text-xs !py-1 !px-2" onClick={() => acceptIntoMerlin(o.scRef)}>
                        Accept → Merlin
                      </button>
                    )}
                    {o.status === "courier_out" && (
                      <button type="button" className="btn text-xs !py-1 !px-2" onClick={() => fetchCourierPod(o.scRef)}>
                        Fetch courier POD
                      </button>
                    )}
                    {o.status === "ready_to_attach" && (
                      <button type="button" className="btn text-xs !py-1 !px-2" onClick={() => attachBoth(o.scRef)}>
                        Attach both PODs
                      </button>
                    )}
                    {o.status === "attached" && <span className="text-xs text-ok">Attached</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>

      {log.length > 0 && (
        <Panel title="Jordan actions" subtitle="Demo only — nothing reaches a live portal">
          <ul className="space-y-1 text-sm text-gray-700 dark:text-muted">
            {log.map((line, i) => (
              <li key={i}>· {line}</li>
            ))}
          </ul>
        </Panel>
      )}

      <UnderTheHood filter={["retail portal", "Merlin SO", "courier POD"]} />
    </div>
  );
}
