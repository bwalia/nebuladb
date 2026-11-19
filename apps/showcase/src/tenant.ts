/**
 * Which site this host serves. The showcase image serves both the combined
 * internal showcase and per-customer demo sites; a customer site is picked
 * by its host (sourcesupplies.nebuladb.net, or an env-prefixed variant
 * like int-sourcesupplies.…). For local testing: `?site=sourcesupplies`
 * (remembered for the tab) and `?site=showcase` to go back.
 */
export type Site = "showcase" | "sourcesupplies";

const KEY = "nebula-site";

export function currentSite(loc: Pick<Location, "hostname" | "search"> = window.location): Site {
  const param = new URLSearchParams(loc.search).get("site");
  try {
    if (param === "sourcesupplies" || param === "showcase") sessionStorage.setItem(KEY, param);
  } catch {
    /* storage blocked: the param still works for this load */
  }
  if (/^([a-z]+-)?sourcesupplies\./.test(loc.hostname)) return "sourcesupplies";
  let stored: string | null = null;
  try {
    stored = sessionStorage.getItem(KEY);
  } catch {
    stored = null;
  }
  return (param ?? stored) === "sourcesupplies" ? "sourcesupplies" : "showcase";
}

let customer: boolean | undefined;
/**
 * True on a customer demo site. Those pages hide the engineering detail
 * (SQL text, EXPLAIN plans, prompts, step timings, scoring debug) that the
 * internal showcase shows; the results and business outcome stay.
 */
export function isCustomerSite(): boolean {
  if (customer === undefined) customer = typeof window !== "undefined" && currentSite() !== "showcase";
  return customer;
}
