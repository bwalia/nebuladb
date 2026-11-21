/**
 * Sites shown on the Engagement page, and the bucket each one's events are in.
 *   showcase_events  customer demo sites on the showcase pods (njs/analytics.js `event`, signed-in visitors)
 *   web_events       other websites carrying the pixel (px.nebuladb.net/px.js, njs `pxGif`/`pxPost`)
 * A pixel site must also be in showcase.pixel.sites (Helm), which holds its public key and allowed origins.
 */
export interface EngagementSite {
  host: string;
  label: string;
  bucket: "showcase_events" | "web_events";
}

export const SITES: EngagementSite[] = [
  { host: "sourcesupplies.nebuladb.net", label: "Source Supplies", bucket: "showcase_events" },
  { host: "www.workstation.co.uk", label: "Workstation", bucket: "web_events" },
  { host: "shop.workstation.co.uk", label: "Workstation Shop", bucket: "web_events" },
  { host: "shop.workstation.be", label: "Workstation Shop BE", bucket: "web_events" },
  { host: "www.workstationai.de", label: "Workstation AI DE", bucket: "web_events" },
  { host: "www.workstationai.nl", label: "Workstation AI NL", bucket: "web_events" },
  { host: "www.workstationai.pt", label: "Workstation AI PT", bucket: "web_events" },
  { host: "www.workstationai.ee", label: "Workstation AI EE", bucket: "web_events" },
  { host: "www.workstationai.us", label: "Workstation AI US", bucket: "web_events" },
  { host: "shop.workstationai.us", label: "Workstation Shop US", bucket: "web_events" },
];

export const BUCKETS = [...new Set(SITES.map((s) => s.bucket))];
