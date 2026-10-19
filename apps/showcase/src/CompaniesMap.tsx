/**
 * A Google Map of the companies in a Companies House search result.
 *
 * CH results carry a postal address but no coordinates, so each company's UK
 * postcode is geocoded (postcodes.io — free, no key) and pinned on the map.
 * Clicking a pin selects the company, exactly like clicking the list row.
 *
 * The map needs a Google Maps JS key in VITE_GOOGLE_MAPS_API_KEY (build-time);
 * without one the component degrades to a short hint instead of breaking.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { GoogleMap, useJsApiLoader, MarkerF, InfoWindowF } from "@react-google-maps/api";

export interface MapCompany {
  company_number?: string;
  title?: string;
  company_status?: string;
  address_snippet?: string;
  /** UK postcode, used to geocode the pin. */
  postcode?: string;
}

const MAPS_KEY = import.meta.env.VITE_GOOGLE_MAPS_API_KEY ?? "";
const UK_CENTRE = { lat: 54.0, lng: -2.5 };
const CONTAINER = { width: "100%", height: "20rem" } as const;

const norm = (p: string) => (p || "").toUpperCase().replace(/\s+/g, "");

/** Bulk-geocode UK postcodes via postcodes.io. Returns normalised-postcode -> {lat,lng}. */
async function geocodePostcodes(
  postcodes: string[],
  signal: AbortSignal,
): Promise<Record<string, { lat: number; lng: number }>> {
  const uniq = Array.from(new Set(postcodes.map((p) => p.trim()).filter(Boolean)));
  const out: Record<string, { lat: number; lng: number }> = {};
  for (let i = 0; i < uniq.length; i += 100) {
    // postcodes.io caps bulk lookups at 100 per request.
    const batch = uniq.slice(i, i + 100);
    try {
      const res = await fetch("https://api.postcodes.io/postcodes", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ postcodes: batch }),
        signal,
      });
      const body = (await res.json()) as {
        result?: Array<{ query: string; result: { latitude: number; longitude: number } | null }>;
      };
      for (const row of body.result ?? []) {
        if (row.result && row.result.latitude != null && row.result.longitude != null) {
          out[norm(row.query)] = { lat: row.result.latitude, lng: row.result.longitude };
        }
      }
    } catch {
      /* a failed batch just leaves those companies unpinned */
    }
  }
  return out;
}

interface Pin extends MapCompany {
  lat: number;
  lng: number;
}

export function CompaniesMap({
  companies,
  selected,
  onSelect,
}: {
  companies: MapCompany[];
  selected: string | null;
  onSelect: (companyNumber: string) => void;
}) {
  const { isLoaded } = useJsApiLoader({ id: "companies-house-map", googleMapsApiKey: MAPS_KEY });
  const [coords, setCoords] = useState<Record<string, { lat: number; lng: number }>>({});
  const [open, setOpen] = useState<string | null>(null);
  const mapRef = useRef<google.maps.Map | null>(null);

  // The postcodes to geocode, as a stable key so we only refetch when they change.
  const postcodeKey = useMemo(
    () => companies.map((c) => c.postcode ?? "").join("|"),
    [companies],
  );

  useEffect(() => {
    const ctrl = new AbortController();
    const postcodes = companies.map((c) => c.postcode).filter((p): p is string => !!p);
    if (postcodes.length === 0) {
      setCoords({});
    } else {
      void geocodePostcodes(postcodes, ctrl.signal).then((m) => setCoords(m));
    }
    return () => ctrl.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [postcodeKey]);

  const pins = useMemo<Pin[]>(() => {
    const out: Pin[] = [];
    for (const c of companies) {
      const ll = c.postcode ? coords[norm(c.postcode)] : undefined;
      if (ll) out.push({ ...c, ...ll });
    }
    return out;
  }, [companies, coords]);

  // Frame all the pins once they (and the map) are ready.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || pins.length === 0) return;
    if (pins.length === 1) {
      map.setCenter({ lat: pins[0].lat, lng: pins[0].lng });
      map.setZoom(13);
      return;
    }
    const bounds = new google.maps.LatLngBounds();
    for (const p of pins) bounds.extend({ lat: p.lat, lng: p.lng });
    map.fitBounds(bounds, 48);
  }, [pins]);

  if (!MAPS_KEY) {
    return (
      <p className="mt-3 text-xs opacity-70 border border-gray-200 dark:border-edge rounded px-3 py-2">
        Set <code>VITE_GOOGLE_MAPS_API_KEY</code> (Maps JavaScript API) to show the companies on a map.
      </p>
    );
  }
  if (!isLoaded) {
    return (
      <div className="mt-3 flex h-80 items-center justify-center rounded border border-gray-200 dark:border-edge text-sm opacity-70">
        Loading map…
      </div>
    );
  }

  const unmapped = companies.length - pins.length;
  return (
    <div className="mt-3">
      <GoogleMap
        mapContainerStyle={CONTAINER}
        center={pins[0] ? { lat: pins[0].lat, lng: pins[0].lng } : UK_CENTRE}
        zoom={pins.length ? 6 : 5}
        onLoad={(m) => {
          mapRef.current = m;
        }}
        onUnmount={() => {
          mapRef.current = null;
        }}
        options={{ streetViewControl: false, mapTypeControl: false }}
      >
        {pins.map((p) => (
          <MarkerF
            key={p.company_number}
            position={{ lat: p.lat, lng: p.lng }}
            onClick={() => {
              setOpen(p.company_number ?? null);
              if (p.company_number) onSelect(p.company_number);
            }}
            {...(selected && p.company_number === selected
              ? { zIndex: 999, animation: google.maps.Animation.BOUNCE }
              : {})}
          />
        ))}
        {open &&
          (() => {
            const p = pins.find((x) => x.company_number === open);
            if (!p) return null;
            return (
              <InfoWindowF position={{ lat: p.lat, lng: p.lng }} onCloseClick={() => setOpen(null)}>
                <div style={{ maxWidth: 220, color: "#111" }}>
                  <div style={{ fontWeight: 600 }}>{p.title}</div>
                  <div style={{ fontSize: 12, opacity: 0.8 }}>
                    {p.company_number} · {p.company_status}
                  </div>
                  {p.address_snippet && (
                    <div style={{ fontSize: 12, marginTop: 4 }}>{p.address_snippet}</div>
                  )}
                </div>
              </InfoWindowF>
            );
          })()}
      </GoogleMap>
      {unmapped > 0 && (
        <p className="mt-1 text-xs opacity-60">
          {unmapped} of {companies.length} not shown (no UK postcode to place them).
        </p>
      )}
    </div>
  );
}
