/**
 * A Google Map of a use-case demo's search hits, one price-style chip per
 * hit, numbered by rank. Hits carry their own coordinates in metadata, so
 * there is no geocoding step. Selecting a chip or a list row highlights
 * the same hit in both places.
 *
 * Needs VITE_GOOGLE_MAPS_API_KEY at build time; without it the component
 * shows a one-line hint and the plain list still works.
 */
import { useEffect, useMemo, useRef } from "react";
import { GoogleMap, OverlayViewF, OverlayView, useJsApiLoader } from "@react-google-maps/api";

export interface MapHit {
  id: string;
  rank: number;
  lat: number;
  lng: number;
  label: string;
  title: string;
}

const MAPS_KEY = import.meta.env.VITE_GOOGLE_MAPS_API_KEY ?? "";
const UK_CENTRE = { lat: 53.0, lng: -1.8 };
const CONTAINER = { width: "100%", height: "20rem", borderRadius: "0.375rem" } as const;

// Centre the chip on its point instead of hanging it off the top-left corner.
const centreChip = (w: number, h: number) => ({ x: -w / 2, y: -h });

export function ResultsMap({
  hits,
  selected,
  onSelect,
}: {
  hits: MapHit[];
  selected: string | null;
  onSelect: (id: string) => void;
}) {
  // Same loader id as CompaniesMap so the Maps script is only loaded once.
  const { isLoaded } = useJsApiLoader({ id: "companies-house-map", googleMapsApiKey: MAPS_KEY });
  const mapRef = useRef<google.maps.Map | null>(null);
  const boundsKey = useMemo(() => hits.map((h) => `${h.lat},${h.lng}`).join("|"), [hits]);

  // Frame every hit whenever the result set changes.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || hits.length === 0) return;
    if (hits.length === 1) {
      map.setCenter({ lat: hits[0].lat, lng: hits[0].lng });
      map.setZoom(13);
      return;
    }
    const bounds = new google.maps.LatLngBounds();
    for (const h of hits) bounds.extend({ lat: h.lat, lng: h.lng });
    map.fitBounds(bounds, 56);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [boundsKey, isLoaded]);

  if (!MAPS_KEY) {
    return (
      <p className="mb-3 text-xs opacity-70 border border-gray-200 dark:border-edge rounded px-3 py-2">
        Set <code>VITE_GOOGLE_MAPS_API_KEY</code> (Maps JavaScript API) to show these results on a map.
      </p>
    );
  }
  if (!isLoaded) {
    return (
      <div className="mb-3 flex h-80 items-center justify-center rounded border border-gray-200 dark:border-edge text-sm opacity-70">
        Loading map…
      </div>
    );
  }

  return (
    <div className="mb-3">
      <GoogleMap
        mapContainerStyle={CONTAINER}
        center={hits[0] ? { lat: hits[0].lat, lng: hits[0].lng } : UK_CENTRE}
        zoom={6}
        onLoad={(m) => {
          mapRef.current = m;
          if (hits.length > 1) {
            const bounds = new google.maps.LatLngBounds();
            for (const h of hits) bounds.extend({ lat: h.lat, lng: h.lng });
            m.fitBounds(bounds, 56);
          }
        }}
        onUnmount={() => {
          mapRef.current = null;
        }}
        options={{ streetViewControl: false, mapTypeControl: false, clickableIcons: false }}
      >
        {hits.map((h) => {
          const on = h.id === selected;
          return (
            <OverlayViewF
              key={h.id}
              position={{ lat: h.lat, lng: h.lng }}
              mapPaneName={OverlayView.OVERLAY_MOUSE_TARGET}
              getPixelPositionOffset={centreChip}
              zIndex={on ? 999 : 100 - h.rank}
            >
              <button
                type="button"
                title={`${h.id} · ${h.title}`}
                onClick={() => onSelect(h.id)}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 4,
                  whiteSpace: "nowrap",
                  padding: "3px 8px 3px 3px",
                  borderRadius: 999,
                  border: `1px solid ${on ? "#4C8DFF" : "rgba(0,0,0,0.25)"}`,
                  background: on ? "#4C8DFF" : "#fff",
                  color: on ? "#fff" : "#111",
                  font: "600 12px/1.2 system-ui, sans-serif",
                  boxShadow: "0 1px 4px rgba(0,0,0,0.3)",
                  cursor: "pointer",
                  transform: on ? "scale(1.12)" : undefined,
                }}
              >
                <span
                  style={{
                    display: "inline-flex",
                    alignItems: "center",
                    justifyContent: "center",
                    width: 18,
                    height: 18,
                    borderRadius: 999,
                    background: on ? "#fff" : "#4C8DFF",
                    color: on ? "#4C8DFF" : "#fff",
                    fontSize: 11,
                  }}
                >
                  {h.rank}
                </span>
                {h.label}
              </button>
            </OverlayViewF>
          );
        })}
      </GoogleMap>
    </div>
  );
}
