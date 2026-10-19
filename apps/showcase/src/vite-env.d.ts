/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Google Maps JavaScript API key for the Companies House map (optional). */
  readonly VITE_GOOGLE_MAPS_API_KEY?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
