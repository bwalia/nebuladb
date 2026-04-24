import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev-mode proxy: the React app and NebulaDB are on different
// origins in dev, so we let Vite forward `/api/*` to the server
// rather than fighting CORS. In production the nginx layer in
// `Dockerfile` does the same forwarding, so the app code uses
// the same relative URLs in both modes.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: process.env.NEBULA_TARGET || "http://localhost:8080",
        changeOrigin: true,
      },
      "/healthz": {
        target: process.env.NEBULA_TARGET || "http://localhost:8080",
        changeOrigin: true,
      },
      "/metrics": {
        target: process.env.NEBULA_TARGET || "http://localhost:8080",
        changeOrigin: true,
      },
    },
  },
});
