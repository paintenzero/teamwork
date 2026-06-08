import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Proxy /api (HTTP + WS) to the orchestrator so the browser is same-origin and
// never needs CORS — and, per the architecture, never talks to Redis directly.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:3001",
        changeOrigin: true,
        ws: true,
      },
    },
  },
});
