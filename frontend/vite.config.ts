import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev, /api is proxied to the coordinator so the browser talks to one origin.
// In production the coordinator serves the built files from frontend/dist itself.
const target = process.env.COORDINATOR_URL ?? "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { "/api": { target, changeOrigin: true } },
  },
  test: { environment: "node", include: ["src/**/*.test.{ts,tsx}"] },
} as never);
