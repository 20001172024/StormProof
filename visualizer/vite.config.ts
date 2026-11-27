import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// base "./" makes every asset URL relative, so the built site works from
// any path: GitHub Pages serves it under /StormProof/.
export default defineConfig({
  base: "./",
  plugins: [react()],
});
