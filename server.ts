import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import express from "express";
import { createApiRouter } from "./server/routes.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FRONTEND_DIR = path.resolve(__dirname, "frontend");
const DIST_DIR = path.resolve(FRONTEND_DIR, "dist");

if (fs.existsSync(path.resolve(__dirname, ".env"))) {
  try {
    process.loadEnvFile(path.resolve(__dirname, ".env"));
  } catch {
    // Ignore malformed optional .env file
  }
}

async function startServer() {
  const app = express();
  app.use(express.json());
  app.use("/api", createApiRouter());

  const isProd = process.env.NODE_ENV === "production";
  if (!isProd) {
    const { createServer: createViteServer } = await import("vite");
    const vite = await createViteServer({
      root: FRONTEND_DIR,
      configFile: path.resolve(FRONTEND_DIR, "vite.config.ts"),
      server: {
        middlewareMode: true,
        host: "0.0.0.0",
        allowedHosts: true,
      },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    app.use(express.static(DIST_DIR));
    app.get("*", (_req, res) => {
      res.sendFile(path.resolve(DIST_DIR, "index.html"));
    });
  }

  const port = 3000;
  app.listen(port, "0.0.0.0", () => {
    console.log(`Keitaro Wrap server listening on http://0.0.0.0:${port}`);
  });
}

startServer().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
