import express from "express";
import { runScanner } from "./scanner";

export const app = express();
const port = Number(process.env.PORT) || 3000;

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.get("/scan", async (_req, res) => {
  const result = await runScanner();
  res.json(result);
});

if (process.env.NODE_ENV !== "test") {
  app.listen(port, () => {
    console.log(`Server listening on http://localhost:${port}`);
  });
}
