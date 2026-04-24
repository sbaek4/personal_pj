import request from "supertest";
import { describe, expect, it } from "vitest";
import { app } from "./index";

describe("API", () => {
  it("returns health status", async () => {
    const response = await request(app).get("/health");

    expect(response.status).toBe(200);
    expect(response.body).toEqual({ ok: true });
  });

  it("returns scanner result", async () => {
    const response = await request(app).get("/scan");

    expect(response.status).toBe(200);
    expect(response.body.status).toBe("ok");
    expect(response.body.itemsProcessed).toBe(0);
    expect(response.body.findings).toEqual([]);
    expect(typeof response.body.scannedAt).toBe("string");
  });

  it("scans query text for aws key patterns", async () => {
    const accessKey = "AKIA1234567890ABCDEF";
    const response = await request(app).get(`/scan?text=hello-${accessKey}-world`);

    expect(response.status).toBe(200);
    expect(response.body.findings).toEqual([
      {
        position: 6,
        type: "aws-access-key-id",
        value: accessKey
      }
    ]);
  });
});
