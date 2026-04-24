import { describe, expect, it } from "vitest";
import { ScannerEngine } from "./engine";

describe("ScannerEngine", () => {
  it("finds aws access key ids and secret access key candidates", async () => {
    const accessKey = "AKIA1234567890ABCDEF";
    const secretKey = "abcdEFGHijklMNOPqrstUVWXyz0123456789+/AB";
    const input = `prefix ${accessKey} middle ${secretKey} suffix`;
    const engine = new ScannerEngine();

    const result = await engine.scan(input);

    expect(result).toEqual([
      {
        position: input.indexOf(accessKey),
        type: "aws-access-key-id",
        value: accessKey
      },
      {
        position: input.indexOf(secretKey),
        type: "aws-secret-access-key",
        value: secretKey
      }
    ]);
  });

  it("returns empty findings when no pattern matches", async () => {
    const engine = new ScannerEngine();

    const result = await engine.scan("this is harmless sample text");

    expect(result).toEqual([]);
  });
});
