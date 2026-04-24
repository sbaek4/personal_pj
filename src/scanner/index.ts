export type ScannerResult = {
  scannedAt: string;
  itemsProcessed: number;
  findings: Array<{
    position: number;
    type: "aws-access-key-id" | "aws-secret-access-key";
    value: string;
  }>;
  status: "ok";
};

import { ScannerEngine } from "./engine";

export async function runScanner(input = ""): Promise<ScannerResult> {
  const engine = new ScannerEngine();
  const findings = await engine.scan(input);

  return {
    scannedAt: new Date().toISOString(),
    itemsProcessed: input.length,
    findings,
    status: "ok"
  };
}
