export type ScannerResult = {
  scannedAt: string;
  itemsProcessed: number;
  status: "ok";
};

export async function runScanner(): Promise<ScannerResult> {
  // Placeholder for core scan logic.
  return {
    scannedAt: new Date().toISOString(),
    itemsProcessed: 0,
    status: "ok"
  };
}
