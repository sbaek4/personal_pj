export type SecretType = "aws-access-key-id" | "aws-secret-access-key";

export type SecretFinding = {
  position: number;
  type: SecretType;
  value: string;
};

export class ScannerEngine {
  private readonly patterns: Array<{ type: SecretType; regex: RegExp }> = [
    // AWS access key IDs often start with AKIA/ASIA and are 20 chars long.
    { type: "aws-access-key-id", regex: /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/g },
    // Potential AWS secret access key candidate (40 base64-ish chars).
    { type: "aws-secret-access-key", regex: /\b[A-Za-z0-9/+=]{40}\b/g }
  ];

  public async scan(input: string): Promise<SecretFinding[]> {
    const findings: SecretFinding[] = [];

    for (const pattern of this.patterns) {
      for (const match of input.matchAll(pattern.regex)) {
        findings.push({
          position: match.index ?? -1,
          type: pattern.type,
          value: match[0]
        });
      }
    }

    return findings.sort((a, b) => a.position - b.position);
  }
}
