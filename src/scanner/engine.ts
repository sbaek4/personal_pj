/**
 * 문자열 안에서 "AWS 키처럼 보이는 패턴"을 찾는 엔진입니다.
 *
 * 중요한 개념:
 * - "정규식으로 걸렸다"가 곧 "진짜 비밀키다"는 뜻은 아닙니다. (오탐 가능)
 * - 실제 운영에서는 추가 검증(엔트로피, 주변 문맥, 금지 목록 등)을 붙이는 경우가 많습니다.
 */

export type SecretType = "aws-access-key-id" | "aws-secret-access-key";

export type SecretFinding = {
  position: number;
  type: SecretType;
  value: string;
};

export class ScannerEngine {
  private readonly patterns: Array<{ type: SecretType; regex: RegExp }> = [
    /** AWS Access Key ID는 보통 AKIA/ASIA로 시작하고 길이가 20인 경우가 많습니다. */
    { type: "aws-access-key-id", regex: /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/g },
    /** Secret Access Key는 Base64 비슷한 40자 문자열로 자주 표현됩니다(후보 탐지). */
    { type: "aws-secret-access-key", regex: /\b[A-Za-z0-9/+=]{40}\b/g }
  ];

  /**
   * 입력 문자열을 훑어서 매칭된 위치/타입/값을 배열로 돌려줍니다.
   * async인 이유: Kafka 워커 파이프라인에서 "CPU 작업을 다른 스레드로 넘기기" 쉽게 하려는 설계 흔적입니다.
   */
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
