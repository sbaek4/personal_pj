/**
 * Kafka "토픽" 이름을 상수로 모아둔 것입니다.
 * 문자열을 여기저기 하드코딩하면 오타가 나기 쉬워서, 한 파일에서만 관리합니다.
 */
export const TOPICS = {
  /** Git 웹훅 등에서 받은 원시 커밋 이벤트가 들어오는 입력 토픽 */
  commitsRaw: "commits.raw",
  /** 실제 스캔(정규식 등)을 할 작업 단위 메시지 */
  scanJobs: "scan.jobs",
  /** 스캔 결과(발견된 비밀 등) */
  scanFindings: "scan.findings",
  /** 스캔 단계 실패·잘못된 job 페이로드 */
  scanDlq: "scan.dlq",
  /** 커밋 수집 단계에서 파싱/검증/작업 생성 실패 */
  commitsDlq: "commits.dlq"
} as const;
