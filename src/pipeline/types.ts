/**
 * Kafka 파이프라인에서 주고받는 "이벤트"의 TypeScript 타입 모음입니다.
 * 타입을 정해두면 Producer/Consumer가 같은 형식을 기대하게 되어 실수를 줄일 수 있습니다.
 */

export type CommitRawEvent = {
  repoId: string;
  commitSha: string;
  changedFiles: Array<{
    path: string;
    patch: string;
  }>;
  receivedAt: string;
};

export type ScanJobEvent = {
  jobId: string;
  repoId: string;
  commitSha: string;
  filePath: string;
  chunkIndex: number;
  content: string;
  createdAt: string;
};

export type SecretType = "aws-access-key-id" | "aws-secret-access-key";

export type ScanFinding = {
  position: number;
  type: SecretType;
  value: string;
};

export type ScanResultEvent = {
  jobId: string;
  repoId: string;
  commitSha: string;
  filePath: string;
  chunkIndex: number;
  findings: ScanFinding[];
  scannedAt: string;
};

/**
 * 스캔 단계에서 실패했을 때 scan.dlq 토픽으로 보내는 메시지입니다.
 * (정상 처리가 불가능한 메시지를 따로 모아두는 큐를 DLQ, Dead Letter Queue라고 부릅니다.)
 */
export type ScanErrorEvent = {
  jobId: string;
  repoId: string;
  commitSha: string;
  filePath: string;
  chunkIndex: number;
  error: string;
  failedAt: string;
};

/**
 * scan.jobs 메시지가 JSON이 아니거나 필수 필드가 없을 때 DLQ에 넣는 형태입니다.
 * 이때는 jobId 같은 정보가 없을 수 있어서 별도 타입으로 둡니다.
 */
export type ScanJobInvalidPayloadDlqEvent = {
  dlqKind: "invalid_scan_job_payload";
  error: string;
  rawSnippet?: string;
  failedAt: string;
};

/**
 * commits.raw를 읽다가 파싱/검증/작업 생성 중 실패했을 때 commits.dlq로 보냅니다.
 */
export type CommitIngestDlqEvent = {
  dlqKind: "commit_ingest";
  stage: "parse" | "validate" | "produce_jobs";
  error: string;
  rawSnippet?: string;
  failedAt: string;
};
