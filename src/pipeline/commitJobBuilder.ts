/**
 * 커밋 이벤트 → 스캔 작업(ScanJob)으로 바꾸는 순수 로직입니다.
 * Kafka와 무관해서 단위 테스트하기 좋고, 나중에 다른 입구(HTTP 등)에서도 재사용할 수 있습니다.
 */
import { randomUUID } from "crypto";
import type { CommitRawEvent, ScanJobEvent } from "./types";

/**
 * 한 번의 커밋 이벤트(여러 파일의 patch 문자열)를 "스캔 작업" 여러 개로 쪼갭니다.
 *
 * 왜 쪼개나요?
 * - patch 전체가 매우 길 수 있어서, 한 번에 Worker에 넣으면 메모리·시간 부담이 큽니다.
 * - 작은 덩어리(chunk)로 나누면 여러 Consumer/Worker가 병렬로 처리하기 좋습니다.
 *
 * @param event - Git 웹훅 등에서 받은 원시 커밋 정보
 * @param maxChunkSize - 한 작업에 실을 문자열 최대 길이
 */
export function buildScanJobsFromCommit(
  event: CommitRawEvent,
  maxChunkSize: number
): ScanJobEvent[] {
  const jobs: ScanJobEvent[] = [];
  const createdAt = new Date().toISOString();

  for (const file of event.changedFiles) {
    const content = file.patch ?? "";
    if (content.length === 0) {
      continue;
    }

    let offset = 0;
    let chunkIndex = 0;

    while (offset < content.length) {
      const chunk = content.slice(offset, offset + maxChunkSize);
      jobs.push({
        jobId: randomUUID(),
        repoId: event.repoId,
        commitSha: event.commitSha,
        filePath: file.path,
        chunkIndex,
        content: chunk,
        createdAt
      });
      chunkIndex += 1;
      offset += maxChunkSize;
    }
  }

  return jobs;
}

/**
 * 이미 파싱된 JSON 값(unknown)을 받아, CommitRawEvent 형태인지 검사하고 변환합니다.
 * Consumer에서는 JSON.parse 단계와 검증 단계를 나누어 DLQ에 기록하기 쉽게 합니다.
 */
export function validateCommitObject(parsed: unknown): CommitRawEvent {
  if (!parsed || typeof parsed !== "object") {
    throw new Error("커밋 이벤트는 객체(JSON object)여야 합니다.");
  }
  const obj = parsed as Record<string, unknown>;
  if (typeof obj.repoId !== "string" || obj.repoId.length === 0) {
    throw new Error("repoId가 없거나 문자열이 아닙니다.");
  }
  if (typeof obj.commitSha !== "string" || obj.commitSha.length === 0) {
    throw new Error("commitSha가 없거나 문자열이 아닙니다.");
  }
  if (!Array.isArray(obj.changedFiles)) {
    throw new Error("changedFiles는 배열이어야 합니다.");
  }

  const changedFiles = obj.changedFiles.map((item, index) => {
    if (!item || typeof item !== "object") {
      throw new Error(`changedFiles[${index}]는 객체여야 합니다.`);
    }
    const file = item as Record<string, unknown>;
    if (typeof file.path !== "string") {
      throw new Error(`changedFiles[${index}].path는 문자열이어야 합니다.`);
    }
    const patch = typeof file.patch === "string" ? file.patch : "";
    return { path: file.path, patch };
  });

  const receivedAt =
    typeof obj.receivedAt === "string" && obj.receivedAt.length > 0
      ? obj.receivedAt
      : new Date().toISOString();

  return {
    repoId: obj.repoId,
    commitSha: obj.commitSha,
    changedFiles,
    receivedAt
  };
}

/**
 * 문자열(JSON) 한 번에 파싱 + 검증까지 합니다. (테스트나 단순 호출에 편합니다.)
 */
export function parseCommitRawEvent(raw: string): CommitRawEvent {
  return validateCommitObject(JSON.parse(raw) as unknown);
}
