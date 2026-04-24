/**
 * 커밋 수집(ingest) 서비스
 *
 * 역할 요약:
 * 1) Consumer: Kafka 토픽 `commits.raw`에서 "원시 커밋 이벤트"를 읽습니다.
 * 2) 변환: 각 파일의 patch 문자열을 일정 크기로 잘라 `scan.jobs`에 넣을 작업 메시지로 만듭니다.
 * 3) Producer: 변환된 작업들을 `scan.jobs` 토픽으로 보냅니다.
 *
 * 실패하면?
 * - JSON 파싱 실패, 필드 검증 실패, Kafka 전송 실패 등은 `commits.dlq`(Dead Letter Queue)로 보냅니다.
 * - DLQ에 넣어두면 나중에 사람이 보거나, 재처리 배치를 돌리기 좋습니다.
 */

import type { CommitIngestDlqEvent } from "./types";
import { TOPICS } from "./topics";
import { createKafkaClient } from "./kafkaConfig";
import { buildScanJobsFromCommit, validateCommitObject } from "./commitJobBuilder";

const clientId = process.env.KAFKA_CLIENT_ID_INGEST ?? "commit-ingest-service";
const groupId = process.env.KAFKA_GROUP_ID_INGEST ?? "commit-ingest-workers";
/** patch 한 덩어리의 최대 길이 (문자 수). 환경 변수로 조절할 수 있습니다. */
const maxChunkSize = Number(process.env.COMMIT_PATCH_CHUNK_SIZE ?? 50_000);

export async function startCommitIngestService(): Promise<void> {
  const kafka = createKafkaClient(clientId);
  const consumer = kafka.consumer({ groupId });
  const producer = kafka.producer();

  await consumer.connect();
  await producer.connect();
  /** 처음부터 읽지 않고, 연결 이후에 들어오는 새 메시지부터 읽는 것이 일반적입니다. */
  await consumer.subscribe({ topic: TOPICS.commitsRaw, fromBeginning: false });

  const sendCommitDlq = async (event: CommitIngestDlqEvent): Promise<void> => {
    await producer.send({
      topic: TOPICS.commitsDlq,
      messages: [
        {
          key: event.failedAt,
          value: JSON.stringify(event)
        }
      ]
    });
  };

  const stop = async () => {
    await consumer.disconnect();
    await producer.disconnect();
  };

  process.on("SIGINT", async () => {
    await stop();
    process.exit(0);
  });
  process.on("SIGTERM", async () => {
    await stop();
    process.exit(0);
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString() ?? "";

      if (raw.length === 0) {
        await sendCommitDlq({
          dlqKind: "commit_ingest",
          stage: "parse",
          error: "메시지 본문(value)이 비어 있습니다.",
          failedAt: new Date().toISOString()
        });
        return;
      }

      let commitEvent;
      try {
        /** 1단계: 문자열을 JSON으로 바꿉니다. 문법이 틀리면 SyntaxError가 납니다. */
        const parsed: unknown = JSON.parse(raw);
        /** 2단계: 필수 필드(repoId, commitSha, changedFiles 등)가 있는지 검사합니다. */
        commitEvent = validateCommitObject(parsed);
      } catch (error) {
        const messageText = error instanceof Error ? error.message : "알 수 없는 오류";
        const stage = error instanceof SyntaxError ? "parse" : "validate";
        await sendCommitDlq({
          dlqKind: "commit_ingest",
          stage,
          error: messageText,
          rawSnippet: raw.slice(0, 2000),
          failedAt: new Date().toISOString()
        });
        return;
      }

      const jobs = buildScanJobsFromCommit(commitEvent, maxChunkSize);

      if (jobs.length === 0) {
        /** 스캔할 내용이 없으면 scan.jobs에 아무 것도 안내도 됩니다. (정상 케이스일 수 있음) */
        return;
      }

      try {
        /**
         * KafkaJS는 한 번에 여러 메시지를 묶어 보낼 수 있습니다.
         * 네트워크 왕복을 줄이려면 배치 전송이 유리합니다.
         */
        await producer.send({
          topic: TOPICS.scanJobs,
          messages: jobs.map((job) => ({
            /** 같은 저장소·커밋의 작업이 같은 파티션으로 가면 순서 관리가 쉬울 때가 있습니다. */
            key: `${job.repoId}:${job.commitSha}:${job.filePath}:${job.chunkIndex}`,
            value: JSON.stringify(job)
          }))
        });
      } catch (error) {
        const messageText = error instanceof Error ? error.message : "Kafka 전송 실패";
        await sendCommitDlq({
          dlqKind: "commit_ingest",
          stage: "produce_jobs",
          error: messageText,
          rawSnippet: raw.slice(0, 500),
          failedAt: new Date().toISOString()
        });
      }
    }
  });
}

if (require.main === module) {
  startCommitIngestService().catch((error) => {
    console.error("커밋 수집 서비스 시작 실패:", error);
    process.exit(1);
  });
}
