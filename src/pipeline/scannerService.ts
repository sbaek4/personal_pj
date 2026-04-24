/**
 * 스캐너(Scanner) 서비스 — Kafka Consumer + Producer + Worker Threads
 *
 * 흐름을 한 줄로 요약하면:
 *   scan.jobs(입력) → 워커 스레드에서 정규식 스캔 → scan.findings(성공) 또는 scan.dlq(실패)
 *
 * 왜 Worker Thread를 쓰나요?
 * - Node.js 메인 스레드는 I/O에는 강하지만, CPU를 많이 쓰는 작업(무거운 정규식 등)이 길게 붙잡히면
 *   Kafka heartbeat/처리 지연이 생길 수 있습니다.
 * - Worker Thread는 별도 스레드에서 CPU 작업을 돌려 메인 스레드를 비워 둡니다.
 *
 * backpressure(배압)이란?
 * - 들어오는 속도가 너무 빠르면 메모리가 터질 수 있어서, "지금은 잠깐 멈춰"라고 신호를 주는 것입니다.
 * - 여기서는 inFlight(처리 중인 작업 수)가 MAX_IN_FLIGHT를 넘으면 consumer.pause()로 읽기를 잠시 멈춥니다.
 */

import type { ScanErrorEvent, ScanJobEvent, ScanJobInvalidPayloadDlqEvent, ScanResultEvent } from "./types";
import { TOPICS } from "./topics";
import { createKafkaClient } from "./kafkaConfig";
import { ScannerWorkerPool } from "../scanner/workerPool";

const clientId = process.env.KAFKA_CLIENT_ID ?? "scanner-service";
const groupId = process.env.KAFKA_GROUP_ID ?? "scanner-workers";
/** 동시에 처리 중일 수 있는 최대 작업 수 (너무 크면 메모리 부담, 너무 작으면 처리량 저하) */
const maxInFlight = Number(process.env.MAX_IN_FLIGHT ?? 2000);
const workerCount = Number(process.env.SCANNER_WORKERS ?? 4);

export async function startScannerService(): Promise<void> {
  const kafka = createKafkaClient(clientId);
  const consumer = kafka.consumer({ groupId });
  const producer = kafka.producer();
  const workerPool = new ScannerWorkerPool(workerCount);

  let inFlight = 0;
  let paused = false;

  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: TOPICS.scanJobs, fromBeginning: false });

  /**
   * DLQ(Dead Letter Queue)로 실패 메시지를 보냅니다.
   * 운영에서는 DLQ를 모니터링해서 데이터 품질 문제를 잡습니다.
   */
  const sendScanDlqJson = async (payload: ScanErrorEvent | ScanJobInvalidPayloadDlqEvent): Promise<void> => {
    await producer.send({
      topic: TOPICS.scanDlq,
      messages: [{ key: "dlq", value: JSON.stringify(payload) }]
    });
  };

  const stop = async () => {
    await consumer.disconnect();
    await producer.disconnect();
    await workerPool.close();
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
    eachMessage: async ({ topic, partition, message, pause, heartbeat }) => {
      if (!message.value) {
        return;
      }

      if (inFlight >= maxInFlight && !paused) {
        pause();
        paused = true;
      }

      inFlight += 1;

      const raw = message.value.toString();

      try {
        let job: ScanJobEvent;
        try {
          job = JSON.parse(raw) as ScanJobEvent;
        } catch (error) {
          const dlqPayload: ScanJobInvalidPayloadDlqEvent = {
            dlqKind: "invalid_scan_job_payload",
            error: error instanceof Error ? error.message : "JSON 파싱 실패",
            rawSnippet: raw.slice(0, 2000),
            failedAt: new Date().toISOString()
          };
          await sendScanDlqJson(dlqPayload);
          return;
        }

        if (
          typeof job.jobId !== "string" ||
          typeof job.repoId !== "string" ||
          typeof job.commitSha !== "string" ||
          typeof job.filePath !== "string" ||
          typeof job.chunkIndex !== "number" ||
          typeof job.content !== "string"
        ) {
          const dlqPayload: ScanJobInvalidPayloadDlqEvent = {
            dlqKind: "invalid_scan_job_payload",
            error: "scan.job 필수 필드가 없거나 타입이 올바르지 않습니다.",
            rawSnippet: raw.slice(0, 2000),
            failedAt: new Date().toISOString()
          };
          await sendScanDlqJson(dlqPayload);
          return;
        }

        const findings = await workerPool.scan(job.content);

        const resultEvent: ScanResultEvent = {
          jobId: job.jobId,
          repoId: job.repoId,
          commitSha: job.commitSha,
          filePath: job.filePath,
          chunkIndex: job.chunkIndex,
          findings,
          scannedAt: new Date().toISOString()
        };

        await producer.send({
          topic: TOPICS.scanFindings,
          messages: [
            {
              key: `${job.repoId}:${job.commitSha}:${job.filePath}:${job.chunkIndex}`,
              value: JSON.stringify(resultEvent)
            }
          ]
        });
      } catch (error) {
        try {
          const job = JSON.parse(raw) as ScanJobEvent;
          const errorEvent: ScanErrorEvent = {
            jobId: job.jobId,
            repoId: job.repoId,
            commitSha: job.commitSha,
            filePath: job.filePath,
            chunkIndex: job.chunkIndex,
            error: error instanceof Error ? error.message : "Unknown scan failure",
            failedAt: new Date().toISOString()
          };

          await sendScanDlqJson(errorEvent);
        } catch {
          const dlqPayload: ScanJobInvalidPayloadDlqEvent = {
            dlqKind: "invalid_scan_job_payload",
            error: error instanceof Error ? error.message : "스캔 실패 후 DLQ 구성 중 파싱도 실패",
            rawSnippet: raw.slice(0, 2000),
            failedAt: new Date().toISOString()
          };
          await sendScanDlqJson(dlqPayload);
        }
      } finally {
        inFlight -= 1;
        await heartbeat();

        if (paused && inFlight < Math.floor(maxInFlight * 0.7)) {
          consumer.resume([{ topic, partitions: [partition] }]);
          paused = false;
        }
      }
    }
  });
}

if (require.main === module) {
  startScannerService().catch((error) => {
    console.error("Scanner service failed to start:", error);
    process.exit(1);
  });
}
