/**
 * 학습용 샘플 Producer
 *
 * 실제 시스템에서는 GitHub 웹훅 서버 등이 `commits.raw`에 메시지를 넣습니다.
 * 로컬에서 Kafka가 떠 있을 때, 이 스크립트를 실행하면 테스트용 커밋 이벤트 한 건을 보낼 수 있습니다.
 *
 * 실행 순서 예시:
 * 1) Kafka 실행
 * 2) `npm run build`
 * 3) 터미널 A: `npm run start:ingest`  (commits.raw → scan.jobs)
 * 4) 터미널 B: `npm run start:scanner` (scan.jobs → scan.findings)
 * 5) 터미널 C: `npm run start:sample-producer` (이 파일)
 */

import { createKafkaClient } from "./kafkaConfig";
import { TOPICS } from "./topics";
import type { CommitRawEvent } from "./types";

const clientId = process.env.KAFKA_CLIENT_ID_SAMPLE_PRODUCER ?? "sample-commit-producer";

async function main(): Promise<void> {
  const kafka = createKafkaClient(clientId);
  const producer = kafka.producer();

  await producer.connect();

  const sampleCommit: CommitRawEvent = {
    repoId: "demo-repo",
    commitSha: "abc1234",
    changedFiles: [
      {
        path: "config.env",
        patch: "SECRET=AKIA1234567890ABCDEF\nOTHER=abcdEFGHijklMNOPqrstUVWXyz0123456789+/AB\n"
      }
    ],
    receivedAt: new Date().toISOString()
  };

  await producer.send({
    topic: TOPICS.commitsRaw,
    messages: [
      {
        key: `${sampleCommit.repoId}:${sampleCommit.commitSha}`,
        value: JSON.stringify(sampleCommit)
      }
    ]
  });

  console.log(`전송 완료: 토픽 "${TOPICS.commitsRaw}"에 샘플 커밋 1건을 넣었습니다.`);

  await producer.disconnect();
}

main().catch((error) => {
  console.error("샘플 Producer 실패:", error);
  process.exit(1);
});
