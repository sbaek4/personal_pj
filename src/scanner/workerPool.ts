/**
 * Worker Thread 풀(풀 = 미리 여러 개를 만들어 두고 돌려 쓰는 것)
 *
 * 왜 풀을 쓰나요?
 * - Worker를 "메시지마다 새로 만들고 종료"하면 생성/종료 비용이 커서 처리량이 떨어집니다.
 * - 그래서 프로세스 시작 시 Worker를 N개 만들어 두고, 작업이 올 때마다 비어 있는 Worker에 넘깁니다.
 *
 * 라운드 로빈(round-robin)이란?
 * - 작업을 Worker들에게 번갈아 가며 나눠주는 아주 단순한 방식입니다.
 */

import os from "os";
import path from "path";
import { randomUUID } from "crypto";
import { Worker } from "worker_threads";
import type { ScanFinding } from "../pipeline/types";

type WorkerRequest = {
  taskId: string;
  content: string;
};

type WorkerResponse = {
  taskId: string;
  findings?: ScanFinding[];
  error?: string;
};

type PendingTask = {
  resolve: (value: ScanFinding[]) => void;
  reject: (reason?: unknown) => void;
};

export class ScannerWorkerPool {
  private readonly workers: Worker[] = [];
  /** taskId → 아직 끝나지 않은 Promise의 resolve/reject를 저장합니다. */
  private readonly pending = new Map<string, PendingTask>();
  /** 다음에 일을 줄 Worker 인덱스 */
  private nextWorkerIndex = 0;

  constructor(workerCount = Math.max(1, os.cpus().length - 1)) {
    /** 컴파일 후에는 이 파일이 dist/scanner/workerPool.js 같은 위치에 생깁니다. */
    const workerPath = path.resolve(__dirname, "worker", "scanWorker.js");

    for (let i = 0; i < workerCount; i += 1) {
      const worker = new Worker(workerPath);
      worker.on("message", (message: WorkerResponse) => this.handleWorkerMessage(message));
      worker.on("error", (error) => {
        console.error("Scanner worker error:", error);
      });
      this.workers.push(worker);
    }
  }

  /**
   * 메인 스레드에서 호출하면, 내부적으로 Worker에게 메시지를 보내고
   * Worker가 끝내면 Promise가 resolve됩니다.
   */
  public scan(content: string): Promise<ScanFinding[]> {
    const taskId = randomUUID();
    const worker = this.workers[this.nextWorkerIndex];
    this.nextWorkerIndex = (this.nextWorkerIndex + 1) % this.workers.length;

    return new Promise<ScanFinding[]>((resolve, reject) => {
      this.pending.set(taskId, { resolve, reject });
      const payload: WorkerRequest = { taskId, content };
      worker.postMessage(payload);
    });
  }

  public async close(): Promise<void> {
    const terminations = this.workers.map((worker) => worker.terminate());
    await Promise.all(terminations);
    this.pending.clear();
  }

  private handleWorkerMessage(message: WorkerResponse): void {
    const pendingTask = this.pending.get(message.taskId);
    if (!pendingTask) {
      return;
    }

    this.pending.delete(message.taskId);

    if (message.error) {
      pendingTask.reject(new Error(message.error));
      return;
    }

    pendingTask.resolve(message.findings ?? []);
  }
}
