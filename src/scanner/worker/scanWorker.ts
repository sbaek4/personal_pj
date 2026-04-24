/**
 * Worker Thread에서 실행되는 파일입니다.
 *
 * parentPort란?
 * - 메인 스레드와 Worker 스레드가 메시지를 주고받는 "창구"입니다.
 * - postMessage로 보내고, on("message")로 받습니다.
 *
 * 이 Worker는 CPU 작업(정규식 스캔)을 메인 스레드 밖에서 처리합니다.
 */

import { parentPort } from "worker_threads";
import { ScannerEngine } from "../engine";
import type { ScanFinding } from "../../pipeline/types";

type WorkerRequest = {
  taskId: string;
  content: string;
};

type WorkerResponse = {
  taskId: string;
  findings?: ScanFinding[];
  error?: string;
};

/** Worker는 오래 살아 있으므로 엔진(정규식)을 한 번만 만들어 재사용합니다. */
const engine = new ScannerEngine();

if (!parentPort) {
  throw new Error("scanWorker must be run as a worker thread");
}

parentPort.on("message", async (message: WorkerRequest) => {
  try {
    const findings = await engine.scan(message.content);
    const response: WorkerResponse = { taskId: message.taskId, findings };
    parentPort?.postMessage(response);
  } catch (error) {
    const response: WorkerResponse = {
      taskId: message.taskId,
      error: error instanceof Error ? error.message : "Unknown worker error"
    };
    parentPort?.postMessage(response);
  }
});
