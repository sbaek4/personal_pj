import { describe, expect, it } from "vitest";
import { buildScanJobsFromCommit, parseCommitRawEvent, validateCommitObject } from "./commitJobBuilder";

describe("commitJobBuilder", () => {
  it("커밋 이벤트를 chunk 크기에 맞게 여러 작업으로 나눕니다", () => {
    const event = {
      repoId: "r1",
      commitSha: "sha1",
      changedFiles: [{ path: "a.txt", patch: "0123456789" }],
      receivedAt: "t0"
    };
    const jobs = buildScanJobsFromCommit(event, 4);
    expect(jobs.length).toBe(3);
    expect(jobs[0].chunkIndex).toBe(0);
    expect(jobs[0].content).toBe("0123");
    expect(jobs[1].content).toBe("4567");
    expect(jobs[2].content).toBe("89");
  });

  it("validateCommitObject는 필수 필드가 없으면 에러를 던집니다", () => {
    expect(() => validateCommitObject({})).toThrow();
  });

  it("parseCommitRawEvent는 올바른 JSON 문자열을 객체로 바꿉니다", () => {
    const raw = JSON.stringify({
      repoId: "r",
      commitSha: "c",
      changedFiles: [{ path: "p", patch: "x" }],
      receivedAt: "t"
    });
    expect(parseCommitRawEvent(raw).repoId).toBe("r");
  });
});
