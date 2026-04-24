# sample_1

TypeScript Node.js scanner service using Express, ESLint, Prettier, and Vitest.

## What this project does (easy overview)

This repo is a **secret-pattern scanner** learning project. It looks for strings that **look like** AWS access key IDs and AWS secret access key candidates inside text (for example, a git patch or any string you pass in).

There are two ways to use it:

1. **HTTP API (Express)** — quick experiments without Kafka. Call `GET /scan?text=...` and get JSON back.
2. **Kafka pipeline** — closer to a real high-volume system. A **commit-like event** is published to Kafka, split into **scan jobs**, processed by **worker threads**, and results are published to another topic. Failures go to **dead-letter queues (DLQs)** so you can inspect or replay them later.

Important: a regex match is **not proof** of a real secret. Treat findings as **candidates** for review or further checks.

## Kafka in plain English

Kafka is a **message bus**: services **produce** events to named **topics**, and other services **consume** those events to do work.

- **Producer**: writes messages to a topic (for example, “new commit arrived”).
- **Consumer**: reads messages from a topic in a **consumer group** (multiple instances can share the load).
- **Topic**: a named stream of messages (this project uses `commits.raw`, `scan.jobs`, `scan.findings`, plus DLQ topics).
- **DLQ (dead-letter queue)**: a separate topic for messages that could not be processed normally (bad JSON, validation errors, scan errors). It keeps the main pipeline from getting stuck on poison messages.

In this repo, the flow is:

`commits.raw` → ingest service → `scan.jobs` → scanner service → `scan.findings`  
(with `commits.dlq` and `scan.dlq` for failures).

## How to run locally

### Prerequisites

- **Node.js** 20+ and **npm** 10+
- A running **Kafka** cluster reachable at `localhost:9092` (default), **or** set `KAFKA_BROKERS` to your broker list.

If your broker allows **auto topic creation**, topics may appear automatically when the apps first produce/consume. Otherwise, create the topics used in `src/pipeline/topics.ts` (`commits.raw`, `scan.jobs`, `scan.findings`, `commits.dlq`, `scan.dlq`) in your Kafka admin UI or CLI.

### Install and test (no Kafka)

```bash
npm install
npm run test
npm run lint
```

### Run the HTTP API only (no Kafka)

```bash
npm run dev
```

Then open or call:

- `http://localhost:3000/health`
- `http://localhost:3000/scan?text=hello-AKIA1234567890ABCDEF-world`

### Run the Kafka pipeline (three terminals)

Build first (the worker entrypoints run compiled JavaScript from `dist/`):

```bash
npm run build
```

**Terminal 1 — ingest** (reads `commits.raw`, writes `scan.jobs`):

```bash
npm run start:ingest
```

**Terminal 2 — scanner** (reads `scan.jobs`, writes `scan.findings`, uses worker threads):

```bash
npm run start:scanner
```

**Terminal 3 — sample producer** (publishes one demo commit to `commits.raw`):

```bash
npm run start:sample-producer
```

After the sample producer runs, you should see activity in terminals 1 and 2. To observe output topics, use your Kafka tooling (for example, `kafka-console-consumer` or a UI) subscribed to `scan.findings` or `scan.dlq` / `commits.dlq`.

### Production-style run (compiled HTTP server)

```bash
npm run build
npm run start
```

By default the HTTP server listens on port **3000**. Override with the `PORT` environment variable.

## Project Structure

```text
src/
  index.ts               # Express API entrypoint
  index.test.ts          # API tests
  pipeline/
    topics.ts            # Kafka topic names
    types.ts             # Event contracts
    kafkaConfig.ts       # Shared Kafka client factory
    commitJobBuilder.ts  # Commit event → scan jobs (pure functions)
    commitIngestService.ts # Consumer: commits.raw → Producer: scan.jobs (+ commits.dlq)
    sampleCommitProducer.ts # Learning sample: publishes one commit to commits.raw
    scannerService.ts    # Consumer: scan.jobs → Producer: scan.findings (+ scan.dlq)
    commitJobBuilder.test.ts
  scanner/
    index.ts             # Scanner orchestration
    engine.ts            # Secret detection engine
    engine.test.ts       # Engine unit tests
    workerPool.ts        # Worker thread pool
    worker/
      scanWorker.ts      # Worker execution file
```

## Requirements

- Node.js 20+
- npm 10+

## Scripts

- `npm run dev` - Run development server
- `npm run build` - Compile TypeScript to `dist/`
- `npm run start` - Run compiled build
- `npm run start:ingest` - Run commit ingest (reads `commits.raw`, writes `scan.jobs`)
- `npm run start:scanner` - Run scanner workers (reads `scan.jobs`, writes `scan.findings`)
- `npm run start:sample-producer` - Publish one demo message to `commits.raw` (local learning)
- `npm run lint` - Run ESLint
- `npm run format` - Run Prettier
- `npm run test` - Run test suite

## API

- `GET /health`
  - Returns service health status.
- `GET /scan?text=<content>`
  - Scans the `text` query string for potential AWS credentials.
  - Returns an object containing `findings` with:
    - `position`: index of the match in the input string
    - `type`: secret type (`aws-access-key-id` or `aws-secret-access-key`)
    - `value`: matched text

## Branching Model

- `master`: stable integration branch
- `features`: active feature development
- `testing`: integration/staging validation
- `prod`: production-ready release branch

Suggested flow:
1. Branch from `features` for work items (for example `features/scanner-improvements`).
2. Merge into `testing` for combined validation.
3. Promote tested commits into `prod`.
4. Keep `master` aligned with your chosen source of truth.

## Streaming Scanner Architecture

Kafka + Worker Threads pipeline:

1. **Producer (example)**: `sampleCommitProducer` writes a demo `CommitRawEvent` to `commits.raw`.
2. **Ingest consumer/producer**: `commitIngestService` reads `commits.raw`, splits patches into jobs, writes `scan.jobs`.
   - Parse/validation/produce failures go to **`commits.dlq`**.
3. **Scanner consumer/producer**: `scannerService` reads `scan.jobs`, scans in worker threads, writes `scan.findings`.
   - Invalid job payloads + scan failures go to **`scan.dlq`**.

See **How to run locally** above for the exact commands.

### Environment Variables

Shared:

- `KAFKA_BROKERS` (default: `localhost:9092`)

Ingest service:

- `KAFKA_CLIENT_ID_INGEST` (default: `commit-ingest-service`)
- `KAFKA_GROUP_ID_INGEST` (default: `commit-ingest-workers`)
- `COMMIT_PATCH_CHUNK_SIZE` (default: `50000`)

Scanner service:

- `KAFKA_CLIENT_ID` (default: `scanner-service`)
- `KAFKA_GROUP_ID` (default: `scanner-workers`)
- `SCANNER_WORKERS` (default: `4`)
- `MAX_IN_FLIGHT` (default: `2000`)

Sample producer:

- `KAFKA_CLIENT_ID_SAMPLE_PRODUCER` (default: `sample-commit-producer`)
