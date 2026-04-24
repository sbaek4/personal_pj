# sample_1

TypeScript Node.js scanner service using Express, ESLint, Prettier, and Vitest.

## Project Structure

```text
src/
  index.ts               # Express API entrypoint
  index.test.ts          # API tests
  scanner/
    index.ts             # Scanner orchestration
    engine.ts            # Secret detection engine
    engine.test.ts       # Engine unit tests
```

## Requirements

- Node.js 20+
- npm 10+

## Scripts

- `npm run dev` - Run development server
- `npm run build` - Compile TypeScript to `dist/`
- `npm run start` - Run compiled build
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
- `feature`: active feature development
- `testing`: integration/staging validation
- `prod`: production-ready release branch

Suggested flow:
1. Branch from `feature` for work items (for example `feature/scanner-improvements`).
2. Merge into `testing` for combined validation.
3. Promote tested commits into `prod`.
4. Keep `master` aligned with your chosen source of truth.
