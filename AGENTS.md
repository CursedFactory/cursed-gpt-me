# AGENTS.md

Lightweight guidance for agentic coding in this early-stage repo.

## Project Context

- Repo: `cursed-gpt-me`
- Current maturity: early prototype
- Goal: collect/store social-style conversation data (starting with Discord-like data) for LLM training experiments
- Active code today: datastore only
- Runtime: Bun + TypeScript
- Database: SQLite via Prisma (`@prisma/adapter-libsql`)

## Layout

- `README.md` - brief top-level description
- `datastore/package.json` - workspace-level scripts
- `datastore/datastore-core/` - core datastore package
- `datastore/datastore-core/prisma/schema.prisma` - schema/models
- `datastore/datastore-core/src/db.ts` - Prisma client setup
- `datastore/datastore-core/src/index.ts` - exported datastore APIs

## Current Tooling Reality

- No root `package.json`
- No lint config/scripts yet
- No test config/scripts yet
- No CI or `tsconfig*.json` files yet
- No Cursor/Copilot policy files present

Do not assume Jest/Vitest/ESLint/Prettier unless added to repo.

## Setup And Dev Commands

Run these from `datastore/`:

- Install deps: `bun install`
- Prisma generate (core): `bun run core:generate`
- Prisma migrate (core): `bun run core:migrate`
- Prisma Studio (core): `bun run core:studio`
- Run CLI package script: `bun run cli`

## Build / Lint / Test

### Build

- No explicit build script exists yet.
- Current flow is Bun-driven direct TS execution.

### Lint

- No lint script exists yet.
- If added, expose it in `datastore/package.json` (for example `bun run lint`).

### Test

- No tests are checked in yet.
- If Bun test is introduced, standard commands should be:
  - All tests: `bun test`
  - Single file: `bun test path/to/file.test.ts`
  - Single case: `bun test path/to/file.test.ts -t "case name"`
- Single-test requirement: `bun test <test-file>` and `bun test <test-file> -t "<name-pattern>"`.

## Prisma/DB Notes

- `schema.prisma` datasource uses `env("DATABASE_URL")` and SQLite provider.
- `src/db.ts` computes a local default DB path under `prisma/dev.db`.
- Runtime behavior: use `DATABASE_URL` when provided, otherwise local file DB fallback.
- After schema changes: run generate + migrate.

## Code Style Guide (Inferred From Existing Code)

### Imports

- ESM only (`import`/`export`)
- Prefer named exports
- Group imports: external, Node, then local
- Use explicit relative local paths

### Formatting

- 2-space indentation
- Semicolons required
- Keep quote style consistent (double quotes in current files)
- Use trailing commas in multiline literals/calls

### Types

- Add explicit types on exported/public APIs
- Prefer unions for constrained string domains
- Prefer object params for multi-argument APIs
- Avoid `any`; use concrete or inferred TS/Prisma types

### Naming

- Functions/vars: `camelCase`
- Types: `PascalCase`
- Use descriptive domain names (`prompt`, `message`, `stats`)

### Error Handling

- Do not swallow DB errors
- Propagate low-level errors unless wrapping adds real context
- If wrapping, preserve original cause and actionable message
- Ensure DB client disconnect is available in shutdown paths

### Data Access

- Keep Prisma client construction centralized in `src/db.ts`
- Keep data operations in datastore-core API layer
- Use explicit Prisma query fields (`where`, `orderBy`, `take`, `include`)
- Keep safe/default limits in listing APIs

## Cursor / Copilot Rules Check

- `.cursor/rules/`
- `.cursorrules`
- `.github/copilot-instructions.md`

- No Cursor rule files found
- No Copilot instructions file found

If these files are later added, treat them as repo policy and update this document.
