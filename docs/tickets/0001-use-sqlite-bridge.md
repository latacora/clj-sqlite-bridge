# Ticket: Migrate sqlite-cache to clj-sqlite-bridge

## Context

sqlite-cache currently embeds SQLite UDF bridging helpers inline. A dedicated
library now exists in `clj-sqlite-bridge` to provide this behavior.

## Goal

Replace sqlite-cache's internal UDF bridge implementation with a dependency on
`clj-sqlite-bridge` to reduce duplication and keep the behavior centralized.

## Tasks

- Add `com.latacora/clj-sqlite-bridge` as a dependency in sqlite-cache.
- Replace usages of `com.latacora.sqlite-cache.bridge` with
  `com.latacora.sqlite.bridge`.
- Delete the old bridge namespace from sqlite-cache once all callers are
  migrated.
- Ensure existing tests (and any new tests) pass.

## Acceptance Criteria

- sqlite-cache no longer defines its own UDF bridge namespace.
- All UDF functionality continues to work, including REGEXP support.
- CI passes with the new dependency.
