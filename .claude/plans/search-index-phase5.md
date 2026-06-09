# Search Index — Phase 5 polish plan (AI-3236 follow-up)

Status: **not started** — deferred out of PR #542. This file is a working note for whoever
picks up the polish items. The core feature (Phases 1–4) is in PR #542 on branch
`martinvasko-ai-3236-search-is-very-slow`.

Module: `src/keboola_mcp_server/search_index/`

---

## Reference: how rebuild works today (so the polish items make sense)

`builder.build_index` (builder.py:45) writes to `<db>.tmp`, then `storage.atomic_publish`
(storage.py:121) does `os.replace(tmp_path, final_path)` — an atomic POSIX rename.

Disk-usage timeline for a 9 MB old → 10 MB new rebuild:

| Step | On disk | Note |
|------|---------|------|
| fetch from API | 9 MB | nothing written yet |
| `with file_lock` | 9 MB | `fcntl.LOCK_EX` on `default.db.lock`, cross-process dedup |
| build `.db.tmp` | **9 + 10 = 19 MB peak** | new DB built alongside old |
| `os.replace(.db.tmp, .db)` | atomic swap; old inode unlinked | only moment of the "swap" |
| lock released | 10 MB | |

After the swap the old inode is unlinked from the directory immediately. If a concurrent
reader still has it open the kernel keeps the inode alive until that fd closes, then the OS
reclaims the space. `query.run_query` / `query.list_by_kinds` open+read+close within a single
call, so the overlap window is milliseconds. **Steady-state peak after publish = new size only.**

Freshness ("dirty flag") = the DB file's `mtime`. `storage.is_stale(path, ttl=30*60)`
(storage.py:94) compares `time.time() - mtime` against the TTL. After 30 min the next search
triggers a background rebuild (`lifecycle.ensure_index_built`) while the stale DB keeps
serving reads.

Crash safety: a build that dies mid-way leaves an orphan `.db.tmp`; the next build for the
same token deletes it (`if tmp_path.exists(): tmp_path.unlink()`, builder.py:70). The old
`default.db` is never touched until the atomic swap, so a failed build never corrupts the
serving copy.

---

## Phase 5 items

### 1. Orphan `.db.tmp` cleanup at startup

**Problem.** A crashed or killed build leaves `<root>/<project>/<token>/default.db.tmp` on
disk. It is only reclaimed when the *next* build for that exact token runs. A token that is
never used again leaks its tmp file indefinitely.

**Fix.** On server startup (or first `ensure_index_built` per process), sweep
`<root>/*/*/*.db.tmp` and unlink any tmp file with no active build task in `lifecycle._builds`.
Guard each unlink with the per-token `file_lock` so we never delete a tmp that a live build is
mid-write on.

**Where.** New `storage.sweep_orphan_tmp(root)` called from `server.py` startup, or lazily
from `lifecycle.ensure_index_built` once per process via a module-level `_swept` flag.

**Risk.** Low. Pure disk hygiene, no behavior change. Must respect the file lock to avoid
racing a concurrent build.

**Effort.** ~half day incl. tests (orphan present → removed; orphan with live lock → kept).

### 2. Use `built_at_iso` from `meta` instead of file `mtime` for staleness

**Problem.** `is_stale` reads `db_path.stat().st_mtime`. `mtime` is fragile — `touch`, backup
restores, `rsync`, `cp -p`, container image layering can all reset it, causing either a
perpetually-fresh stale index or a needless rebuild. The build already stamps an authoritative
`built_at_iso` into the `meta` table (`storage.init_schema`, storage.py:127).

**Fix.** Add `storage.read_built_at(db_path) -> datetime | None` (one `SELECT value FROM meta
WHERE key='built_at_iso'`). Change `is_stale` to prefer `built_at_iso` and fall back to `mtime`
only when the meta row is missing (older schema). Keep the missing-file → stale branch.

**Where.** `storage.is_stale` + a small read helper. Touches `lifecycle` only through the
existing `is_stale` call site, so no API change.

**Risk.** Low–medium. Opening the DB for a `meta` read on every staleness check adds a tiny
sqlite open per request — cache the value or accept the cost (the check already stats the file).
Measure before/after; if the open cost matters, memoize per `(path, mtime)`.

**Effort.** ~half day incl. tests (fresh meta → not stale; aged meta → stale; missing meta →
mtime fallback; `touch` on file does NOT flip freshness when meta is present).

### 3. (optional) Circuit breaker on repeated build failures

**Problem.** If a token's build keeps failing (API outage, disk full), every search reschedules
a rebuild, hammering the API on each request.

**Fix.** Track consecutive failures per `(project_id, token_hash)` in `lifecycle._builds`. After
N consecutive failures, skip scheduling for a cool-down window (e.g. 5 min). Reset on success.

**Where.** `lifecycle.ensure_index_built` + `_BuildState` (add `failures: int`,
`cooldown_until: datetime | None`).

**Risk.** Low. Pure scheduling guard; search still falls back to live during cooldown.

**Effort.** ~half day. This is RFC Phase 5 row "circuit breaker"; pairs with removing the live
fallback for healthy indexes.

---

## Explicitly NOT in this plan

- Deep config-body caching beyond what Phase 4 already does (already shipped in PR #542).
- Semantic objects / `search_semantic_context` — separate work item, separate Linear issue.
- Cross-replica shared index store.
- LRU eviction of the cache directory (track separately if disk pressure becomes real).

---

## RFC

The authoritative design doc is `feature_spec/search_index/RFC.md`. The rollout table there
lists Phase 5 as `future`: "Remove live-API fallback for indexed object types when the index is
healthy; add circuit breaker + observability metrics." Items 2 and 3 above feed that row.
