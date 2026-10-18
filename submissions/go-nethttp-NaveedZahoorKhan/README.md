# go-nethttp-NaveedZahoorKhan

A feed API on SQLite, tuned for the $12 box (1 vCPU, 2 GB).

## Stack

| | |
|---|---|
| Language | Go 1.24.4 (pinned in `install.sh`, verified by SHA-256) |
| HTTP | the standard library `net/http` server, HTTP/1.1 keep-alive |
| SQLite driver | `github.com/mattn/go-sqlite3` v1.14.22 (cgo) |
| SQLite | the amalgamation vendored **inside** go-sqlite3, compiled from source by `build.sh` — so the SQLite version is pinned by the driver, not by whatever `libsqlite3` the box happens to have |
| Nginx or direct | **Direct** — the server binds `0.0.0.0:80` and handles keep-alive connections itself (no Nginx). See "Direct, not Nginx" below. |

Dependencies are vendored (`vendor/`), so `build.sh` needs no network and always
compiles the same code.

## Why it's shaped this way

The load is overwhelmingly reads (a feed read and a post read every loop; a like
~15% of loops; a new post ~2%). The box has one core, so the wins are: do as
little work per request as possible, let reads run concurrently, and never block
the one core on anything avoidable.

**Reader pool, concurrent under WAL.** A fixed set of read-only SQLite
connections (`READERS`, default 4) sits in a buffered channel. A request takes a
connection, runs its query, and returns it. WAL lets these readers run at the
same time without blocking each other or the writer. Each connection keeps its
own prepared statements for the two read queries (`/feed`, `/posts/:id`) and the
post-exists check, so the query plan is compiled once per connection and reused
— that's a plan cache, not a data cache (rule 5). The feed query is the exact
projection the spec shows, ordered by the covering index
`posts_created_at_id_idx`.

**One writer, durable before responding.** WAL allows a single writer, so writes
go through one dedicated connection guarded by a mutex. Each `POST` waits for its
own statement to finish (and therefore its commit) before the handler returns,
so a 201 always means the row is committed. `INSERT ... RETURNING id, created_at`
gets the id and the server-generated `created_at` in one round trip. Likes use
`INSERT ... ON CONFLICT DO NOTHING` and read `changes()` (rows affected) to decide
201 (new) vs 200 (already liked) — one statement, no pre-read.

**SQLite pragmas (rule 6 compliant).** Per connection: `journal_mode=WAL`,
`synchronous=NORMAL` (durable with WAL, no `OFF`), a 256 MB `mmap_size` and a
16 MB page cache so hot pages stay mapped, `temp_store=MEMORY`, and
`busy_timeout=5000` as a safety net. Readers also set `query_only=ON`. The build
compiles the amalgamation with `-O2` and a few `SQLITE_OMIT_*`/default flags that
only shrink our own binary's SQLite — no box tuning.

**No work wasted per request.**
- Responses are written straight into a byte buffer in the exact key order the
  suite checks — no `encoding/json` on the hot path, no reflection, no map
  ordering surprises.
- JWTs are verified in full on every request: base64url-decode, confirm
  `alg=HS256`, recompute the HMAC-SHA256 over `header.payload` and compare in
  constant time, check `exp`, then validate that `sub` is a positive-integer
  string and `username` is a string. Nothing about a token is remembered between
  requests (rule 5). Auth is checked before the id on authenticated routes, as
  the spec requires.
- Post ids are validated by hand (digits only, positive), so `0`, `-1`, `abc`
  and `1.5` are rejected as `invalid post id` without touching the database.

**Keep-alive.** `IdleTimeout` is 75 s so a VU's connection survives its 5–15 s
think-time without being recycled mid-loop.

**Runtime.** `GOMAXPROCS=2` (default, override via env): one core for the box,
but a second P lets the Go scheduler overlap a blocking cgo/SQLite call with
network I/O instead of stalling the whole server. `READERS` is tunable the same
way.

## Direct, not Nginx

Go's `net/http` holds tens of thousands of idle keep-alive connections cheaply
(a goroutine plus a few KB each), so serving the internet directly skips the
14–20 % of the single core that Nginx would burn and removes Nginx's
`keepalive 64` upstream pool as a bottleneck.

Measured in a Docker container pinned to the droplet's limits
(`--cpus=1 --memory=2g --memory-swap=2g`), with k6 running `bench/load.js`:

| VUs | Nginx mode | Direct mode |
|----:|---|---|
| 20k | pass, p99 46 ms, CPU ~80 % | pass, p99 **1.96 ms**, CPU ~35 % |
| 25k | — | pass, p99 **2.35 ms**, CPU ~40 %, 0 errors |

Direct mode cut p99 ~20× and CPU roughly in half, and never exceeded ~40 % CPU
or ~875 MB / 2 GB at 25k users — plenty of headroom. (These are indicative:
k6 shared the host with the container and short holds were used; the official
score comes from 5-minute holds on the droplet with k6 on a separate machine.)

## Correctness

Passes all 42 checks in `test/test.sh` on a fresh copy of the seed database.

Note for anyone running the suite on a non-UTC machine: the suite's "created_at
is now" check strips the `Z` and parses the timestamp with GNU `date`, which
reads a naive timestamp as **local** time. On a box that isn't on UTC that shows
a false drift equal to your UTC offset. The server stores true UTC
(`strftime('%Y-%m-%dT%H:%M:%fZ','now')`); run the suite with `TZ=UTC` (as CI
does) and it's 42/42.

## Run it

```bash
sudo bash submissions/go-nethttp-NaveedZahoorKhan/install.sh   # once, as root
bash test/run.sh submissions/go-nethttp-NaveedZahoorKhan       # build + start + test
```

Env it reads: `SQLITE_PATH`, `JWT_SECRET`, `HOST`, `PORT` (plus optional
`GOMAXPROCS`, `READERS`).
