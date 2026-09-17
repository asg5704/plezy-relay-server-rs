# relay-rs

Personal Rust rewrite of Plezy's Watch Together relay (`~/oss/plezy/server/`,
Go), targeting `protocolVersion = 2` only, for deployment to Railway. Wire
protocol is vendored at `relay_protocol.json`.

Full architecture/design spec (crate choices, module layout, concurrency
model, persistence model, Dockerfile sketch, Railway setup notes) lives at
`~/.claude/plans/iridescent-jumping-treehouse.md` — read that first if
anything below is ambiguous. Business logic (exact per-message-type
semantics, quotas, host-transfer eligibility algorithm) was extracted from
`~/oss/plezy/server/main.go` et al.; re-derive from there if `room.rs`
behavior is ever in question.

## Status

Modules implemented and building: `protocol.rs`, `ids.rs`, `error.rs`,
`room.rs` + `host_transfer.rs` (room state machine), `registry.rs`,
`quotas.rs`, `client_ip.rs`, `logs.rs`, `snapshot.rs`, `cleanup.rs`,
`connection.rs`, `ws.rs`, `config.rs`. `main.rs` is wired up (`AppState`,
router, background tasks, graceful shutdown). `cargo test` is clean: 51
tests (26 unit + `tests/protocol_conformance.rs`,
`tests/room_state_machine.rs`, `tests/ws_integration.rs`), all stable
across repeated runs. Also manually smoke-tested via `docker build`/
`docker run` (`/health`, `/logs`, a real WS `create` round-trip persisting
to a mounted `/data` volume, `docker stop` triggering graceful shutdown).

## Development

### Running locally

```
cargo run
```

Config comes from `config.rs`, every flag has an env-var fallback (Railway
sets these via its dashboard, so no CLI flags are needed there):

| Flag | Env var | Default | Notes |
| --- | --- | --- | --- |
| `--addr` | `ADDR` | unset | Full `host:port`; overrides `--port`/`PORT` when set. |
| `--port` | `PORT` | `8080` | Binds `0.0.0.0:<port>`. Railway injects `PORT` automatically. |
| `--log-dir` | `LOG_DIR` | `/data/logs` | Crash-log storage directory (currently unused by `logs.rs`, which stores in memory — reserved for a future on-disk store). |
| `--state-file` | `STATE_FILE` | `/data/rooms.json` | Room-state snapshot path. |
| `--trusted-proxy-cidrs` | `TRUSTED_PROXY_CIDRS` | `""` | Comma-separated CIDRs. Empty means "trust no proxy." See the Railway section below before setting this. |

The defaults point at `/data/...`, which won't exist on a dev machine.
Override both for local runs, e.g.:

```
STATE_FILE=./tmp/rooms.json LOG_DIR=./tmp/logs RUST_LOG=info cargo run
```

`RUST_LOG` controls `tracing-subscriber`'s `EnvFilter` — unset, it emits
nothing; `RUST_LOG=info` is a reasonable default for local runs, `debug`
for more detail.

### Running tests

```
cargo test
```

Currently 26 unit tests (`host_transfer`, `client_ip`, `ids`, `quotas`).
There's no `tests/` integration suite yet — see checklist item 3.

### Docker

```
docker build -t relay-rs .
docker run --rm -p 8080:8080 \
  -e RUST_LOG=info \
  -v "$(pwd)/tmp/data:/data" \
  relay-rs
curl http://127.0.0.1:8080/health
```

The image is a multi-stage `rust:1.98.1-alpine` → `scratch` build (see
`Dockerfile`) with no shell or CA certs in the final image — it's a
single static binary. Mount `/data` (as above) if you want
`rooms.json`/crash logs to survive a container restart; without it,
state is lost when the container stops.

### Deploying to Railway

1. Deploy this repo as its own Railway service (separate from the Go
   relay, if that's still running).
2. **Attach a Railway Volume mounted at `/data`.** The Dockerfile's
   `VOLUME /data` does *not* provision persistent storage on Railway by
   itself — without an attached Volume, `rooms.json` and crash logs are
   wiped on every redeploy.
3. Set the service's health-check path to `GET /health` in the Railway
   dashboard.
4. Leave `TRUSTED_PROXY_CIDRS` unset for the first deploy — the relay
   starts up with a loud warning and trusts no proxy, which is safe by
   default. After deploying, make one real external request and check
   the logs for the resolved peer address (and whether Railway even sends
   `X-Forwarded-For`), then set `TRUSTED_PROXY_CIDRS` to match Railway's
   actual edge network. **Do not** reuse the value from
   `~/oss/plezy/server/docker-compose.yml` — that's tuned for Plezy's own
   VPS + reverse-proxy topology, not Railway's.
5. Point Plezy's Settings → custom relay URL at the deployed service only
   after a manual end-to-end test (create/join, playback sync, a guest
   disconnect+resume, a host transfer) on two real devices — see
   checklist item 6.

## Checklist — what's left

### 1. Finish `main.rs` — done
`AppState` is constructed, the router is built, and the listener is
bound. Implemented in this session:
- [x] Parse `Config` (`config.rs`, already done), init `tracing-subscriber`.
- [x] Build `ClientIpResolver` from `trusted_proxy_cidrs` — **warn loudly
      and continue** if unset/empty per the accepted design decision (don't
      refuse to start).
- [x] `snapshot::load_snapshot(state_file, now)` at startup, then
      `Registry::new(snapshot_handle)` + `registry.restore(restored)`.
- [x] Construct `AppState { registry, logs, client_ip, connections,
      connect_limiter, snapshot }` (fields already defined in `lib.rs`).
- [x] Build the `axum::Router`: `GET /relay` → `ws::ws_handler`, `POST /logs`
      and `GET /logs/:id` → handlers already written in `logs.rs`
      (`post_logs`, `get_logs`), `GET /health` → `logs::health`. Apply
      `tower_http` body-size limit on `POST /logs`.
- [x] Bind on `config.bind_addr()` with `axum::serve` +
      `into_make_service_with_connect_info::<SocketAddr>()` (needed for
      `ws.rs`'s `ConnectInfo<SocketAddr>` extractor).
- [x] Spawn `snapshot::run(rx, registry, state_file)` as a background task
      (pair it with the `snapshot::channel()` sender wired into
      `AppState.snapshot`).
- [x] Spawn `cleanup::run(app)` as a background task (5-min sweep).
- [x] Graceful shutdown: SIGTERM/ctrl_c handler that calls
      `snapshot.flush_and_stop(timeout)` before exiting.

### 2. Dependency fix already applied
`futures-util` was only in `[dev-dependencies]` but `connection.rs` uses it
in non-test code (`SinkExt`/`StreamExt` for splitting the WS socket) — this
broke `cargo build`. Moved it to `[dependencies]`. Confirmed `cargo build`
and `cargo test` are clean after this change (verified during this
session).

### 3. Tests — done
All three planned files exist; `cargo test` is clean (51 tests: 26 unit +
25 across the new integration files, 0 warnings), stable across repeated
runs (no sleep-based flakiness):
- [x] `tests/protocol_conformance.rs` — loads `relay_protocol.json` at
      test time and asserts every `protocol.rs` const/limit matches it
      verbatim. **This immediately caught a real bug**: the vendored
      `relay_protocol.json` had `maxRoomSize: 6`, while `protocol.rs`,
      Go's generated const, and the canonical
      `~/oss/plezy/relay_protocol.json` all say `8`. Re-vendored the file
      from the canonical source to fix it (see `git diff` on
      `relay_protocol.json`).
- [x] `tests/room_state_machine.rs` — idempotent host re-`create` (both
      the pure `RoomState::reconnect_host` reannounce-on-absence behavior
      and `Registry::create`'s idempotent-reuse-vs-reclaim-vs-`room_exists`
      branching), guest reservation resume within/past the 5-minute grace
      window, host transfer eligibility integration (through
      `transfer_host` + the real `hostTransferEligibility` broadcast, not
      just `host_transfer.rs`'s pure-function unit tests), leave/endSession
      two-phase commit and rollback-on-persist-failure (via `begin_*`
      /`finish_*` with `persisted: true/false`).
- [x] `tests/ws_integration.rs` — a real bound axum server + real
      `tokio-tungstenite` WS client over real TCP: create → join from a
      second connection → broadcast/sendTo → leave → endSession, plus a
      resume-after-abrupt-disconnect edge, a per-IP connection-quota edge,
      and a connect-attempt rate-limiter edge (isolated from the
      connection quota by closing each connection between attempts, since
      both caps happen to be the same number).
- [x] `cargo test` clean after adding these.

### 4. Dockerfile — done
Multi-stage `rust:1.98.1-alpine` → `scratch` build (bumped from the plan's
`1.82-alpine` sketch to match `rust-toolchain.toml`'s pinned `1.98.1`; no
CA-cert copy since there's no outbound TLS in scope). `docker build`
verified clean; see item 6 for the run-time smoke test that passed
against it (`/health`, `/logs`, a raw WS upgrade + `create` round-trip
that persisted to a mounted `/data` volume, and `docker stop` triggering
graceful shutdown + snapshot flush with exit code 0).

### 5. README dev instructions — done
Added a `## Development` section above: local run instructions
(`cargo run` + the full `config.rs` flag/env-var table with defaults),
`cargo test`, `docker build`/`docker run` steps, and the Railway
deployment notes (Volume attachment for `/data`, health-check path
`GET /health`, and the `TRUSTED_PROXY_CIDRS` discovery procedure).

### 6. Deploy + verify
- [x] `docker build` + `docker run` locally, confirm `/health` and a manual
      WS session round-trip (`websocat` wasn't installed locally, so this
      was verified with a raw Python WS-handshake script instead — a
      `create` message got a real `created` reply and persisted to the
      mounted `/data` volume as expected).
- [ ] Deploy to Railway as its own service, confirm `/health` via the
      public URL.
- [ ] Point Plezy's Settings → custom relay URL at it on two real devices,
      run an actual Watch Together session (create/join, playback sync,
      one guest disconnect+resume, a host transfer) before treating it as
      production-ready.
