# WS Architecture

This document describes the current websocket architecture and the intended direction.

## Package Layout

- `ws/client`: low-level websocket client, read/write pumps, send queue, lifecycle flags.
- `ws/stmt`: stmt protocol wrapper and reconnect around `WSConn`.
- `ws/schemaless`: schemaless protocol wrapper with auto-reconnect.
- `ws/tmq`: tmq consumer compatibility wrapper (delegates to unified adapter).
- `ws/unified`: unified websocket adapters (query/stmt/schemaless/tmq) with failover/reconnect.

## Layering

1. Transport layer
   - Owned by `ws/client`.
   - Responsibilities: send queue, ping/pong, close signaling, last error tracking.
2. Protocol layer
   - Owned by unified adapters and protocol codecs.
   - Responsibilities: encode request, route response by request id, parse protocol payload.
3. Recovery layer
   - Auto-reconnect orchestration and client swap safety checks.
   - Reconnect safety invariants are implemented in the adapter runtime paths.
   - `tmq`/`schemaless`/`stmt`/`query` unified adapters share the same invariants.
   - Compatibility wrappers (`ws/tmq`, `ws/schemaless`, `ws/stmt`) delegate to unified.

## Request Flow (schemaless/tmq)

1. Build request envelope.
2. Load current client pointer.
3. Register response channel keyed by request id.
4. Send envelope via client send queue.
5. Wait on response channel, close signal, client done, or timeout.
6. On close/network error with auto-reconnect enabled, reconnect and retry once.
7. On request failure, append a sanitized request summary to error context.
8. Request summary generation is lazy and runs only on failure paths.

## Reconnect Flow (schemaless/tmq)

1. Enter reconnect lock.
2. If object closed, return closed error.
3. Short-circuit only when a replacement client exists and is still running.
4. Retry dial/bootstrap up to configured count.
5. On successful new client:
   - Replace current pointer atomically.
   - Close replaced old client.
   - For tmq, resubscribe topics.
6. On failure, clear and close only if current client still matches failed client.

## Current Improvement Direction

1. Keep reconnect safety rules explicit in runtime swap/reconnect code paths and regression tests.
2. Move more duplicated send/retry skeleton into shared helpers where behavior is identical.
3. Keep package-specific protocol parsing isolated; do not over-abstract protocol semantics.
