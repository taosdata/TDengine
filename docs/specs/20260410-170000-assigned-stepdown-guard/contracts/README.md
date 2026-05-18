# Contracts

No new external API, CLI, wire protocol, or user-facing interface contract is introduced by this feature.

Scope is limited to internal sync decision logic in assigned-leader stepdown behavior.

Contract confirmation checklist:
- Assigned-leader restoring guard does not change external RPC shape.
- No new SQL, REST, WebSocket, or CLI contract is added.
- Scenario validation remains in internal system-test coverage only.

If later implementation introduces externally observable protocol/schema changes, add explicit contract artifacts in this directory.
