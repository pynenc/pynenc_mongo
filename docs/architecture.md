# Architecture

How `pynenc-mongo` uses MongoDB internally.

## Automatic Document Chunking

MongoDB enforces a 16 MB BSON document limit. The plugin handles large payloads
transparently: data above the `chunk_threshold` (default 15 MB) is compressed with
zlib and split into chunks stored in dedicated chunk collections. Retrieval
reassembles and decompresses chunks automatically — no changes needed in application code.

## Retryable Operations

All collection operations are wrapped with exponential backoff retry logic that handles
transient failures automatically:

- `AutoReconnect`, `ConnectionFailure`, `NetworkTimeout`
- `NotPrimaryError`, `ServerSelectionTimeoutError`

Retry behaviour is fully configurable via `max_retries`, `retry_base_delay`,
`retry_max_delay`, `retry_max_time`, and `retry_indefinitely`.

## Status Transitions and Locking

The orchestrator ensures exactly one runner can change an invocation's status at a time
using an array-push lock protocol on the document:

1. Runner pushes a unique claim ID into the `transition_lock` array on the document
2. Runner reads back the document — if its claim is first in the array, it holds the lock
3. With the lock held, it reads the current status, validates the transition, and writes the new status
4. Finally, it clears the `transition_lock` array to release the lock

If another writer's claim is first, the runner raises `InvocationStatusRaceConditionError`
and the caller retries or backs off. This guarantees mutual exclusion without
transactions or external locking services.

## Connection Pooling

The plugin uses a singleton `PynencMongoClient` per unique connection configuration.
Connections are pooled (default max 100) and indexes are created lazily on first use
to avoid blocking startup.

## Collection Layout

Each component writes to its own set of MongoDB collections, all prefixed with the
component name:

| Component             | Collections                                                                                                                                                                                                           |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Broker**            | `broker_message_queue`                                                                                                                                                                                                |
| **Orchestrator**      | `orchestrator_invocations`, `orchestrator_invocation_args`, `orchestrator_blocking_edges`, `orchestrator_runner_heartbeats`                                                                                           |
| **State Backend**     | `state_backend_results`, `state_backend_exceptions`, `state_backend_invocations`, `state_backend_history`, `state_backend_workflows`, `state_backend_app_info`, `state_backend_workflow_data`, `state_backend_chunks` |
| **Trigger**           | `trg_conditions`, `trg_triggers`, `trg_condition_triggers`, `trg_valid_conditions`, `trg_execution_claims`, `trg_trigger_run_claims`                                                                                  |
| **Client Data Store** | `arg_cache`, `arg_cache_chunks`                                                                                                                                                                                       |

## Logging

The plugin logs through the Pynenc application logger and the standard Python logger:

| Level       | What's logged                                                                 |
| ----------- | ----------------------------------------------------------------------------- |
| **DEBUG**   | Duplicate key handling during history insertion                               |
| **INFO**    | Runner heartbeat registration, inactive runner detection, recovery operations |
| **WARNING** | Retry attempts with delay information                                         |
| **ERROR**   | Final retry failures, claim failures, timestamp parsing errors                |
