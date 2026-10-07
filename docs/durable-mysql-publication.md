# Optional durable MySQL publication

The provider lives in the existing crypto-kafka-lib artifact under `com.crp.system.libs.kafka.publisher.durable.mysql`. It has no auto-configuration, automatic schema creation, scheduler or database driver. Ordinary users need no R2DBC/Reactor runtime. Provider users supply their existing Spring R2DBC `DatabaseClient`, transaction manager/operator and MySQL driver. The library compiles against Boot 3.1.5 / Spring 6.0.13 / Reactor 3.5.11 APIs. Optional dependencies are compile-only and are absent from the published runtime dependency graph.

## Capture inside the caller transaction

```kotlin
val outbox = MySqlDurableEventStore(client) // accounts.financial_event_outbox
val records = MySqlFinancialRecordStore(client, outbox)

transactionalOperator.transactional(
    applyAmountAndIncrementVersion() // caller money policy; holds the pocket write lock
        .flatMap { captured ->
            records.recordBalance(
                captured.typedRecord, // exact locked post-write DECIMAL(40,20) balance/version
                DurableEvent(captured.stableId, "funds-update", captured.playerKey, captured.serializedJson),
            )
        }
).then()
```

`append`, `recordBalance` and `recordOutcome` require a writable Spring reactive transaction already bound to the exact `DatabaseClient.connectionFactory`. Calling them outside that context fails rather than silently autocommitting. The typed store and outbox must use the same connection factory identity. No transaction is opened for the money caller. Subscription must remain in the reactive transaction context; do not detach with `subscribe()` or invoke the store after commit.

`PocketBalanceRecord` is keyed by `(pocketId,pocketVersion)`. `FinancialOutcomeRecord` is keyed by `(sourceService,trackingId,operation)` and requires terminal `committed`/`rejected` plus `debit`/`credit` direction. The caller decides amounts, target wallets, pocket-version increments, outcomes, stable IDs and wire DTOs. The helper only persists captured typed values and opaque transport atomically. `balanceAfter`/`amount` must fit DECIMAL(40,20) without rounding. UTC `recordedAt` and nullable `sourceBusinessOccurredAt` use LocalDateTime with microsecond precision. `recordedAt` is capture time, not asserted database commit time; committed row visibility is the proof.

A duplicate true key and stable event id succeeds only when every immutable typed field and transport `topic`, nullable `key`, and exact `body` agrees. JSON whitespace and property order are part of transport identity. Replays must reuse the original capture timestamp/payload; read the stored terminal result or exit through the caller's existing handled-request path. Reconstructing the record with a new `now()` conflicts intentionally. An identity conflict aborts the caller transaction and never logs values or JSON. Database strict mode must reject malformed/truncated typed strings; the post-insert comparison also detects coerced values.

Null original business time or movement context is an unknown legacy cohort. It cannot be silently assigned to the capture date or presented as verified controls. The source acceptance fields stay separate from its recorded acceptance time.

`MySqlPocketStateReader(client).read(pocketId)` performs a transaction-bound `SELECT FOR UPDATE` of amount, pocket_version and `UTC_TIMESTAMP(6)`. It returns `PocketState(balance,version,recordedAt)` and retains the pocket lock through caller commit/rollback. It never increments a version or chooses a money operation. `currentTime()` reads the same database UTC clock in that transaction for a rejected/no-pocket outcome. Both reject detached/autocommit reads. A caller may optionally pass a validated qualified pocket table name for an isolated schema.

## Relay outside the money transaction

```kotlin
val relay = MySqlDurableEventRelay(outbox, channels.get("reporting"))
// Explicitly owned service scheduling subscribes and handles failures:
relay.relayOnce(limit = 100) // Mono<RelayBatchResult>
```

The default lease is two minutes, acknowledgement timeout one minute, retry delay five seconds. A lease must exceed the positive acknowledgement timeout. Each claim is 1..1000 records in a short `FOR UPDATE SKIP LOCKED` transaction. The claim commits before publication; calling it inside another transaction fails. Records are sent concurrently within the bounded batch so later records do not sit behind earlier acknowledgement timeouts. A caller that invokes overlapping batches must still bound its scheduler/concurrency and connection pool.

`EventPublisher.publishJsonWithResult` preserves the body unchanged and completes only on broker acknowledgement. Disabled, missing, full-queue, rejected, delivery-failed and timeout results leave the outbox pending with a retry time. A legacy third-party EventPublisher implementation inherits a safe failed result until it implements this API; typed serialization is never used as a fallback for raw JSON. The interface uses JVM default compatibility to preserve existing compiled implementations.

The relay adds `event_id` to Kafka headers. Successful acknowledgement updates `delivered_at` only while the same unexpired lease token still owns that event. Expired leases recover after process death. Old relay tokens cannot acknowledge or release a newer claim. A database failure during acknowledgement leaves the record recoverable. A crash after broker acknowledgement but before the database mark can replay at a new Kafka offset; consumers must deduplicate stable business identity. This is at-least-once delivery, once in downstream effect.

When a broker timeout cancels its future, the producer may still complete the physical send. The retained record may replay. Neither future cancellation nor a full publisher queue is a delivery acknowledgement. Pending records are never deleted due to attempt count, age or outage.

## Schema, retention and capacity

The `durable/mysql-schema.sql` resource is reference DDL for the three tables; it does not run automatically. Wallet owns the additive accounts migration, preflight and role bindings. `body` is opaque TEXT (max 65,535 UTF-8 bytes), not MySQL JSON. There are no business columns in the transport table. Typed tables have their own exact keys and event-id association. No foreign key is added that could erase or prevent retained evidence when a pocket changes or is removed.

`cleanupDelivered(retention,limit)` removes only previously acknowledged rows older than the positive retention interval, at most 1000 per call. Use a separately reviewed cleanup identity; relay privileges do not require DELETE. Keep pending rows indefinitely until broker acknowledgement and alert on pending count, oldest age and disk/backlog growth. Removing a delivered transport row eventually removes source transport deduplication for that identity; retain for the supported replay horizon and deduplicate business identity downstream.

Typed balances/outcomes retain a 35-day minimum, with longer retention for any open exact snapshot/recheck/dispute or replay. Current pocket-version evidence and versions referenced by open snapshot manifests cannot be pruned just by a timestamp cutoff. No uncoordinated typed cleanup is installed. Their recorded-time indexes support later owned bounded cleanup once snapshot/run ownership determines safe cutoffs.

Volume is one balance row plus transport per changed pocket, plus one outcome row plus transport per terminal operation. Measure representative `data_length`/`index_length` and row/index bytes on scratch MySQL; reserve database, backlog and index overhead. Backlog alerts must fail the reporting completeness gate before capacity runs out, not drop financial facts.

## Verification

`JAVA_HOME` must be Java 17 for Gradle 7.6. Run scoped ordinary tests and absent-dependency startup tasks offline with `--no-daemon --max-workers=2`. `FINANCIAL_OUTBOX_MYSQL_URL` opts in to MySQL tests and points at the owner-provided `reporting_integrity` scratch database. MySQL tests create only the named `reporting_integrity_lib` scratch database on the owner-provided loopback test instance, never a production schema. The regressions cover rollback, autocommit rejection, restart, broker/full-queue/disabled failures, timeout retry, expired leases, stale-token fencing, disjoint bounded claims, duplicate identity conflicts, typed true-key replay and bounded delivered cleanup. Existing real-Kafka tests remain separately opt-in via their bootstrap environment variable.
