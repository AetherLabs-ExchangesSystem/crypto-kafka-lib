# Reporting proposal review and proposed revisions

**Status:** APPROVED 2026-10-07 — owner: "ok i agree with these" after asking whether implementation may start. Corrective implementation is authorized; production deployment remains a separate step after verification.

## Problem (user view)

As the owner and finance team, we need the reports on **10.130.82.3**, fed by Kafka on **217.216.57.42**, to show trustworthy money and player balances. The six October 5 proposals describe guarantees that the current implementation cannot fully provide. This revision keeps that placement and identifies the corrections needed before calling the reports finance-grade.

The review covers the six supplied proposals, both handover books, all thirteen agent cards, the local PR review guide, and selected producer-to-reporting flows. It does not authorize implementation or resume the tasks embedded in the handovers. The original proposal files and their historical approval statuses remain unchanged.

## Why

### P1 Balance updates with equal timestamps give different answers across batches

P13a promises the latest balance per pocket, using a DB change time. The Debit reporting branch actually stamps the update with `System.currentTimeMillis()` after the pocket update, before commit. That is application time with millisecond precision, not a database ordering token.

The reporting consumer keeps the last equal-timestamp update **within** a batch, but its SQL accepts only a strictly newer timestamp **across** batches. Its existing integration test explicitly expects these different answers:

| Same pocket and same timestamp | Final balance |
|---|---:|
| Update to 10, then update to 20, in one batch | 20 |
| The identical updates in two batches | 10 |

This makes liability depend on batch boundaries. Different producer clocks and publication after commit add further ordering risks. A player Kafka key orders arrival within a partition; it does not establish database commit order across services.

Evidence: `Crypto/DebitEventConsumerService-wt-reporting/src/main/kotlin/com/crp/system/application/commands/settle/SettleBalanceHandler.kt:80-111`; `GO_ReportingService/internal/business/sources/balances/source.go:179-202`; `.../balances/statements.sql.go:30-46`; `.../balances/source_integration_test.go:105-120`. Proposal reference: step 3, lines 66 and 105-109.

**Required revision:** use a monotonically increasing pocket version assigned under the pocket's database write lock, carried with the exact post-write balance. Import the same version with the initial snapshot. Preserve `changedAtMs` for display, not conflict resolution. Merely changing `<` to `<=` lets a stale retry overwrite a newer balance and does not resolve cross-producer ordering.

### P1 Financial events can disappear after their database change commits

Step 3, lines 53 and 96-102, proposes acknowledged delivery with a persistent spool and says P13a is never dropped. The implemented settlement reporting channel uses `backpressure=DROP`, in-memory queues and bounded retries. A full queue goes to the failure handler; the default handler logs/counts the loss. The Debit branch commits its money transaction before publishing the settlement result, and an already-handled redelivery does not publish the result again. Its funds push uses a separate `FundsUpdateProducer`, whose `kafka.send(...)` result is ignored; that path also has no durable record tied to the money commit.

A process crash between commit and publication, queue overflow, or exhausted delivery retries can therefore leave a completed money operation without its reporting fact. Consumer offsets cannot recover a message that never reached Kafka. A durable spool written only after a send fails also leaves the commit-to-spool crash window.

Evidence: `Crypto/DebitEventConsumerService-wt-reporting/src/main/resources/application.properties:43-52`; `.../application/commands/settle/SettleBalanceHandler.kt:55-76,113-145,149-172`; `.../infrastructure/kafka/FundsUpdateProducer.kt:14-15`; `Crypto/crypto-kafka-lib-wt-event-publisher/src/main/kotlin/com/crp/system/libs/kafka/publisher/core/ChannelEventPublisher.kt:87-97`; `.../adapters/LoggingPublishFailureHandler.kt:12-40`. The handover book acknowledges the DROP configuration at line 86.

**Required revision:** use a durable source-side record in the same transaction as financial state changes, with a retrying relay and stable event identity. Settlement results and pocket state need this guarantee. Best-effort session telemetry can be a separate, explicitly accepted policy. Until durable delivery exists, label balances and settled GGR as provisional and publish the actual loss counters in reporting health.

### P1 Zero reconciliation difference does not prove agreement with settled PAM money

Step 5, lines 85-97, describes daily ledger reconciliation, and the overview declares seven days of zero differences sufficient for finance-grade reporting. The current casino comparison is against South's accepted movement ledger. South is never told about a later asynchronous PAM failure, so both control totals and reporting's raw comparison retain that movement. Failed settlements are reported separately. Payments and sportsbook comparisons also use two-day windows rather than the proposed exact single-day comparison.

For example, South can accept a debit of 10, PAM can later reject it, and the principal comparison can still show 10 versus 10. If the separate failure event is lost, that comparison does not expose the failed movement. Agreement between two computations of accepted requests is useful, but does not prove that PAM debited 10.

Evidence: `Metagrator_Integration _South/internal/business/wallet/service.go:242-321`; `Crypto/DebitEventConsumerService/src/main/kotlin/com/crp/system/application/commands/settle/SettleBalanceHandler.kt:49-68`; `GO_ReportingService/internal/business/reconciliation/statements.go:58-83`; `.../reconciliation.go:8-36,61-74`; contract `docs/contracts/reporting-events.md:95-104`.

**Required revision:** name the existing check as accepted-movement reconciliation and disclose its window. Finance sign-off additionally requires complete settlement outcomes and an independent source-side computation of committed PAM movements, published over Kafka. The reporting service still reads no PAM database. Specify how accepted amounts, rejected movements and settled amounts reconcile, including a net betwin and a refund. Known failed requests should explain the bridge rather than become unexplained differences.

### P1 The closing balance compares different instants

P13b is described as an end-of-day check with an exact difference alert. WalletService actually reads current pockets around 00:05 UTC, while reporting compares its 00:00 liability snapshot. The implementation uses a default 0.5% tolerance to allow for intervening movements and lag. A missed change inside that tolerance can pass; legitimate activity exceeding it can fail. The live pockets query cannot reconstruct midnight history later.

Evidence: `Crypto/WalletService-wt-reporting/src/main/kotlin/com/crp/system/infrastructure/jobs/DailyClosingBalancesJob.kt:99-112,154-179`; `.../infrastructure/repositories/ClosingBalanceRepository.kt:72-83`; `GO_ReportingService/internal/business/reconciliation/reconciliation.go:50-59`; `GO_ReportingService/README.md:133`. Proposal references: overview D17; step 3 P13b; step 4, line 88.

**Required revision:** describe the present job as an approximate balance check at its actual snapshot time. Exact closing liability needs a source snapshot and reporting state at the same logical boundary, with a durable publication and an explicit completeness marker. Keep the tolerance visible and separate this check from exact financial reconciliation. This is a documented implementation limitation, not a newly discovered change in behavior.

### P1 Zero-wagering operator free spins can overstate NGR

The proposal leaves this as an open question, but the current code confirms the conflict. An assignment with wagering multiplier zero targets MAIN. Reporting excludes every `OPERATOR_FREE_SPIN` promo payout from bonus cost and costs free spins only at `wagering_released`. The wagering release path still tries to debit BONUS and credit MAIN.

A zero-wagering payout can therefore already be real money while its cost remains uncounted. The release path also requires review to ensure it cannot credit the same winnings twice when BONUS contains other funds. This is conditional on enabling such campaigns; the handover already records it as unresolved.

Evidence: `GO_FreeSpinsService/internal/business/assignmentdirectory/assignmentdirectory.go:137-140`; `.../freespinswagering/wagering.go:145-185`; `GO_ReportingService/internal/business/sources/freespins/decode.go:105-115`; `GO_ReportingService/migrations/000001_foundation.up.sql:97-101`; handover book, lines 169 and 177.

**Required revision:** define one cost recognition rule tied to the actual credit to real money, correlate the payout and release identities, and prove that both money and cost are counted once. Zero-wagering campaigns remain outside finance sign-off until this path is resolved. Confirm provider funding decisions separately; do not infer them from the payout type alone.

### P1 The rollback can delete an existing Postgres installation

Step 1, line 280, proposes purging Postgres and removing `/var/lib/postgresql/18`; line 274 stops the whole Postgres service. The handover book, line 155, reports that PG18 already runs on .3 and says to identify its users. The old claim that no Postgres exists there is therefore not a safe premise for those commands.

**Required revision:** rollback the reporting unit, reporting-specific tunnel/redirect and owned Grafana configuration first. Restore reporting-specific settings from recorded backups. Never stop, purge or delete a shared cluster or unrelated database as the reporting rollback. Cluster ownership and existing users must be established before any installation or sizing change. The review did not inspect the live host, so its reported existing cluster remains a deployment prerequisite to verify.

### P2 The raw-off scaling guarantee is not implemented

Step 2, lines 125 and 290-306, promises a raw-off mode with a two-hour deduplication table. The current configuration explicitly refuses `store_raw_detail=false`; reconciliation and rebuild read casino raw rows. Consequently P5 does not, by itself, enable the proposed storage escape route.

Even if implemented as written, two hours of deduplication would not cover an outbox retry delivered at a new Kafka offset after that window. Atomic offsets protect repeated processing of the same offsets, not producer duplicates at new offsets after their business key expires.

Evidence: `GO_ReportingService/internal/platform/config/validate.go:102-104`; `.../config/validate_test.go:137`; `GO_ReportingService/README.md:75-76,126`; `.../reconciliation/statements.go:77-79`.

**Required revision:** scope this release to raw-on reporting on .3 and replace the scale claim with a measured capacity budget for that box. Include raw tables, indexes, WAL, forever-retained player tables, backup copies and restore scratch space. Treat raw-off as a later change requiring a durable business-identity strategy and replacement reconciliation/rebuild inputs. Any future deduplication horizon must cover the supported retry/replay horizon.

### P2 The six proposals are no longer one consistent rollout contract

The supplied step 1 amendment selects outbound SSH; the overview and step 5 still deploy WireGuard. The handovers replace staging with local Docker rehearsal, move the import to .3 and require HTTPS, while the original proposals still require .58 and LAN HTTP. The reporting-repo copies of proposals 0 and 2 also differ from the supplied copies.

Other material mismatches: `account.player.events` is now mandatory; the cut-over is fixed for the deployment lifetime; payment control totals now come from Deposit/Withdraw consumers; some proposed monthly retention is not enforced; and Debit/Credit include money-transaction changes even though step 3 says money logic does not change. Pending withdrawal SQL correctly treats cancellation/release as one terminal outcome, while the proposed subtraction formula can count both.

Evidence: step 1, lines 89-118 versus its lines 123-237; step 5, lines 105-119; handover book, lines 15-17,34-46,86-94; `GO_ReportingService/internal/platform/config/validate.go:102-104`; Debit's transactional block cited above; `GO_ReportingService/migrations/000004_kpi_views.up.sql:483-506`; proposal-copy diffs inspected on 2026-10-07.

**Required revision:** consolidate these already recorded decisions into the six proposal files before the next approval or production run. Keep old mechanisms only in a clearly marked revision history, with a single active installation, rollback and verification procedure. Expand the review scope of money-path changes rather than describing every publisher as purely additive telemetry.

## What changes

This proposed revision changes the review contract as follows. It makes no executable change.

| Area | Before | Proposed revision |
|---|---|---|
| Placement | Reporting on .3, Kafka on .42 | Keep .3 and .42 |
| Existing network | Build a new mechanism from conflicting instructions | Verify the existing .3-to-.42 route first; reuse it when encrypted and suitable; use the documented restricted SSH fallback only when needed |
| Pocket ordering | Newest millisecond timestamp wins | Database-assigned pocket version wins; identical version is an idempotent replay |
| Financial delivery | Post-commit background send, with a claimed spool | Same-transaction durable record and retrying relay |
| Casino reconciliation | Zero accepted-ledger difference implies settled money is correct | Distinguish accepted requests from committed PAM settlement and prove the bridge |
| Closing liability | Exact end-of-day check | Present approximate 00:05 check honestly; exact same-boundary check is a separate requirement |
| Free-spin cost | All operator payouts wait for wagering release | Cost actual real-money credits once, including multiplier-zero campaigns |
| Rollback | Stop/purge the Postgres installation | Undo only reporting-owned artifacts and restore recorded settings |
| Capacity | Switch raw off at higher volume | Raw stays on in this release; prove .3 capacity at the declared launch load |
| Rollout documentation | Mixed staging, WireGuard and HTTP steps | One procedure consistent with the handover's current implementation and verified decisions |

The local network evidence supports the owner's expectation that office access may already exist: `proposals/2026-10-02-kafka-cluster-move.md:51-52` records the office VPN exit in Kafka's allowlist, and `deploy/netshop-vpn.conf` specifies that VPN. It does **not** establish .3's live route, source address, Kafka metadata reachability, or whether the whole data path is encrypted. Verify those from .3 without changing .42's firewall or Kafka listener. No move of either box is proposed.

## What does NOT change

- The service consumes Kafka and writes its reporting Postgres on .3; it does not query money databases on .42.
- UTC buckets, original-currency totals, dynamic EUR views, current-player tags for lifetime daily reports, and platform-level deposits/withdrawals remain the intended reporting model.
- This review changes no source code, configuration, schema, deployment, branch, PR or historical approval.

## Key code changes

The following behavior was approved for implementation on 2026-10-07. Per-repository records will capture the concrete schema, code and verification changes under this approved contract.

```text
Producer, inside the existing pocket transaction:
  lock the pocket
  apply the monetary change
  increment pocket_version
  persist an event containing pocket_id, pocket_version and balance_after
  commit all of these together

Relay:
  publish the durable event with its stable identity
  mark it delivered after broker acknowledgement
  retry safely across process restarts

Consumer:
  accept a strictly greater pocket_version
  treat an identical version and balance as a replay
  surface an identical version with a different balance as a contract error
  keep older versions from overwriting current state
```

Every pocket writer and the import snapshot must use the same version contract. Kafka offsets may remain stored with reporting effects in the existing transaction. A Kafka offset used as a tie-breaker alone cannot fix out-of-order publication by different writers. Version introduction and durable producer storage require their own reviewed schema changes and rollback plans in the affected repositories.

## Risks and rollback

A versioned balance migration cannot safely mix unversioned and versioned producers under an unexplained precedence rule. Gate rollout on complete writer coverage and a matching import snapshot. Size the durable publisher backlog and its cleanup; a broker outage must not produce unbounded disk growth.

The current approximate closing check and accepted-request reconciliation can remain useful during rollout if they are labeled accurately. They must not be presented as exact proof of settled money. Keep existing reporting effects provisional until the relevant acceptance gates pass.

Rollback of subsequent executable changes must be reporting-specific. Preserve existing Postgres data, source ledger integrity and .42's existing firewall. This document-only revision can be removed or superseded without runtime effect.

## Verification

Required acceptance checks for the revised guarantees:

1. Equal-time updates produce the same final balance in one batch, separate batches, swapped arrival order and replay. Greater pocket versions always win, including across producer hosts and import/live overlap.
2. Crash after the money commit but before publication, fill the publisher queue, and hold Kafka unavailable beyond the current delivery timeout. Each committed financial fact eventually arrives once in effect, with no lost final balance or settlement result.
3. Accept a casino debit, then fail its PAM settlement. The accepted/settled bridge explains it and financial totals equal committed PAM ledger effects, including net betwin and refund cases. Losing an outcome must cause a completeness failure.
4. A balance change just after midnight cannot be mistaken for a midnight discrepancy. Compare source and reporting snapshots at one declared logical boundary; prove the completeness of both inputs.
5. Zero-wagering and positive-wagering campaigns each credit real money and recognize cost once, including duplicate payout/release delivery and a pre-existing BONUS balance.
6. Rehearse reporting rollback on a cluster containing an unrelated database. That database and its service remain available and unchanged.
7. Measure .3 disk and memory needs at the declared launch volume, including a backup/import/restore run; verify statement timeouts and reporting-owned resource caps against the existing services.

Fresh checks run for this review:

```text
GOPROXY=off GOSUMDB=off go test -count=1 \
  ./internal/business/sources/balances ./internal/platform/config

Both packages passed (exit 0).
```

Those checks cover unit tests and config refusal behavior. The equal-timestamp cross-batch defect is established by the actual SQL and the existing integration test, which deliberately expects 20 in one batch and 10 in two batches; this review did not run that database test. The tests' current expectations therefore do not establish financial correctness.

Source snapshots inspected: reporting `60d8b77`; rollout `e391ef1`; import tooling `a533cfe`; installer `bdd19f0`; rehearsal `bb6f59e`; Debit publisher `c05134f`; publisher library `ec022c8`; Wallet publisher `1dd8130`. These checkouts were clean when inspected. Rollout's current commit is explicitly a WIP save. The pending cards remain review/verification work, not evidence that the final merged release has passed.

Limits: selected financial and deployment flows were traced end to end; this was not a review of every line in all publisher PRs. No production or staging host was contacted; no live connectivity or GitHub PR state was verified; no full integration suite, final rehearsal or real-backup rehearsal was run. The handover reports those remaining release gates. The supplied `/Users/mobilelead/Documents/develop/proposals/...` guide path does not exist; the reviewed guide is `Casino-Crypto/proposals/2026-10-06-reporting-pr-review-guide.md`.


## crypto-kafka-lib implementation delta (approved scope)

Implement Task 2 in the existing artifact: acknowledged raw JSON publishing; an explicitly constructed optional R2DBC MySQL outbox store and relay; exact duplicate identity validation; bounded leases, retries and delivered-row cleanup. The store rejects insertion without the caller's active bound transaction. No scheduler or database driver is added to existing users. Generic transport records contain opaque TEXT, with money-domain typed records owned by services.

Verification uses real isolated MySQL transaction rollback, committed-record restart, failed/queue-full/disabled acknowledgement, lease recovery/token fencing, duplicate content mismatch and bounded cleanup. The ordinary auto-configuration is tested without optional R2DBC dependencies. No production changes, remote access, publication or Maven identity change is part of this library task.


### Concrete Task 2 provider delta

The optional provider includes `MySqlFinancialRecordStore` for the eight callers' immutable typed values and transport, and `MySqlPocketStateReader` for the repeated locked post-write snapshot/database UTC clock query. These are technical persistence/capture seams, not amount, wallet, version-increment or outcome policy. `read` and `currentTime` require the same caller-bound writable transaction as the stores. The caller makes the atomic amount/version UPDATE and supplies all domain context.

The source DDL adds three InnoDB tables: transport with opaque TEXT; pocket history keyed only by `(pocket_id,pocket_version)`; terminal outcomes keyed only by `(source_service,tracking_id,operation)`. Typed controls retain at least 35 days and longer for open snapshot/recheck/replay ownership; pending transport is never age-deleted. Source B2B acceptance retains nullable original business time/context rather than fabricating a historical cohort.


### Verification result (2026-10-07, local)

Test-first RED logs: `/private/tmp/reporting-lib-raw-red.log` (missing raw acknowledgement method), `/private/tmp/reporting-lib-durable-red.log` (missing provider APIs), `/private/tmp/reporting-lib-reader-red.log` and `/private/tmp/reporting-lib-reader-clock-red.log` (missing locked capture/DB clock API). The first real MySQL run found a typed DATETIME mapping mismatch; explicit typed driver reads corrected it. Optional test dependencies originally activated unrelated Boot database auto-configuration, so ordinary tests retain their database-free runtime and MySQL has its own opt-in task.

Fresh final GREEN command:

```sh
FINANCIAL_OUTBOX_MYSQL_URL='<owner scratch URL ending /reporting_integrity>' JAVA_HOME='<Java 17>' ./gradlew test --tests 'com.crp.system.*' testWithoutR2dbc --tests 'com.crp.system.*' testWithoutMicrometer --tests 'com.crp.system.*' durableMysqlTest --tests 'com.crp.system.*' --offline --no-daemon --max-workers=2
```

Final result: ordinary tests 251 (247 passed, four existing opt-in external-Kafka skips), absent-R2DBC startup 1 passed, absent-Micrometer 2 passed, real MySQL durability/capture 13 passed; zero failures/errors. Log: `/private/tmp/reporting-lib-final-reader-green.log`. No external Kafka bootstrap was configured; real broker availability/cluster deployment remains a separate rehearsal. Public development dependencies fetched without changing the baseline: Spring R2DBC 6.0.13 and Reactor 3.5.11; test MySQL driver io.asyncer:r2dbc-mysql:1.0.5.

Tested library artifact: `/private/tmp/reporting-integrity-artifacts/crypto-kafka-lib-1.3.1-financial-integrity.jar`, SHA256 `c3d80e69306ce52e79dbb42f43de3576ccde5bb068ae306089494ee98642f4ec`. Runtime artifact identity remains unchanged; consumers will pin the tested source commit in the existing official JitPack repository after the authorized root-owned remote workflow.
