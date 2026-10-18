package com.crp.system.libs.kafka.publisher.durable.mysql

import com.crp.system.libs.kafka.publisher.api.EventPublisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

/** Explicit caller scheduling. At-least-once transport: consumers deduplicate by stable event identity. */
class MySqlDurableEventRelay(
    private val store: MySqlDurableEventStore,
    private val publisher: EventPublisher,
    private val leaseDuration: Duration = Duration.ofMinutes(2),
    private val retryDelay: Duration = Duration.ofSeconds(5),
    private val acknowledgementTimeout: Duration = Duration.ofSeconds(60),
) {
    init {
        require(!acknowledgementTimeout.isZero && !acknowledgementTimeout.isNegative && leaseDuration > acknowledgementTimeout) {
            "lease must exceed positive acknowledgement timeout"
        }
        require(!retryDelay.isNegative) { "retry delay must be nonnegative" }
    }

    /** One bounded batch; claim transaction closes before publication. Concurrent invocations claim disjoint work. */
    fun relayOnce(limit: Int = 100): Mono<RelayBatchResult> = store.claim(limit,leaseDuration).flatMap { records ->
        // All sends begin promptly after claim, so later records do not wait behind earlier send timeouts.
        Flux.fromIterable(records).flatMap({ record -> send(record) }, limit).collectList().map { outcomes ->
            RelayBatchResult(records.size,outcomes.count { it == Outcome.DELIVERED },
                outcomes.count { it == Outcome.FAILED },outcomes.count { it == Outcome.LEASE_LOST })
        }
    }

    private fun send(record: ClaimedDurableEvent): Mono<Outcome> = Mono.defer {
        Mono.fromFuture(publisher.publishJsonWithResult(record.event.topic,record.event.key,record.event.body,
            mapOf("event_id" to record.event.eventId)))
    }.timeout(acknowledgementTimeout)
        .map { Outcome.DELIVERED }
        .onErrorResume { error ->
            store.releaseForRetry(record,retryDelay,error.javaClass.simpleName)
                .map { if (it) Outcome.FAILED else Outcome.LEASE_LOST }
        }.flatMap { outcome ->
            // Database acknowledgement errors leave the lease recoverable; do not classify them as broker success.
            if (outcome == Outcome.DELIVERED) store.markDelivered(record).map { if (it) outcome else Outcome.LEASE_LOST }
            else Mono.just(outcome)
        }

    private enum class Outcome { DELIVERED, FAILED, LEASE_LOST }
}
