package com.crp.system.libs.kafka.publisher.durable.mysql

import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.transaction.NoTransactionException
import org.springframework.transaction.reactive.TransactionSynchronizationManager
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.UUID

/** Optional provider: construct explicitly with the service's existing DatabaseClient. No beans or scheduler. */
class MySqlDurableEventStore(
    internal val client: DatabaseClient, tableName: String = "accounts.financial_event_outbox",
) {
    private val table = checkedTableName(tableName)
    private val transactions = TransactionalOperator.create(R2dbcTransactionManager(client.connectionFactory))

    /** The transport insert participates in the caller's writable transaction; it never opens one for the caller. */
    fun append(event: DurableEvent): Mono<Void> = inCallerTransaction(client) {
        require(event.eventId.isNotBlank() && event.eventId.length <= 255) { "invalid stable event id" }
        require(event.topic.isNotBlank() && event.topic.length <= 255) { "invalid topic" }
        require(event.key == null || event.key.length <= 255) { "event key exceeds VARCHAR(255)" }
        require(event.body.toByteArray(Charsets.UTF_8).size <= 65535) { "opaque body exceeds TEXT capacity" }
        insertImmutableRecord(client, table, listOf("event_id"), linkedMapOf(
            "event_id" to event.eventId, "topic" to event.topic, "event_key" to event.key, "body" to event.body,
        ))
    }

    /** Short SKIP LOCKED claim transaction. Returned records are committed claims before any send starts. */
    fun claim(limit: Int, leaseDuration: Duration): Mono<List<ClaimedDurableEvent>> = Mono.defer {
        require(limit in 1..1000) { "claim limit must be 1..1000" }
        val leaseMicros = positiveMicros(leaseDuration)
        val token = UUID.randomUUID().toString()
        val action = client.sql("""
            SELECT event_id,topic,event_key,body FROM $table
            WHERE delivered_at IS NULL AND available_at <= UTC_TIMESTAMP(6)
              AND (lease_until IS NULL OR lease_until <= UTC_TIMESTAMP(6))
            ORDER BY available_at,event_id LIMIT :limit FOR UPDATE SKIP LOCKED
        """.trimIndent()).bind("limit",limit).map { row,_ ->
            ClaimedDurableEvent(DurableEvent(row.get("event_id",String::class.java)!!,
                row.get("topic",String::class.java)!!,row.get("event_key",String::class.java),
                row.get("body",String::class.java)!!), token)
        }.all().collectList().flatMap { records ->
            reactor.core.publisher.Flux.fromIterable(records).concatMap { record ->
                client.sql("""UPDATE $table SET lease_token=:token,
                    lease_until=TIMESTAMPADD(MICROSECOND,:lease,UTC_TIMESTAMP(6)), attempts=attempts+1
                    WHERE event_id=:id""").bind("token",token).bind("lease",leaseMicros)
                    .bind("id",record.event.eventId).fetch().rowsUpdated()
            }.then(Mono.just(records))
        }
        TransactionSynchronizationManager.forCurrentTransaction().map { it.isActualTransactionActive }
            .onErrorReturn(NoTransactionException::class.java,false).flatMap { active ->
                if (active) Mono.error(IllegalStateException("relay claim must run outside caller transaction"))
                else transactions.transactional(action)
            }
    }

    /** A fenced acknowledgement cannot mark a record owned by a new relay or an expired lease. */
    fun markDelivered(record: ClaimedDurableEvent): Mono<Boolean> = client.sql("""
        UPDATE $table SET delivered_at=UTC_TIMESTAMP(6),lease_token=NULL,lease_until=NULL,last_error=NULL
        WHERE event_id=:id AND lease_token=:token AND delivered_at IS NULL AND lease_until>UTC_TIMESTAMP(6)
    """.trimIndent()).bind("id",record.event.eventId).bind("token",record.leaseToken)
        .fetch().rowsUpdated().map { it == 1L }

    /** All send failures remain pending. Store only an error classification, never a payload or player data. */
    fun releaseForRetry(record: ClaimedDurableEvent, retryDelay: Duration, errorKind: String): Mono<Boolean> = Mono.defer {
        require(!retryDelay.isNegative) { "retry delay must be nonnegative" }
        client.sql("""UPDATE $table SET lease_token=NULL,lease_until=NULL,
            available_at=TIMESTAMPADD(MICROSECOND,:delay,UTC_TIMESTAMP(6)),last_error=:error
            WHERE event_id=:id AND lease_token=:token AND delivered_at IS NULL""")
            .bind("delay",retryDelay.toNanos()/1000).bind("error",errorKind.take(255))
            .bind("id",record.event.eventId).bind("token",record.leaseToken).fetch().rowsUpdated().map { it == 1L }
    }

    /** Pending rows never expire. Each call deletes at most limit already-acknowledged old rows. */
    fun cleanupDelivered(retention: Duration, limit: Int): Mono<Long> = Mono.defer {
        require(limit in 1..1000) { "cleanup limit must be 1..1000" }
        client.sql("""DELETE FROM $table WHERE delivered_at < TIMESTAMPADD(MICROSECOND,:age,UTC_TIMESTAMP(6))
            ORDER BY delivered_at,event_id LIMIT :limit""").bind("age",-positiveMicros(retention))
            .bind("limit",limit).fetch().rowsUpdated()
    }

    private fun positiveMicros(duration: Duration): Long {
        require(!duration.isNegative && !duration.isZero && duration.toNanos()/1000>0) { "duration must be positive" }
        return duration.toNanos()/1000
    }
}
