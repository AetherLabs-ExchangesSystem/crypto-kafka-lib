package com.crp.system.libs.kafka.publisher.durable.mysql

import org.springframework.r2dbc.core.DatabaseClient
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.LocalDateTime

/** The exact locked source state, with UTC database capture time. This is not an asserted commit timestamp. */
data class PocketState(val balance: BigDecimal, val version: Long, val recordedAt: LocalDateTime)

/** Shared technical capture. Caller owns amount/version changes and the surrounding writable transaction. */
class MySqlPocketStateReader(
    private val client: DatabaseClient, pocketTable: String = "accounts.pockets",
) {
    private val table = checkedTableName(pocketTable)

    /** SELECT FOR UPDATE keeps the exact captured balance/version stable until the caller commits or rolls back. */
    fun read(pocketId: String): Mono<PocketState> = inCallerTransaction(client) {
        client.sql("SELECT amount,pocket_version,UTC_TIMESTAMP(6) AS recorded_at FROM $table WHERE id=:id FOR UPDATE")
            .bind("id",pocketId).map { row,_ ->
                PocketState(row.get("amount",BigDecimal::class.java)
                    ?: throw IllegalStateException("source pocket amount is missing"),
                    row.get("pocket_version",java.lang.Long::class.java)?.toLong()
                        ?: throw IllegalStateException("source pocket version is missing"),
                    row.get("recorded_at",LocalDateTime::class.java)!!)
            }.one().switchIfEmpty(Mono.error(IllegalStateException("source pocket is missing")))
    }

    /** Database UTC capture time for a terminal outcome with no changed or available pocket. */
    fun currentTime(): Mono<LocalDateTime> = inCallerTransaction(client) {
        client.sql("SELECT UTC_TIMESTAMP(6) AS recorded_at")
            .map { row,_ -> row.get("recorded_at",LocalDateTime::class.java)!! }.one()
    }
}
