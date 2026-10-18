package com.crp.system.libs.kafka.publisher.durable.mysql

import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.transaction.NoTransactionException
import org.springframework.transaction.reactive.TransactionSynchronizationManager
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.LocalDateTime

internal fun checkedTableName(name: String): String {
    require(Regex("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?").matches(name)) { "invalid durable table name" }
    return name.split('.').joinToString(".") { "`$it`" }
}

/** Reject detached/autocommit inserts: the exact DatabaseClient factory must already be bound. */
internal fun <T : Any> inCallerTransaction(client: DatabaseClient, action: () -> Mono<T>): Mono<T> =
    TransactionSynchronizationManager.forCurrentTransaction()
        .onErrorMap(NoTransactionException::class.java) { IllegalStateException("durable recording requires caller transaction", it) }
        .flatMap { context ->
            if (!context.isActualTransactionActive || context.isCurrentTransactionReadOnly || !context.hasResource(client.connectionFactory))
                Mono.error(IllegalStateException("durable recording requires caller writable transaction bound to DatabaseClient"))
            else Mono.defer(action)
        }

/** The no-op duplicate UPDATE holds its row lock until the caller commits; compare every captured field. */
internal fun insertImmutableRecord(
    client: DatabaseClient, table: String, keys: List<String>, values: Map<String, Any?>,
): Mono<Void> {
    val columns = values.keys.joinToString(",") { "`$it`" }
    val parameters = values.keys.joinToString(",") { ":$it" }
    val sql = "INSERT INTO $table ($columns) VALUES ($parameters) ON DUPLICATE KEY UPDATE `${keys.first()}`=`${keys.first()}`"
    fun bind(spec: DatabaseClient.GenericExecuteSpec, entries: Map<String, Any?>): DatabaseClient.GenericExecuteSpec =
        entries.entries.fold(spec) { statement, (name, value) ->
            if (value == null) statement.bindNull(name, String::class.java) else statement.bind(name, value)
        }
    val where = keys.joinToString(" AND ") { "`$it`=:$it" }
    return bind(client.sql(sql), values).fetch().rowsUpdated().then(
        bind(client.sql("SELECT $columns FROM $table WHERE $where"), values.filterKeys { it in keys })
            .map { row, _ -> values.filter { (name, expected) ->
                val actual = if (expected == null) row.get(name) else row.get(name,expected.javaClass)
                if (actual is BigDecimal && expected is BigDecimal) actual.compareTo(expected) != 0 else actual != expected
            }.keys.toList() }.one().switchIfEmpty(Mono.error(DurableRecordConflictException()))
            .flatMap { conflicts -> if (conflicts.isEmpty()) Mono.empty<Void>() else Mono.error(DurableRecordConflictException(conflicts)) },
    )
}

internal fun validateDecimal(value: BigDecimal) {
    require(value.stripTrailingZeros().scale() <= 20 && value.precision() - value.scale() <= 20) { "amount must fit DECIMAL(40,20) exactly" }
}
internal fun validateTime(value: LocalDateTime?) {
    require(value == null || value.nano % 1000 == 0) { "UTC capture time must have at most microsecond precision" }
}
