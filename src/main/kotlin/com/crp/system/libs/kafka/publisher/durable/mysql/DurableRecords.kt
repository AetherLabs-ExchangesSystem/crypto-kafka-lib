package com.crp.system.libs.kafka.publisher.durable.mysql

import java.math.BigDecimal
import java.time.LocalDateTime

/** Opaque, already serialized transport. The caller chooses its stable identity and business payload. */
data class DurableEvent(val eventId: String, val topic: String, val key: String?, val body: String)
data class ClaimedDurableEvent(val event: DurableEvent, val leaseToken: String)
data class RelayBatchResult(val claimed: Int, val delivered: Int, val failed: Int, val leaseLost: Int)

/** Never includes the record body, balances, or other player data in its message. */
class DurableRecordConflictException(columns: List<String> = emptyList()) :
    IllegalStateException("immutable durable record identity conflicts with existing columns: ${columns.joinToString()}")

/** Exact post-write snapshot captured while the caller still owns the pocket write lock. Times are UTC. */
data class PocketBalanceRecord(
    val pocketId: String, val pocketVersion: Long, val balanceAfter: BigDecimal,
    val walletId: String, val currencyId: String, val playerId: Long,
    val currencyCode: String, val currencySysname: String, val walletSysName: String, val walletKind: String,
    val sourceService: String, val operation: String, val recordedAt: LocalDateTime,
    val trackingId: String? = null, val companyId: String? = null, val brandId: String? = null,
    val sourceBusinessOccurredAt: LocalDateTime? = null, val movementContext: String? = null,
)

/** Terminal outcome supplied by money-domain code. Null business time/context marks an unknown legacy cohort. */
data class FinancialOutcomeRecord(
    val sourceService: String, val trackingId: String, val operation: String, val status: String, val direction: String,
    val amount: BigDecimal, val currencyId: String, val currencyCode: String, val playerId: Long,
    val pocketId: String?, val walletId: String?, val walletSysName: String?, val currencySysname: String?,
    val recordedAt: LocalDateTime,
    val pocketVersion: Long? = null, val partnerId: String? = null, val partnerReferenceId: String? = null,
    val failureReason: String? = null, val companyId: String? = null, val brandId: String? = null,
    val sourceBusinessOccurredAt: LocalDateTime? = null, val movementContext: String? = null,
)
