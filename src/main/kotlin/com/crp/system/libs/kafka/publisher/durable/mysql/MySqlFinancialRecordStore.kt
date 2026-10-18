package com.crp.system.libs.kafka.publisher.durable.mysql

import org.springframework.r2dbc.core.DatabaseClient
import reactor.core.publisher.Mono

/** Technical persistence only: caller captures balances, increments versions and decides terminal outcomes. */
class MySqlFinancialRecordStore(
    private val client: DatabaseClient,
    private val outbox: MySqlDurableEventStore,
    balanceTable: String = "accounts.pocket_balance_records",
    outcomeTable: String = "accounts.financial_outcomes",
) {
    private val balances = checkedTableName(balanceTable)
    private val outcomes = checkedTableName(outcomeTable)
    init { require(client.connectionFactory === outbox.client.connectionFactory) { "typed record and outbox must use the same transaction factory" } }

    fun recordBalance(record: PocketBalanceRecord, event: DurableEvent): Mono<Void> = inCallerTransaction(client) {
        require(record.pocketVersion >= 0) { "pocket version must be nonnegative" }
        validateDecimal(record.balanceAfter);validateTime(record.recordedAt);validateTime(record.sourceBusinessOccurredAt)
        insertImmutableRecord(client,balances,listOf("pocket_id","pocket_version"),linkedMapOf(
            "pocket_id" to record.pocketId,"pocket_version" to record.pocketVersion,"balance_after" to record.balanceAfter,
            "wallet_id" to record.walletId,"currency_id" to record.currencyId,"player_id" to record.playerId,
            "currency_code" to record.currencyCode,"currency_sysname" to record.currencySysname,
            "wallet_sys_name" to record.walletSysName,"wallet_kind" to record.walletKind,
            "source_service" to record.sourceService,"operation" to record.operation,"tracking_id" to record.trackingId,
            "company_id" to record.companyId,"brand_id" to record.brandId,"recorded_at" to record.recordedAt,
            "source_business_occurred_at" to record.sourceBusinessOccurredAt,"movement_context" to record.movementContext,
            "event_id" to event.eventId,
        )).then(outbox.append(event))
    }

    fun recordOutcome(record: FinancialOutcomeRecord, event: DurableEvent): Mono<Void> = inCallerTransaction(client) {
        require(record.status in setOf("committed","rejected")) { "outcome must be terminal" }
        require(record.direction in setOf("debit","credit")) { "outcome direction must be debit or credit" }
        require(record.pocketVersion == null || record.pocketVersion >= 0) { "pocket version must be nonnegative" }
        require(record.amount.signum() >= 0) { "outcome amount must be nonnegative" }
        validateDecimal(record.amount);validateTime(record.recordedAt);validateTime(record.sourceBusinessOccurredAt)
        insertImmutableRecord(client,outcomes,listOf("source_service","tracking_id","operation"),linkedMapOf(
            "source_service" to record.sourceService,"tracking_id" to record.trackingId,"operation" to record.operation,
            "status" to record.status,"direction" to record.direction,"amount" to record.amount,
            "currency_id" to record.currencyId,"currency_code" to record.currencyCode,"currency_sysname" to record.currencySysname,
            "player_id" to record.playerId,"pocket_id" to record.pocketId,"wallet_id" to record.walletId,
            "pocket_version" to record.pocketVersion,"wallet_sys_name" to record.walletSysName,
            "partner_id" to record.partnerId,"partner_reference_id" to record.partnerReferenceId,"failure_reason" to record.failureReason,
            "company_id" to record.companyId,"brand_id" to record.brandId,"recorded_at" to record.recordedAt,
            "source_business_occurred_at" to record.sourceBusinessOccurredAt,"movement_context" to record.movementContext,
            "event_id" to event.eventId,
        )).then(outbox.append(event))
    }
}
