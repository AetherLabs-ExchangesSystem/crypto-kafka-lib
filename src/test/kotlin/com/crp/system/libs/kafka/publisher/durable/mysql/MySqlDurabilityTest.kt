package com.crp.system.libs.kafka.publisher.durable.mysql

import com.crp.system.libs.kafka.publisher.api.EventPublisher
import com.crp.system.libs.kafka.publisher.api.PublishResult
import com.crp.system.libs.kafka.publisher.core.DisabledEventPublisher
import com.crp.system.libs.kafka.publisher.core.SendOutcome
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import com.crp.system.libs.kafka.publisher.testing.RecordingEventPublisher
import com.crp.system.libs.kafka.publisher.testsupport.FakeRecordSender
import com.crp.system.libs.kafka.publisher.testsupport.Harness
import io.r2dbc.spi.ConnectionFactories
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.api.assertThrows
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import java.time.Clock
import java.time.Duration
import java.time.LocalDateTime
import java.math.BigDecimal
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** Uses only the named scratch database on the owner-provided loopback MySQL. */
@EnabledIfEnvironmentVariable(named="FINANCIAL_OUTBOX_MYSQL_URL",matches="r2dbc:mysql://.+/reporting_integrity")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MySqlDurabilityTest {
    private val adminUrl: String = System.getenv("FINANCIAL_OUTBOX_MYSQL_URL")
    private val scratchUrl: String = adminUrl.removeSuffix("reporting_integrity")+"reporting_integrity_lib"
    private val factory = ConnectionFactories.get(scratchUrl)
    private val client = DatabaseClient.create(factory)
    private val tx = TransactionalOperator.create(R2dbcTransactionManager(factory))
    private val store = MySqlDurableEventStore(client, "financial_event_outbox")
    private val event = DurableEvent("pocket:p1:1", "funds", "42", " {\"pocketVersion_\":1} ")
    private val wait = Duration.ofSeconds(15)

    @BeforeAll
    fun initialize() {
        val admin = DatabaseClient.create(ConnectionFactories.get(adminUrl))
        admin.sql("CREATE DATABASE IF NOT EXISTS reporting_integrity_lib").fetch().rowsUpdated().block(wait)
        listOf("financial_event_outbox", "pocket_balance_records", "financial_outcomes").forEach {
            client.sql("DROP TABLE IF EXISTS $it").fetch().rowsUpdated().block(wait)
        }
        val schema = javaClass.getResource("/durable/mysql-schema.sql")!!.readText()
        schema.split(";").map(String::trim).filter(String::isNotEmpty).forEach { client.sql(it).fetch().rowsUpdated().block(wait) }
        client.sql("CREATE TABLE IF NOT EXISTS money_probe (id INT PRIMARY KEY, amount DECIMAL(40,20) NOT NULL)").fetch().rowsUpdated().block(wait)
    }

    @BeforeEach
    fun prepare() {
        tx.transactional(reactor.core.publisher.Flux.fromIterable(listOf("financial_event_outbox","pocket_balance_records","financial_outcomes","money_probe"))
            .concatMap { client.sql("DELETE FROM $it").fetch().rowsUpdated() }
            .then(client.sql("INSERT INTO money_probe VALUES (1,100)").fetch().rowsUpdated())).block(wait)
    }

    private fun commit(event: DurableEvent = this.event) = tx.transactional(store.append(event)).block(wait)
    private fun scalar(sql: String): Long = client.sql(sql).map { row, _ -> (row.get(0) as Number).toLong() }.one().block(wait)!!
    private fun pending() = scalar("SELECT COUNT(*) FROM financial_event_outbox WHERE delivered_at IS NULL")
    private fun relay(publisher: EventPublisher = RecordingEventPublisher("reporting")) =
        MySqlDurableEventRelay(store, publisher, Duration.ofSeconds(3), Duration.ZERO, Duration.ofSeconds(1))

    @Test
    fun `append rejects autocommit and unrelated transaction`() {
        assertThrows<Exception> { store.append(event).block(wait) }
        val unrelated = ConnectionFactories.get(scratchUrl)
        assertThrows<Exception> {
            TransactionalOperator.create(R2dbcTransactionManager(unrelated)).transactional(store.append(event)).block(wait)
        }
        assertThat(pending()).isZero()
    }

    @Test
    fun `failed money transaction rolls back typed record and transport record`() {
        val records = MySqlFinancialRecordStore(client, store, "pocket_balance_records", "financial_outcomes")
        val balance = balance()
        assertThrows<IllegalStateException> {
            tx.transactional(client.sql("UPDATE money_probe SET amount=90 WHERE id=1").fetch().rowsUpdated()
                .then(records.recordBalance(balance, event)).then(Mono.error<Void>(IllegalStateException("rollback money")))).block(wait)
        }
        assertThat(scalar("SELECT amount FROM money_probe WHERE id=1")).isEqualTo(100)
        assertThat(scalar("SELECT COUNT(*) FROM pocket_balance_records")).isZero()
        assertThat(pending()).isZero()
    }

    @Test
    fun `committed record survives restart before publication and is acknowledged once in effect`() {
        commit()
        val restartedStore = MySqlDurableEventStore(DatabaseClient.create(factory), "financial_event_outbox")
        val publisher = RecordingEventPublisher("reporting")
        val result = MySqlDurableEventRelay(restartedStore, publisher).relayOnce(2).block(wait)!!
        assertThat(result.delivered).isEqualTo(1)
        assertThat(publisher.recorded.single().json).isEqualTo(event.body)
        assertThat(publisher.recorded.single().headers["event_id"]).isEqualTo(event.eventId)
        assertThat(relay(publisher).relayOnce(2).block(wait)!!.claimed).isZero()
    }

    @Test
    fun `failed disabled and queue-full broker acknowledgements retain retryable record`() {
        commit()
        val disabled = relay(DisabledEventPublisher("disabled", Clock.systemUTC())).relayOnce(2).block(wait)!!
        assertThat(disabled.failed).isEqualTo(1)
        assertThat(pending()).isEqualTo(1)
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        Harness(ChannelSettings(enabled=true,lanes=1,queueCapacity=1), FakeRecordSender(FakeRecordSender.stuck(entered,release))).use { h ->
            h.publisher.publishJson("funds", "first", "{}")
            assertThat(entered.await(5,TimeUnit.SECONDS)).isTrue()
            h.publisher.publishJson("funds", "queued", "{}")
            try { assertThat(relay(h.publisher).relayOnce(2).block(wait)!!.failed).isEqualTo(1) }
            finally { release.countDown() }
        }
        assertThat(pending()).isEqualTo(1)
        Harness(sender=FakeRecordSender(FakeRecordSender.throwing(IllegalStateException("broker unavailable")))).use { h ->
            assertThat(relay(h.publisher).relayOnce(2).block(wait)!!.failed).isEqualTo(1)
        }
        assertThat(pending()).isEqualTo(1)
        assertThat(relay().relayOnce(2).block(wait)!!.delivered).isEqualTo(1)
    }

    @Test
    fun `unacknowledged send times out and remains retryable`() {
        commit()
        Harness(sender=FakeRecordSender { _,_,_ -> }).use { h ->
            assertThat(relay(h.publisher).relayOnce(2).block(wait)!!.failed).isEqualTo(1)
        }
        assertThat(pending()).isEqualTo(1)
        assertThat(relay().relayOnce(2).block(wait)!!.delivered).isEqualTo(1)
    }

    @Test
    fun `expired lease recovers and stale token cannot acknowledge new claim`() {
        commit()
        val first = store.claim(1, Duration.ofSeconds(1)).block(wait)!!.single()
        assertThat(store.claim(1, Duration.ofSeconds(1)).block(wait)).isEmpty()
        client.sql("UPDATE financial_event_outbox SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND").fetch().rowsUpdated().block(wait)
        val second = store.claim(1, Duration.ofSeconds(2)).block(wait)!!.single()
        assertThat(second.leaseToken).isNotEqualTo(first.leaseToken)
        assertThat(store.markDelivered(first).block(wait)).isFalse()
        assertThat(store.markDelivered(second).block(wait)).isTrue()
    }

    @Test
    fun `concurrent relays claim disjoint bounded records`() {
        repeat(6) { commit(event.copy(eventId="event:$it")) }
        val batches = Mono.zip(store.claim(2,Duration.ofSeconds(3)),store.claim(2,Duration.ofSeconds(3))).block(wait)!!
        assertThat(batches.t1).hasSize(2)
        assertThat(batches.t2).hasSize(2)
        assertThat((batches.t1+batches.t2).map { it.event.eventId }).doesNotHaveDuplicates()
    }

    @Test
    fun `stable identity accepts only exact topic key and body agreement`() {
        commit(); commit()
        assertThat(pending()).isEqualTo(1)
        listOf(event.copy(topic="other"),event.copy(key=null),event.copy(body="{}"),event.copy(body=event.body.trim())).forEach {
            assertThrows<DurableRecordConflictException> { commit(it) }
        }
        assertThat(pending()).isEqualTo(1)
    }

    @Test
    fun `typed records and transport reject inconsistent true-key replay`() {
        val records=MySqlFinancialRecordStore(client,store,"pocket_balance_records","financial_outcomes")
        val balance=balance()
        tx.transactional(records.recordBalance(balance,event)).block(wait)
        tx.transactional(records.recordBalance(balance,event)).block(wait)
        assertThrows<DurableRecordConflictException> {
            tx.transactional(records.recordBalance(balance.copy(balanceAfter=BigDecimal("99")),event)).block(wait)
        }
        val outcome=FinancialOutcomeRecord(sourceService="debit",trackingId="tracking",operation="debit",status="rejected",
            direction="debit",amount=BigDecimal("10"),currencyId="eur",currencyCode="EUR",playerId=42,
            pocketId="p1",walletId="w1",walletSysName="MAIN",currencySysname="EUR",recordedAt=balance.recordedAt)
        val result=event.copy(eventId="debit:tracking:debit",topic="settlement",body="{\"status\":\"rejected\"}")
        tx.transactional(records.recordOutcome(outcome,result)).block(wait)
        tx.transactional(records.recordOutcome(outcome,result)).block(wait)
        assertThrows<DurableRecordConflictException> {
            tx.transactional(records.recordOutcome(outcome.copy(status="committed"),result)).block(wait)
        }
        assertThat(scalar("SELECT COUNT(*) FROM financial_outcomes")).isEqualTo(1)
    }

    @Test
    fun `cleanup removes only delivered old rows in bounded batches`() {
        repeat(5) { commit(event.copy(eventId="event:$it")) }
        relay().relayOnce(3).block(wait)
        client.sql("UPDATE financial_event_outbox SET delivered_at=UTC_TIMESTAMP(6)-INTERVAL 40 DAY WHERE delivered_at IS NOT NULL").fetch().rowsUpdated().block(wait)
        assertThat(store.cleanupDelivered(Duration.ofDays(35),2).block(wait)).isEqualTo(2)
        assertThat(pending()).isEqualTo(2)
        assertThat(scalar("SELECT COUNT(*) FROM financial_event_outbox")).isEqualTo(3)
    }

    private fun balance()=PocketBalanceRecord(pocketId="p1",pocketVersion=1,balanceAfter=BigDecimal("90"),
        walletId="w1",currencyId="eur",playerId=42,currencyCode="EUR",currencySysname="EUR",walletSysName="MAIN",
        walletKind="MAIN",sourceService="debit",operation="debit",recordedAt=LocalDateTime.parse("2026-10-07T10:00:00"))
}
