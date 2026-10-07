package com.crp.system.libs.kafka.publisher.durable.mysql

import io.r2dbc.spi.ConnectionFactories
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.math.BigDecimal
import java.time.Duration

@EnabledIfEnvironmentVariable(named="FINANCIAL_OUTBOX_MYSQL_URL",matches="r2dbc:mysql://.+/reporting_integrity")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MySqlPocketStateReaderTest {
    private val adminUrl: String=System.getenv("FINANCIAL_OUTBOX_MYSQL_URL")
    private val factory=ConnectionFactories.get(adminUrl.removeSuffix("reporting_integrity")+"reporting_integrity_lib")
    private val client=DatabaseClient.create(factory)
    private val tx=TransactionalOperator.create(R2dbcTransactionManager(factory))
    private val reader=MySqlPocketStateReader(client,"pocket_capture_probe")
    private val wait=Duration.ofSeconds(15)

    @BeforeAll
    fun initialize() {
        DatabaseClient.create(ConnectionFactories.get(adminUrl)).sql("CREATE DATABASE IF NOT EXISTS reporting_integrity_lib")
            .fetch().rowsUpdated().block(wait)
        client.sql("CREATE TABLE IF NOT EXISTS pocket_capture_probe (id VARCHAR(255) PRIMARY KEY,amount DECIMAL(40,20) NOT NULL,pocket_version BIGINT NOT NULL DEFAULT 0)")
            .fetch().rowsUpdated().block(wait)
    }
    @BeforeEach
    fun prepare() {
        tx.transactional(client.sql("DELETE FROM pocket_capture_probe").fetch().rowsUpdated()
            .then(client.sql("INSERT INTO pocket_capture_probe VALUES ('p1',100,0)").fetch().rowsUpdated())).block(wait)
    }

    @Test
    fun `locked post-write capture rolls back with the source transaction and rejects detached reads`() {
        assertThrows<IllegalStateException> { reader.read("p1").block(wait) }
        assertThrows<IllegalStateException> {
            tx.transactional(update("-10").then(reader.read("p1")).flatMap { captured ->
                assertThat(captured.balance).isEqualByComparingTo("90")
                assertThat(captured.version).isEqualTo(1)
                assertThat(captured.recordedAt.nano%1000).isZero()
                Mono.error<Void>(IllegalStateException("rollback pocket and capture"))
            }).block(wait)
        }
        val source=tx.transactional(reader.read("p1")).block(wait)!!
        assertThat(source.balance).isEqualByComparingTo("100")
        assertThat(source.version).isZero()
    }

    @Test
    fun `concurrent writers capture their own exact post-write balance and version before releasing lock`() {
        val firstUpdated=Sinks.one<Boolean>()
        val first=tx.transactional(update("-10").doOnSuccess { firstUpdated.tryEmitValue(true) }
            .then(Mono.delay(Duration.ofMillis(100))).then(reader.read("p1")))
        val second=firstUpdated.asMono().then(tx.transactional(update("20").then(reader.read("p1"))))
        val captured=Mono.zip(first,second).block(wait)!!
        assertThat(captured.t1.balance).isEqualByComparingTo("90")
        assertThat(captured.t1.version).isEqualTo(1)
        assertThat(captured.t2.balance).isEqualByComparingTo("110")
        assertThat(captured.t2.version).isEqualTo(2)
    }

    @Test
    fun `no-pocket outcome clock is database UTC time in the caller transaction`() {
        assertThrows<IllegalStateException> { reader.currentTime().block(wait) }
        val captured=tx.transactional(client.sql("SET timestamp=1770091506").fetch().rowsUpdated()
            .then(reader.currentTime())).block(wait)!!
        assertThat(captured).isEqualTo(java.time.LocalDateTime.parse("2026-02-03T04:05:06"))
    }

    private fun update(delta: String)=client.sql("UPDATE pocket_capture_probe SET amount=amount+:delta,pocket_version=pocket_version+1 WHERE id='p1'")
        .bind("delta",BigDecimal(delta)).fetch().rowsUpdated()
}
