package com.crp.system.libs.kafka.publisher.nordbc

import com.crp.system.libs.kafka.KafkaProducerConfig
import com.crp.system.libs.kafka.publisher.api.EventPublisherChannels
import com.crp.system.libs.kafka.publisher.spring.EventPublisherAutoConfiguration
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner

class NoR2dbcContextTest {
    @Test
    fun `ordinary library starts and publishes without R2DBC or Reactor`() {
        listOf("org.springframework.r2dbc.core.DatabaseClient","io.r2dbc.spi.ConnectionFactory","reactor.core.publisher.Mono").forEach {
            assertThrows<ClassNotFoundException> { Class.forName(it) }
        }
        ApplicationContextRunner().withConfiguration(AutoConfigurations.of(EventPublisherAutoConfiguration::class.java))
            .withUserConfiguration(KafkaProducerConfig::class.java)
            .withPropertyValues("kafka.bootstrap-servers=127.0.0.1:1",
                "crypto.kafka.publisher.channels.reporting.enabled=true",
                "crypto.kafka.publisher.channels.reporting.producer.max-block-ms=200")
            .run { context ->
                assertThat(context).hasNotFailed()
                assertThat(context.getBean(EventPublisherChannels::class.java).get("reporting")
                    .publishJsonWithResult("t","k","{}").awaitFailure().stage.name).isEqualTo("DELIVERY_FAILED")
            }
    }
}
