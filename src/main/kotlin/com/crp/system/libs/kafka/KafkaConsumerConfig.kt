package com.crp.system.libs.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteBufferDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory


@Configuration
data class KafkaConsumerConfig (
    @Value("\${kafka.bootstrap-servers}")
    private val kafkaServerUrl: String,
    @Value("\${kafka.consumer.group-id:#{null}}")
    private val kafkaGroupId: String?
) {

    @Bean
    @ConditionalOnProperty(name = ["kafka.consumer.group-id"])
    fun consumerFactory(): ConsumerFactory<String, ByteArray> {
        if (kafkaGroupId.isNullOrEmpty()) {
            throw IllegalArgumentException("Kafka consumer group-id must be set")
        }
        val configProps: MutableMap<String, Any> = HashMap()
        configProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaServerUrl
        configProps[ConsumerConfig.GROUP_ID_CONFIG] = kafkaGroupId
        configProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        configProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        return DefaultKafkaConsumerFactory(configProps)
    }

    @Bean
    @ConditionalOnProperty(name = ["kafka.consumer.group-id"])
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, ByteArray> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, ByteArray>()
        factory.setConsumerFactory(consumerFactory())
        // Enable manual acknowledgment mode to prevent message loss
        factory.containerProperties.ackMode = org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL
        return factory
    }
}