package com.crp.system.libs.kafka

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import java.util.HashMap;


@Configuration
data class KafkaProducerConfig (
    @Value("\${kafka.bootstrap-servers}")
    private val kafkaServerUrl: String,
    @Value("\${spring.kafka.producer.properties.max.request.size:1048576}")
    private val maxRequestSize: Int,
) {

    @Bean
    fun producerFactory(): ProducerFactory<String, String> {
        val configProps: MutableMap<String, Any> = HashMap()
        configProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaServerUrl
        configProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        configProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        configProps[ProducerConfig.MAX_REQUEST_SIZE_CONFIG] = maxRequestSize
        return DefaultKafkaProducerFactory(configProps)
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(producerFactory())
    }
}