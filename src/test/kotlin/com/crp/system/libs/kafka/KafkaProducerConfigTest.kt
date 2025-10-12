package com.crp.system.libs.kafka

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class KafkaProducerConfigTest {

    @Test
    fun `should create kafka producer config instance`() {
        // Given
        val config = KafkaProducerConfig("localhost:9092")
        
        // When & Then
        assertThat(config).isNotNull()
    }

    @Test
    fun `should have configuration annotation`() {
        // Given & When
        val configAnnotation = KafkaProducerConfig::class.annotations.find { 
            it.annotationClass.simpleName == "Configuration" 
        }
        
        // Then
        assertThat(configAnnotation).isNotNull()
    }
}