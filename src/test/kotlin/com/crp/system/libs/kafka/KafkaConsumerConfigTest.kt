package com.crp.system.libs.kafka

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class KafkaConsumerConfigTest {

    @Test
    fun `should create kafka consumer config instance`() {
        // Given
        val config = KafkaConsumerConfig("localhost:9092", "test-group")
        
        // When & Then
        assertThat(config).isNotNull()
    }

    @Test
    fun `should have configuration annotation`() {
        // Given & When
        val configAnnotation = KafkaConsumerConfig::class.annotations.find { 
            it.annotationClass.simpleName == "Configuration" 
        }
        
        // Then
        assertThat(configAnnotation).isNotNull()
    }
}