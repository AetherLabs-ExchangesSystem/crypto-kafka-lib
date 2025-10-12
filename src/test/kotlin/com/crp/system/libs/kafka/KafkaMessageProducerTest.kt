package com.crp.system.libs.kafka

import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate

class KafkaMessageProducerTest {

    @Test
    fun `should create kafka message producer instance`() {
        // Given
        val mockKafkaTemplate = mockk<KafkaTemplate<String, String>>()
        val producer = KafkaMessageProducer(mockKafkaTemplate)
        
        // When & Then
        assertThat(producer).isNotNull()
    }

    @Test
    fun `should have service annotation`() {
        // Given & When
        val serviceAnnotation = KafkaMessageProducer::class.annotations.find { 
            it.annotationClass.simpleName == "Service" 
        }
        
        // Then
        assertThat(serviceAnnotation).isNotNull()
    }
}