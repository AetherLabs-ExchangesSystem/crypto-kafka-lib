package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.testing.RecordingEventPublisher
import com.crp.system.libs.kafka.publisher.testsupport.Harness
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Clock

class RawJsonAcknowledgementTest {
    @Test
    fun `raw JSON future waits for broker acknowledgement and preserves exact body`() {
        Harness().use { h ->
            val acknowledgement = java.util.concurrent.CompletableFuture<(SendOutcome) -> Unit>()
            h.sender.script = { _, _, callback -> acknowledgement.complete(callback) }
            val future = h.publisher.publishJsonWithResult("funds", "42", " {\"pocketVersion_\":7} ")
            val call = h.sender.nextCall()
            assertThat(future).isNotDone()
            assertThat(call.record.payload).isEqualTo(" {\"pocketVersion_\":7} ")
            acknowledgement.awaitResult()(SendOutcome.Delivered(2, 71))
            assertThat(future.awaitResult().offset).isEqualTo(71)
        }
    }

    @Test
    fun `disabled raw JSON future fails instead of claiming delivery`() {
        val publisher = DisabledEventPublisher("reporting", Clock.systemUTC())
        assertThat(publisher.publishJsonWithResult("funds", "42", "{}").awaitFailure().stage)
            .isEqualTo(PublishFailureStage.CHANNEL_UNAVAILABLE)
    }

    @Test
    fun `recording publisher raw JSON result records unchanged body and invalid JSON fails`() {
        val publisher = RecordingEventPublisher("reporting")
        publisher.publishJsonWithResult("funds", "42", " {\"pocketVersion_\":7} ").awaitResult()
        assertThat(publisher.recorded.single().json).isEqualTo(" {\"pocketVersion_\":7} ")
        assertThat(publisher.publishJsonWithResult("funds", "42", null).awaitFailure().stage)
            .isEqualTo(PublishFailureStage.SERIALIZATION)
    }
}
