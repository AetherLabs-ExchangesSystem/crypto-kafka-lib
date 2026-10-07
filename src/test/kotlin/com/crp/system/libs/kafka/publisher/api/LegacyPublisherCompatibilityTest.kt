package com.crp.system.libs.kafka.publisher.api

import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.net.URLClassLoader
import java.nio.file.Path
import javax.tools.ToolProvider

class LegacyPublisherCompatibilityTest {
    @TempDir lateinit var temp: Path

    @Test
    fun `implementation compiled against original interface loads and safely rejects acknowledged raw send`() {
        val api=temp.resolve("com/crp/system/libs/kafka/publisher/api")
        java.nio.file.Files.createDirectories(api)
        val old=api.resolve("EventPublisher.java")
        java.nio.file.Files.writeString(old,"""
            package com.crp.system.libs.kafka.publisher.api;
            public interface EventPublisher {
                String getChannel();
                void publish(String topic,String key,Object event,java.util.Map<String,String> headers);
                void publishJson(String topic,String key,String json,java.util.Map<String,String> headers);
                java.util.concurrent.CompletableFuture<PublishResult> publishWithResult(String topic,String key,Object event,java.util.Map<String,String> headers);
            }
        """.trimIndent())
        val legacy=temp.resolve("LegacyPublisher.java")
        java.nio.file.Files.writeString(legacy,"""
            public class LegacyPublisher implements com.crp.system.libs.kafka.publisher.api.EventPublisher {
                public String getChannel() { return "legacy"; }
                public void publish(String t,String k,Object e,java.util.Map<String,String> h) {}
                public void publishJson(String t,String k,String j,java.util.Map<String,String> h) {}
                public java.util.concurrent.CompletableFuture<com.crp.system.libs.kafka.publisher.api.PublishResult> publishWithResult(String t,String k,Object e,java.util.Map<String,String> h) { return null; }
            }
        """.trimIndent())
        val compiled=ToolProvider.getSystemJavaCompiler().run(null,null,null,"-cp",System.getProperty("java.class.path"),
            "-d",temp.toString(),old.toString(),legacy.toString())
        assertThat(compiled).isZero()
        // Parent-first loading intentionally resolves the new interface and only loads the old implementation here.
        URLClassLoader(arrayOf(temp.toUri().toURL()),javaClass.classLoader).use { loader ->
            val publisher=loader.loadClass("LegacyPublisher").getDeclaredConstructor().newInstance() as EventPublisher
            assertThat(publisher.publishJsonWithResult("funds","42","{}").awaitFailure().stage)
                .isEqualTo(PublishFailureStage.CHANNEL_UNAVAILABLE)
        }
    }
}
