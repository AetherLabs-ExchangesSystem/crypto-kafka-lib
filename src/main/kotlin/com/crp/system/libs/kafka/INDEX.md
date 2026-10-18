# Kafka library primitives

| Module | Role | Key exports |
|---|---|---|
| `KafkaMessageProducer.kt` | Existing direct producer | `KafkaMessageProducer` |
| `publisher/api` | Publisher contract, delivery results, extension ports, stable IDs | `EventPublisher`, `EventPublisherChannels`, `PublishResult`, `EventIds` |
| `publisher/core` | Channel queues, backpressure, retries and failure handling | Internal implementation |
| `publisher/adapters` | Kafka, Gson, retry policies and metrics | `GsonEventSerializer`, `ExponentialRetryPolicy` |
| `publisher/spring` | Optional configured channels | `EventPublisherAutoConfiguration`, `EventPublisherProperties` |
| `publisher/testing` | Service test publisher | `RecordingEventPublisher`, `RecordingEventPublisherChannels` |
| `publisher/durable/mysql` | Optional caller-transaction recording, leased relay and typed immutable records | `MySqlDurableEventStore`, `MySqlDurableEventRelay`, `MySqlFinancialRecordStore`, `MySqlPocketStateReader`, `DurableEvent`, `PocketBalanceRecord`, `FinancialOutcomeRecord` |
