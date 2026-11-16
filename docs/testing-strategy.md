# Testing strategy

The project tests the contract at separate boundaries so failures identify whether the problem is order creation, Kafka publishing, billing consumption, or recovery behavior.

## Test levels

| Level | Boundary | Why |
| --- | --- | --- |
| Unit | Order and payment services | Verify small business decisions without the broker |
| Producer integration | HTTP endpoint → Kafka topic | Verify the actual serialized record, key and validation behavior |
| Consumer integration | Kafka topic → payment repository | Verify asynchronous handling, retries, deduplication and recovery |
| End-to-end | Order HTTP API → Kafka → Billing HTTP API | Verify that independently started services complete the whole flow |

All Kafka integration and E2E tests use an isolated Kafka Testcontainer. Awaitility is used for asynchronous assertions with explicit upper bounds; there are no fixed sleeps.

## Event contract and payload errors

`OrderCreated` is shared in the `contracts` module and has four required fields:

- `eventId`: unique event identity and the Billing idempotency key;
- `orderId`: order identity and Kafka key;
- `createdAt`: UTC event timestamp;
- `amount`: positive amount.

Billing deserializes records as text before parsing. It rejects malformed JSON, unknown JSON fields, missing identifiers/timestamps, and non-positive amounts as non-retryable. Such records are published to `orders.DLT`, rather than repeatedly blocking the `orders` partition. Future contract changes should be versioned and covered by compatibility tests; this demo intentionally treats unrecognized fields as invalid.

## Retry, DLT and duplicate semantics

Billing's `DefaultErrorHandler` uses a fixed `100 ms` backoff with two retries (three attempts total). `InvalidOrderEventException` bypasses retries. After retry exhaustion, `DeadLetterPublishingRecoverer` sends the failed message to `orders.DLT`, preserving the payload and adding Spring Kafka recovery headers.

The in-memory repository atomically inserts a payment only if its `eventId` is new. This prevents a repeated delivery from creating a second payment in the running process. It is not durable across restarts and does not provide Kafka/database exactly-once semantics. A production database should use a unique constraint and transactionally coordinate payment persistence with message acknowledgement.

The topic is configured with one partition. Kafka's ordering guarantee is therefore limited to records in that partition; it does not imply ordering between `orders` and `orders.DLT` or across a future multi-partition deployment.

## Reproducibility

Java, Spring Boot, Testcontainers, Awaitility and the Kafka container image versions are pinned. Run the full suite with:

```bash
mvn test
```

CI runs the equivalent Maven verification on pull requests. The integration tests manage their own broker lifecycle and use unique order IDs and consumer groups so previous test records do not satisfy current assertions.
