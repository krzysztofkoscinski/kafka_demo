# Kafka Testing Demo

Portfolio project demonstrating test strategies for two independently runnable Spring Boot services communicating through Apache Kafka.

## Architecture

```text
POST /orders
     |
     v
Order Service -- OrderCreated --> Kafka topic `orders` --> Billing Service
                                                               |
                                                  success      | invalid / retries exhausted
                                                     v         v
                                            in-memory payments `orders.DLT`
```

Order Service validates a request, creates an order and an `OrderCreated` event, then responds with `202 Accepted` after Kafka acknowledges the send. Billing Service consumes the event and stores a payment in an in-memory repository. The repository is intentionally simple: its data is lost when Billing Service restarts.

The shared event contract contains:

| Field | Meaning |
| --- | --- |
| `eventId` | Unique identifier for this event; the idempotency key |
| `orderId` | Order identifier; also the Kafka message key |
| `createdAt` | Event creation time in UTC |
| `amount` | Positive order amount |

## Technology

- Java 17
- Spring Boot 3.4.5 and Spring Kafka
- Maven
- JUnit 5
- Testcontainers 2.0.3
- Awaitility 4.2.2
- Apache Kafka image `apache/kafka:3.8.0`

Versions are pinned in Maven configuration. Docker is needed for integration tests.

## Prerequisites

- JDK 17 or newer
- Maven 3.9.x
- Docker Desktop with the Linux engine enabled (for tests and local Kafka)

## Run locally

Start the local Kafka broker:

```bash
docker compose up -d kafka
```

Build both services (the first command also builds the shared contract) and run each executable JAR in a separate terminal:

```bash
mvn -pl order-service -am package
java -jar order-service/target/order-service-0.0.1-SNAPSHOT-exec.jar

mvn -pl billing-service -am package
java -jar billing-service/target/billing-service-0.0.1-SNAPSHOT-exec.jar
```

Both services default to Kafka at `localhost:9092`; override with `SPRING_KAFKA_BOOTSTRAP_SERVERS`. Order Service listens on port `8080`, Billing Service on `8081`.

Create an order:

```http
POST http://localhost:8080/orders
Content-Type: application/json

{
  "customerId": "customer-42",
  "amount": 149.99
}
```

The endpoint returns `202 Accepted` with an `orderId` once Kafka acknowledges the event. Invalid requests (blank customer ID or non-positive/missing amount) return `400 Bad Request` and are not published. Retrieve a processed payment using `GET http://localhost:8081/payments/orders/{orderId}`.

Stop the local broker when finished:

```bash
docker compose down
```

## Tests

Run all unit, integration and end-to-end tests from the repository root:

```bash
mvn test
```

Docker Desktop on Windows normally exposes its Linux engine through the `desktop-linux` context. If Testcontainers reports the wrong named pipe, inspect `docker context inspect desktop-linux`; for a PowerShell session that needs an explicit endpoint, set `$env:DOCKER_HOST = 'npipe:////./pipe/dockerDesktopLinuxEngine'` before running Maven.

| Level | Test | What it verifies |
| --- | --- | --- |
| Unit | `OrderServiceTest` | Order creation and event fields without Kafka |
| Unit | `PaymentServiceTest` | Mapping an order event to a payment |
| Producer integration | `OrderProducerIntegrationTest` | HTTP validation, topic, message key and event payload against Kafka in Testcontainers |
| Consumer integration | `BillingConsumerIntegrationTest` | Payment creation, retry, duplicate handling, malformed payload and DLT routing |
| End-to-end | `OrderToPaymentE2ETest` | POST request through Order Service, Kafka delivery and payment lookup through Billing Service |

Integration tests start their own Kafka containers; no manually running broker is needed. Tests use unique order IDs and consumer groups to avoid stale records affecting assertions. Awaitility bounds asynchronous success checks; tests do not use `Thread.sleep()`.

## Retry, idempotency and invalid events

- **Transient processing failure:** Billing retries after `100 ms`, up to two retries after the initial delivery (three total attempts).
- **DLT:** after retries are exhausted, the original message is published to `orders.DLT`. The same DLT is used immediately for non-retryable invalid events.
- **Invalid or unknown payload:** Billing reads the message as text, parses the shared event schema strictly, and checks required fields. Malformed JSON, missing/invalid fields and unknown JSON fields are rejected without retries and sent to the DLT; they do not block later valid records.
- **Idempotency:** payments are inserted with an atomic `putIfAbsent` keyed by `eventId`. Re-delivery of the same event does not create another payment. This guarantee is limited to this in-memory process; a production repository should enforce a durable unique constraint on `eventId`.
- **Ordering:** the demo topic has one partition, so Kafka orders records within that partition. This is not a global ordering guarantee across topics, and the in-memory repository is not durable.
- **Timeouts:** asynchronous integration assertions fail within a bounded Awaitility window (10–15 seconds) if the expected payment or DLT record does not appear.

Implementation and test rationale are described in [docs/testing-strategy.md](docs/testing-strategy.md).

## CI

GitHub Actions runs `mvn --batch-mode verify` on every pull request and pushes to `main`. GitHub-hosted Ubuntu runners provide Docker for Testcontainers.

## Project layout

```text
contracts/       Shared OrderCreated event contract and topic
order-service/   HTTP endpoint, event producer, unit and producer integration tests
billing-service/ Kafka listener, retry/DLT, idempotent in-memory payment repository, consumer tests
e2e-tests/       End-to-end test that starts both services against Testcontainers Kafka
.github/         Pull request CI workflow
docs/            Testing strategy and behavior guarantees
docker-compose.yml  Local Kafka broker
```

## Latest test result

Verified with `mvn --batch-mode verify` and Docker Desktop on 2026-11-16:

| Module | Tests | Failures | Errors |
| --- | ---: | ---: | ---: |
| Order Service (unit + producer integration) | 3 | 0 | 0 |
| Billing Service (unit + consumer integration) | 6 | 0 | 0 |
| End-to-end | 1 | 0 | 0 |
| **Total** | **10** | **0** | **0** |

The local Compose broker was also started and passed its healthcheck.
