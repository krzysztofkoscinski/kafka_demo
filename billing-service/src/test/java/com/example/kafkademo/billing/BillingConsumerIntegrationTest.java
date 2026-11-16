package com.example.kafkademo.billing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.kafkademo.contracts.OrderCreatedEvent;
import com.example.kafkademo.contracts.OrderTopics;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
@Import(BillingConsumerIntegrationTest.TestRepositoryConfiguration.class)
class BillingConsumerIntegrationTest {

    private static final String DLT = OrderTopics.ORDER_CREATED + ".DLT";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.8.0")
    );

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @Autowired
    private TestPaymentRepository paymentRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void consumesOrderCreatedEventAndCreatesPayment() throws Exception {
        OrderCreatedEvent event = newEvent();

        kafkaTemplate.send(OrderTopics.ORDER_CREATED, event.orderId(), event).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Payment payment = paymentRepository.findByEventId(event.eventId()).orElseThrow();
            assertThat(payment.orderId()).isEqualTo(event.orderId());
            assertThat(payment.amount()).isEqualByComparingTo(event.amount());
            assertThat(payment.createdAt()).isEqualTo(event.createdAt());
        });
    }

    @Test
    void retriesTransientFailureBeforeCreatingPayment() throws Exception {
        OrderCreatedEvent event = newEvent();
        paymentRepository.failNextAttempts(event.eventId(), 2);

        kafkaTemplate.send(OrderTopics.ORDER_CREATED, event.orderId(), event).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(paymentRepository.attemptCount(event.eventId())).isEqualTo(3);
            assertThat(paymentRepository.findByEventId(event.eventId())).isPresent();
        });
    }

    @Test
    void duplicateEventCreatesOnlyOnePayment() throws Exception {
        OrderCreatedEvent event = newEvent();

        kafkaTemplate.send(OrderTopics.ORDER_CREATED, event.orderId(), event).get(10, TimeUnit.SECONDS);
        kafkaTemplate.send(OrderTopics.ORDER_CREATED, event.orderId(), event).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(paymentRepository.attemptCount(event.eventId())).isEqualTo(2);
            assertThat(paymentRepository.successfulInsertCount(event.eventId())).isEqualTo(1);
        });
    }

    @Test
    void malformedAndUnknownPayloadsAreSentToDltWithoutBlockingLaterEvents() throws Exception {
        assertSentToDlt("not-json");
        assertSentToDlt("null");
        assertSentToDlt("""
                {"eventId":"event-unknown","orderId":"order-unknown","createdAt":"2026-01-01T10:00:00Z","amount":12.50,"unexpected":"field"}
                """.trim());

        OrderCreatedEvent validEvent = newEvent();
        kafkaTemplate.send(OrderTopics.ORDER_CREATED, validEvent.orderId(), validEvent)
                .get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(paymentRepository.findByEventId(validEvent.eventId())).isPresent()
        );
    }

    @Test
    void sendsEventToDltAfterRetryLimitIsExhausted() throws Exception {
        OrderCreatedEvent event = newEvent();
        paymentRepository.failNextAttempts(event.eventId(), 3);

        try (KafkaConsumer<String, String> dltConsumer = new KafkaConsumer<>(consumerProperties())) {
            dltConsumer.subscribe(List.of(DLT));
            kafkaTemplate.send(OrderTopics.ORDER_CREATED, event.orderId(), event)
                    .get(10, TimeUnit.SECONDS);

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(paymentRepository.attemptCount(event.eventId())).isEqualTo(3)
            );
            ConsumerRecord<String, String> deadLetter = awaitDltRecord(dltConsumer, event.orderId());
            assertThat(objectMapper.readValue(deadLetter.value(), OrderCreatedEvent.class)).isEqualTo(event);
            assertThat(paymentRepository.findByEventId(event.eventId())).isEmpty();
        }
    }

    private void assertSentToDlt(String payload) throws Exception {
        String key = UUID.randomUUID().toString();
        try (KafkaConsumer<String, String> dltConsumer = new KafkaConsumer<>(consumerProperties())) {
            dltConsumer.subscribe(List.of(DLT));
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
                producer.send(new ProducerRecord<>(OrderTopics.ORDER_CREATED, key, payload))
                        .get(10, TimeUnit.SECONDS);
            }

            ConsumerRecord<String, String> record = awaitDltRecord(dltConsumer, key);
            assertThat(record.value()).isEqualTo(payload);
        }
    }

    private static ConsumerRecord<String, String> awaitDltRecord(
            KafkaConsumer<String, String> consumer,
            String key
    ) {
        AtomicReference<ConsumerRecord<String, String>> foundRecord = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(100)).records(DLT)) {
                if (record.key().equals(key)) {
                    foundRecord.set(record);
                    break;
                }
            }
            assertThat(foundRecord.get()).isNotNull();
        });
        return foundRecord.get();
    }

    private static Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    private static Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    private static OrderCreatedEvent newEvent() {
        return new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                Instant.now(),
                new BigDecimal("29.95")
        );
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestRepositoryConfiguration {

        @Bean
        @Primary
        TestPaymentRepository testPaymentRepository() {
            return new TestPaymentRepository();
        }
    }

    static class TestPaymentRepository extends InMemoryPaymentRepository {

        private final ConcurrentMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> failuresRemaining = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> successfulInserts = new ConcurrentHashMap<>();

        void failNextAttempts(String eventId, int count) {
            failuresRemaining.put(eventId, new AtomicInteger(count));
        }

        int attemptCount(String eventId) {
            return attempts.getOrDefault(eventId, new AtomicInteger()).get();
        }

        int successfulInsertCount(String eventId) {
            return successfulInserts.getOrDefault(eventId, new AtomicInteger()).get();
        }

        @Override
        public boolean saveIfAbsent(Payment payment) {
            attempts.computeIfAbsent(payment.eventId(), ignored -> new AtomicInteger()).incrementAndGet();
            AtomicInteger remaining = failuresRemaining.get(payment.eventId());
            if (remaining != null && remaining.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
                throw new IllegalStateException("Simulated transient payment storage failure");
            }
            boolean inserted = super.saveIfAbsent(payment);
            if (inserted) {
                successfulInserts.computeIfAbsent(payment.eventId(), ignored -> new AtomicInteger())
                        .incrementAndGet();
            }
            return inserted;
        }
    }
}
