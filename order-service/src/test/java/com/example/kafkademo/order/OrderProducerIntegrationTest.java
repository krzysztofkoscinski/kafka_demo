package com.example.kafkademo.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.kafkademo.contracts.OrderCreatedEvent;
import com.example.kafkademo.contracts.OrderTopics;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class OrderProducerIntegrationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.8.0")
    );

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void postOrderPublishesOrderCreatedEventToKafka() throws Exception {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "order-test-" + UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(List.of(OrderTopics.ORDER_CREATED));

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<CreateOrderRequest> request = new HttpEntity<>(
                    new CreateOrderRequest("customer-42", new BigDecimal("149.99")),
                    headers
            );
            var response = restTemplate.postForEntity(
                    "http://localhost:" + port + "/orders",
                    request,
                    CreateOrderResponse.class
            );

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            assertThat(response.getBody()).isNotNull();
            String orderId = response.getBody().orderId();
            assertThat(orderId).isNotBlank();

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record = null;
                for (var candidate : consumer.poll(Duration.ofMillis(100)).records(OrderTopics.ORDER_CREATED)) {
                    if (candidate.key().equals(orderId)) {
                        record = candidate;
                        break;
                    }
                }
                if (record == null) {
                    throw new AssertionError("Order event was not published");
                }
                OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);

                assertThat(record.key()).isEqualTo(orderId);
                assertThat(event.eventId()).isNotBlank();
                assertThat(event.orderId()).isEqualTo(orderId);
                assertThat(event.createdAt()).isNotNull();
                assertThat(event.amount()).isEqualByComparingTo("149.99");
            });
        }
    }

    @Test
    void rejectsInvalidOrder() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        var response = restTemplate.postForEntity(
                "http://localhost:" + port + "/orders",
                new HttpEntity<>("{\"customerId\":\"\",\"amount\":0}", headers),
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private record CreateOrderRequest(String customerId, BigDecimal amount) {
    }

    private record CreateOrderResponse(String orderId) {
    }
}
