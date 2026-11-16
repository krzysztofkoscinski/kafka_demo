package com.example.kafkademo.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.kafkademo.billing.BillingApplication;
import com.example.kafkademo.billing.Payment;
import com.example.kafkademo.order.OrderApplication;
import com.example.kafkademo.order.OrderController;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class OrderToPaymentE2ETest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.8.0")
    );

    @Test
    void orderSubmittedThroughApiCreatesBillingPayment() {
        String bootstrapServers = KAFKA.getBootstrapServers();
        try (ConfigurableApplicationContext billing = startApplication(
                BillingApplication.class, bootstrapServers
        ); ConfigurableApplicationContext order = startApplication(
                OrderApplication.class, bootstrapServers
        )) {
            int orderPort = ((WebServerApplicationContext) order).getWebServer().getPort();
            int billingPort = ((WebServerApplicationContext) billing).getWebServer().getPort();
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            ResponseEntity<OrderController.CreateOrderResponse> response = restTemplate.exchange(
                    "http://localhost:" + orderPort + "/orders",
                    HttpMethod.POST,
                    new HttpEntity<>(Map.of("customerId", "e2e-customer", "amount", "42.50"), headers),
                    OrderController.CreateOrderResponse.class
            );

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            assertThat(response.getBody()).isNotNull();
            String orderId = response.getBody().orderId();

            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                ResponseEntity<Payment> paymentResponse = restTemplate.getForEntity(
                        "http://localhost:" + billingPort + "/payments/orders/" + orderId,
                        Payment.class
                );
                assertThat(paymentResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
                assertThat(paymentResponse.getBody()).isNotNull();
                assertThat(paymentResponse.getBody().orderId()).isEqualTo(orderId);
                assertThat(paymentResponse.getBody().amount()).isEqualByComparingTo(
                        new BigDecimal("42.50")
                );
            });
        }
    }

    private static ConfigurableApplicationContext startApplication(
            Class<?> applicationClass,
            String bootstrapServers
    ) {
        return new SpringApplicationBuilder(applicationClass)
                .run(
                        "--server.port=0",
                        "--spring.kafka.bootstrap-servers=" + bootstrapServers,
                        "--spring.kafka.consumer.group-id=e2e-billing"
                );
    }
}
