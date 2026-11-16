package com.example.kafkademo.billing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import com.example.kafkademo.contracts.OrderCreatedEvent;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PaymentServiceTest {

    private final InMemoryPaymentRepository repository = new InMemoryPaymentRepository();
    private final PaymentService paymentService = new PaymentService(repository);

    @Test
    void createsPaymentFromOrderEvent() {
        OrderCreatedEvent event = new OrderCreatedEvent(
                "event-1", "order-1", Instant.parse("2026-01-01T10:00:00Z"),
                new BigDecimal("49.95")
        );

        paymentService.createPayment(event);

        assertThat(repository.findByEventId("event-1")).contains(
                new Payment("event-1", "order-1", new BigDecimal("49.95"), event.createdAt())
        );
    }
}
