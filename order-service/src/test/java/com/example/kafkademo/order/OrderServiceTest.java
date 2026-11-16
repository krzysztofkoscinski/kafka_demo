package com.example.kafkademo.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.kafkademo.contracts.OrderCreatedEvent;
import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class OrderServiceTest {

    private final RecordingEventPublisher eventProducer = new RecordingEventPublisher();
    private final OrderService orderService = new OrderService(eventProducer);

    @Test
    void createsOrderAndPublishesEventWithStableFields() {
        String orderId = orderService.createOrder(
                new OrderService.CreateOrderRequest("customer-42", new BigDecimal("19.99"))
        ).join();

        OrderCreatedEvent event = eventProducer.event;
        assertThat(orderId).isEqualTo(event.orderId());
        assertThat(event.eventId()).isNotBlank();
        assertThat(event.createdAt()).isNotNull();
        assertThat(event.amount()).isEqualByComparingTo("19.99");
    }

    private static class RecordingEventPublisher implements OrderEventPublisher {
        private OrderCreatedEvent event;

        @Override
        public CompletableFuture<Void> publish(OrderCreatedEvent event) {
            this.event = event;
            return CompletableFuture.completedFuture(null);
        }
    }
}
