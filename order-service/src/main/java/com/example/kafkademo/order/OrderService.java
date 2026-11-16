package com.example.kafkademo.order;

import com.example.kafkademo.contracts.OrderCreatedEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final OrderEventPublisher eventProducer;

    public OrderService(OrderEventPublisher eventProducer) {
        this.eventProducer = eventProducer;
    }

    public CompletableFuture<String> createOrder(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                orderId,
                Instant.now(),
                request.amount()
        );
        return eventProducer.publish(event).thenApply(result -> orderId);
    }

    public record CreateOrderRequest(String customerId, BigDecimal amount) {
    }
}
