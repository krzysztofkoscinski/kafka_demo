package com.example.kafkademo.order;

import com.example.kafkademo.contracts.OrderCreatedEvent;
import com.example.kafkademo.contracts.OrderTopics;
import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventProducer implements OrderEventPublisher {

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<Void> publish(OrderCreatedEvent event) {
        return kafkaTemplate.send(OrderTopics.ORDER_CREATED, event.orderId(), event)
                .thenApply(result -> null);
    }
}
