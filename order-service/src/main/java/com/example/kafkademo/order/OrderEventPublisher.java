package com.example.kafkademo.order;

import com.example.kafkademo.contracts.OrderCreatedEvent;
import java.util.concurrent.CompletableFuture;

public interface OrderEventPublisher {

    CompletableFuture<Void> publish(OrderCreatedEvent event);
}
