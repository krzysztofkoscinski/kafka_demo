package com.example.kafkademo.billing;

import com.example.kafkademo.contracts.OrderCreatedEvent;
import com.example.kafkademo.contracts.OrderTopics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedListener {

    private final PaymentService paymentService;
    private final ObjectMapper objectMapper;

    public OrderCreatedListener(PaymentService paymentService, ObjectMapper objectMapper) {
        this.paymentService = paymentService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = OrderTopics.ORDER_CREATED)
    public void onOrderCreated(String payload) {
        OrderCreatedEvent event;
        try {
            event = objectMapper.readerFor(OrderCreatedEvent.class)
                    .with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .readValue(payload);
        } catch (JsonProcessingException exception) {
            throw new InvalidOrderEventException("Invalid OrderCreated event payload", exception);
        }

        if (event == null
                || event.eventId() == null || event.eventId().isBlank()
                || event.orderId() == null || event.orderId().isBlank()
                || event.createdAt() == null
                || event.amount() == null
                || event.amount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new InvalidOrderEventException("OrderCreated event has missing or invalid fields");
        }

        paymentService.createPayment(event);
    }
}
