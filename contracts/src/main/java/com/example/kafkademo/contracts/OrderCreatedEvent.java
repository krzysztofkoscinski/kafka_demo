package com.example.kafkademo.contracts;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String eventId,
        String orderId,
        Instant createdAt,
        BigDecimal amount
) {
}
