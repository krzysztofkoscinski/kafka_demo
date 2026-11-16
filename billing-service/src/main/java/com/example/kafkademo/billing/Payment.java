package com.example.kafkademo.billing;

import java.math.BigDecimal;
import java.time.Instant;

public record Payment(String eventId, String orderId, BigDecimal amount, Instant createdAt) {
}
