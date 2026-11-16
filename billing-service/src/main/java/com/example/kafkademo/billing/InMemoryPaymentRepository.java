package com.example.kafkademo.billing;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Repository;

@Repository
public class InMemoryPaymentRepository implements PaymentRepository {

    private final ConcurrentMap<String, Payment> paymentsByEventId = new ConcurrentHashMap<>();

    @Override
    public boolean saveIfAbsent(Payment payment) {
        return paymentsByEventId.putIfAbsent(payment.eventId(), payment) == null;
    }

    @Override
    public Optional<Payment> findByEventId(String eventId) {
        return Optional.ofNullable(paymentsByEventId.get(eventId));
    }

    @Override
    public Optional<Payment> findByOrderId(String orderId) {
        return paymentsByEventId.values().stream()
                .filter(payment -> payment.orderId().equals(orderId))
                .findFirst();
    }
}
