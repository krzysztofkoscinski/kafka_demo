package com.example.kafkademo.billing;

import java.util.Optional;

public interface PaymentRepository {

    boolean saveIfAbsent(Payment payment);

    Optional<Payment> findByEventId(String eventId);

    Optional<Payment> findByOrderId(String orderId);
}
