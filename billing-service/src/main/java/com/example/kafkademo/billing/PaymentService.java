package com.example.kafkademo.billing;

import com.example.kafkademo.contracts.OrderCreatedEvent;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;

    public PaymentService(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    public boolean createPayment(OrderCreatedEvent event) {
        return paymentRepository.saveIfAbsent(new Payment(
                event.eventId(),
                event.orderId(),
                event.amount(),
                event.createdAt()
        ));
    }
}
