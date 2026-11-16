package com.example.kafkademo.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<CreateOrderResponse>> createOrder(
            @Valid @RequestBody CreateOrderRequest request
    ) {
        return orderService.createOrder(
                new OrderService.CreateOrderRequest(request.customerId(), request.amount())
        ).thenApply(orderId -> ResponseEntity.accepted().body(new CreateOrderResponse(orderId)));
    }

    public record CreateOrderRequest(
            @NotBlank String customerId,
            @NotNull @Positive BigDecimal amount
    ) {
    }

    public record CreateOrderResponse(String orderId) {
    }
}
