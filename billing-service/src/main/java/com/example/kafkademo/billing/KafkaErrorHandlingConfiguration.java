package com.example.kafkademo.billing;

import com.example.kafkademo.contracts.OrderCreatedEvent;
import com.example.kafkademo.contracts.OrderTopics;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlingConfiguration {

    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(
                        OrderTopics.ORDER_CREATED + ".DLT",
                        record.partition()
                )
        );
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(100L, 2L)
        );
        errorHandler.addNotRetryableExceptions(InvalidOrderEventException.class);
        return errorHandler;
    }
}
