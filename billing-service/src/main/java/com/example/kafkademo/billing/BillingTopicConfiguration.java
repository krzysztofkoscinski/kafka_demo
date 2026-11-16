package com.example.kafkademo.billing;

import com.example.kafkademo.contracts.OrderTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class BillingTopicConfiguration {

    @Bean
    NewTopic ordersTopic() {
        return TopicBuilder.name(OrderTopics.ORDER_CREATED)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic ordersDeadLetterTopic() {
        return TopicBuilder.name(OrderTopics.ORDER_CREATED + ".DLT")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
