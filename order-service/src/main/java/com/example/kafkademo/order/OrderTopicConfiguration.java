package com.example.kafkademo.order;

import com.example.kafkademo.contracts.OrderTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class OrderTopicConfiguration {

    @Bean
    NewTopic ordersTopic() {
        return TopicBuilder.name(OrderTopics.ORDER_CREATED)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
