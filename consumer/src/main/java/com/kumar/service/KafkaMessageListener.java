package com.kumar.service;

import com.kumar.dto.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {

    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    //consume the message from partition-2
    @KafkaListener(topics = "kumar-cust", groupId = "jt-group-c",
            topicPartitions = {@TopicPartition(topic = "kumar-cust", partitions = {"2"})})
    public void consumePartitionEvents(Customer customer) {
        log.info("consumer consume the partition customer message {} ", customer.toString());
    }

    @KafkaListener(topics = "kumar-cust", groupId = "jt-group-c")
    public void consumeEvents(Customer customer) {
        log.info("consumer consume the customer message {} ", customer.toString());
    }

    @KafkaListener(topics = "kumar-str",groupId = "jt-group-s")
    public void consumer(String message) {
        log.info("consumer consume the string message {} ", message);
    }

}