package com.kumar.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kumar.dto.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class KafkaMessageListener {

    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 3000, multiplier = 1.5, maxDelay = 15000), exclude = {NullPointerException.class})// 3 topic N-1
    @KafkaListener(topics = "${app.topic.name}", groupId = "jt-group-e")
    public void consumeEvents(Customer customer, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
        try {
            log.info("Received Retry Message: {} from {} offset {}", new ObjectMapper().writeValueAsString(customer), topic, offset);
            //validate restricted Customer-ID before process the records
            List<Integer> restrictedIpList = Stream.of(1, 2, 3, 4, 6,8).collect(Collectors.toList());
            if (restrictedIpList.contains(customer.getId())) {
                throw new RuntimeException("Invalid Customer ID received !");
            }

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @DltHandler
    public void listenDLT(Customer customer, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("DLT Received from retry : {} , from {} , offset {}",customer.getEmail(),topic,offset);
    }

    //consume the message from partition-2
    @KafkaListener(topics = "kumar-cust", groupId = "jt-group-c",
            topicPartitions = {@TopicPartition(topic = "kumar-cust", partitions = {"2"})})
    public void consumePartitionEvents(Customer customer) {
        log.info("consumer consume the partition customer message {} ", customer.toString());
    }

    //consume the Customer message
    @KafkaListener(topics = "kumar-cust", groupId = "jt-group-c")
    public void consumeEvents(Customer customer) {
        log.info("consumer consume the customer message {} ", customer.toString());
    }

    //consume the Plain String message
    @KafkaListener(topics = "kumar-str",groupId = "jt-group-s")
    public void consumer(String message) {
        log.info("consumer consume the string message {} ", message);
    }

}