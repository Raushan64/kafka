package com.kumar.service;

import com.kumar.dto.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    Logger log = LoggerFactory.getLogger(KafkaMessagePublisher.class);

    @Autowired
    private KafkaTemplate<String,String> templateString;

    @Autowired
    private KafkaTemplate<String, Customer> templateCustomer;

    @Value("${app.topic.name}")
    private String topicName;

    public void sendMessageToTopic(String message){
        CompletableFuture<SendResult<String, String>> future = templateString.send("kumar-str", message);
        future.whenComplete((result,ex)->{
            if (ex == null) {
                log.info("Sent String message: {}, with partition: {},  and offset: {}", message, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                log.info("Unable to send the String message: {}, due to: {}", message, ex.getMessage());
            }
        });

    }

    //send the message to specific partition and consumer read this
    public void sendEventsToPartitionTopic(Customer customer) {
        CompletableFuture<SendResult<String, Customer>> future = templateCustomer.send("kumar-cust", 2, "okays", customer);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent Customer message to Partition: {}, with partition: {},  and offset: {}", customer, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                log.info("Unable to send the Customer message: {}, due to: {}", customer.toString(), ex.getMessage());
            }
        });
    }

    //send the message to specific partition and consumer read this
    public void sendEventsToTopic(Customer customer) {
        CompletableFuture<SendResult<String, Customer>> future = templateCustomer.send("kumar-cust", customer);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent Customer message: {}, with partition: {},  and offset: {}", customer, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                log.info("Unable to send the Customer message: {}, due to: {}", customer.toString(), ex.getMessage());
            }
        });
    }

    //send the message to Handle the Error - retry
    public void sendEventsToErrorHandlingTopic(Customer customer) {
        CompletableFuture<SendResult<String, Customer>> future = templateCustomer.send(topicName, customer);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent Customer message to ErrorHandling: {}, with partition: {},  and offset: {}", customer, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                log.info("Unable to send the Customer message for ErrorHandling: {}, due to: {}", customer.toString(), ex.getMessage());
            }
        });
    }
}
