package com.kumar.controller;

import com.kumar.dto.Customer;
import com.kumar.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer-app")
public class EventController {

    @Autowired
    private KafkaMessagePublisher publisher;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message) {
        try {
            publisher.sendMessageToTopic(message);
            return ResponseEntity.ok("message published successfully ..");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }

    @PostMapping("/publish")
    public void sendEvents(@RequestBody Customer customer) {
        publisher.sendEventsToTopic(customer);
    }

    @PostMapping("/publish-partition")
    public void sendEventsToPartition(@RequestBody Customer customer) {
        publisher.sendEventsToPartitionTopic(customer);
    }

    @PostMapping("/publish-error-handling")
    public void sendEventsToErrorHandling(@RequestBody Customer customer) {
        publisher.sendEventsToErrorHandlingTopic(customer);
    }


}