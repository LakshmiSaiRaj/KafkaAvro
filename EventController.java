package com.javatechie.controller;

import com.javatechie.dto.Customer;
import com.javatechie.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer-app")
public class EventController {
    @Autowired
    private KafkaMessagePublisher publisher;

    @PostMapping("/publishh")
    public void sendEvents(@RequestBody Customer customer) {
        publisher.sendEventsToTopic(customer);
    }
}
