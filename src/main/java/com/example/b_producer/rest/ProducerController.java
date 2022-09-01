package com.example.b_producer.rest;

import com.example.b_producer.model.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.*;

import java.util.Date;

@RestController
@RequestMapping("/generate")
public class ProducerController {

    @Autowired
    private KafkaTemplate<String, Message> kafkaTemplate;

    @PostMapping("/{n}")
    public void producer(@PathVariable int n){
        Message m = new Message("a,b,c", new Date());
       try {
           for (int i = 0; i <n; i++) {
               ListenableFuture<SendResult<String, Message>> future = kafkaTemplate.send("topic", "key", m );
               future.addCallback(System.out::println, System.err::println);
               kafkaTemplate.flush();
           }
       }
       catch (Exception e){
           System.out.println(e.getMessage());
       }
    }

}
