package ru.dreamkas.statstics.streaming;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class StreamingApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamingApplication.class, args);
    }


//    @KafkaListener(topics = Topics.CASHES_STATISTICS_TOPIC)
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        System.out.print("StreamingApplication.listen__");
        System.out.println("cr.toString() = " + cr.toString());
    }
}
