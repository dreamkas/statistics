package ru.dreamkas.statstics.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class StreamingApplication {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Hadoop\\");
        SpringApplication.run(StreamingApplication.class, args);
    }


    //    @KafkaListener(topics = Topics.CASHES_STATISTICS_TOPIC)
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        System.out.print("StreamingApplication.listen__");
        System.out.println("cr.toString() = " + cr.toString());
    }
}
