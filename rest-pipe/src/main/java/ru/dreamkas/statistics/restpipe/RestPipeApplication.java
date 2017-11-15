package ru.dreamkas.statistics.restpipe;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@SpringBootApplication
public class RestPipeApplication {

    private final KafkaTemplate<Integer, String> template;
    private final AppConfig config;

    @Autowired
    public RestPipeApplication(
        KafkaTemplate<Integer, String> template,
        AppConfig config) {
        this.template = template;
        this.config = config;
    }

    @PostMapping
    public void input(@RequestBody String body) {
        template.send(config.getStatisticTopic(), body);
    }

    public static void main(String[] args) {
        SpringApplication.run(RestPipeApplication.class, args);
    }
}
