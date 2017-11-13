package ru.dreamkas.statstics.streaming.development;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import ru.dreamkas.statstics.streaming.configuration.AppConfig;

@Component
@Profile("development")
@EnableScheduling
public class DevelopmentProducer {

    private final List<String> warehouse;
    private final Random rnd;
    private final Path warehousePath = Paths.get("/home/p.tykvin/dump");

    private final KafkaTemplate<Integer, String> template;
    private final String topic;

    @Autowired
    public DevelopmentProducer(KafkaTemplate<Integer, String> template, AppConfig config) throws IOException {
        this.template = template;
        this.topic = config.getStatisticTopic();
        warehouse = Files.lines(warehousePath).collect(Collectors.toList());
        rnd = new Random(warehouse.size());
    }

    @PostConstruct
    @Scheduled(fixedRate = 200)
    public void run() throws Exception {
        String value = warehouse.get(rnd.nextInt(warehouse.size()));
        template.send(topic, value);
    }
}
