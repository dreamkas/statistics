package ru.dreamkas.statstics.streaming.development;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import ru.dreamkas.statstics.streaming.configuration.AppConfig;

//@Service
//@Profile("development")
//@DependsOn("sparkHandler")
public class DevelopmentProducer {

    private final List<String> warehouse;
    private final Random rnd;
    private final KafkaTemplate<Integer, String> template;
    private final String topic;

    @Autowired
    public DevelopmentProducer(KafkaTemplate<Integer, String> template, AppConfig config, @Value("${warehouse-path:/home/p.tykvin/dump}") String warehousePath) throws IOException {
        this.template = template;
        this.topic = config.getStatisticTopic();
        Path path = Paths.get(warehousePath);
        if (!Files.exists(path)) {
            throw new FileNotFoundException("Warehouse not found");
        }
        warehouse = Files.lines(path).collect(Collectors.toList());
        rnd = new Random(warehouse.size());
    }

    @PostConstruct
    public void run() throws Exception {
        Executors.newCachedThreadPool().execute(() -> {
            while (!Thread.interrupted()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                String value = warehouse.get(rnd.nextInt(warehouse.size()));
                template.send(topic, value);
            }
        });
    }
}
