package ru.dreamkas.statstics.streaming.consumers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import ru.dreamkas.statstics.streaming.constants.ConsumingGroups;
import ru.dreamkas.statstics.streaming.handlers.StreamRecordHandler;

//@Component
public class CashesStatiticsConsumer {

    private final StreamRecordHandler recordHandler;

    @Autowired
    public CashesStatiticsConsumer(StreamRecordHandler recordHandler) {this.recordHandler = recordHandler;}

    @KafkaListener(id = ConsumingGroups.STREAM, topics = "cashes-statistics-development")
    public void consume(String value) {
        recordHandler.handle(value);
    }
}
