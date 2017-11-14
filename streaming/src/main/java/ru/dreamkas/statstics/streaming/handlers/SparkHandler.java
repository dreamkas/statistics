package ru.dreamkas.statstics.streaming.handlers;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import ru.dreamkas.statstics.streaming.aggreagators.Aggregator;
import ru.dreamkas.statstics.streaming.configuration.AppConfig;
import ru.dreamkas.statstics.streaming.terminal.TerminalOperation;

@Component
public class SparkHandler {

    private final SparkSession ss;
    private final AppConfig config;
    private final JavaStreamingContext jssc;
    private final List<String> topics;
    private final Aggregator aggregator;
    private final TerminalOperation terminalOperation;

    @Autowired
    public SparkHandler(
        SparkSession ss,
        AppConfig config,
        JavaStreamingContext jssc,
        Aggregator aggregator,
        TerminalOperation terminalOperation) {
        this.ss = ss;
        this.config = config;
        this.jssc = jssc;
        this.topics = Collections.singletonList(config.getStatisticTopic());
        this.aggregator = aggregator;
        this.terminalOperation = terminalOperation;
    }

    public void startStreaming() {
        JavaInputDStream<ConsumerRecord<String, String>> stream =
            KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, config.getConsumerConfigs())
            );
        stream.map(ConsumerRecord::value)
            .filter(s -> !s.isEmpty())
            .foreachRDD((rdd, time) -> {
                terminalOperation.finish(
                    aggregator.aggregate(
                        ss
                            .read()
                            .json(rdd)
                    )
                );
            });
    }
}
