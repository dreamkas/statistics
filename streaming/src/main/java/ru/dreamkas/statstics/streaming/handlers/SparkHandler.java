package ru.dreamkas.statstics.streaming.handlers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.dreamkas.statstics.streaming.configuration.AppConfig;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.List;

@Component
public class SparkHandler implements StreamRecordHandler {

    private final SparkSession ss;
    private final AppConfig config;
    private final JavaStreamingContext jssc;
    private final List<String> topics;

    @Autowired
    public SparkHandler(
            SparkSession ss,
            AppConfig config,
            JavaStreamingContext jssc
    ) {
        this.ss = ss;
        this.config = config;
        this.jssc = jssc;
        this.topics = Collections.singletonList(config.getStatisticTopic());
    }


    @PostConstruct
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
                    ss
                            .read()
                            .json(rdd).show();

                });
        jssc.start();              // Start the computation
        try {
            jssc.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handle(String value) {

    }


}
