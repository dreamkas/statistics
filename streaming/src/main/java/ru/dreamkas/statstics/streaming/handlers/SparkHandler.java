package ru.dreamkas.statstics.streaming.handlers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.dreamkas.statstics.streaming.configuration.AppConfig;
import ru.dreamkas.statstics.streaming.configuration.SparkInitializer;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

@Component
public class SparkHandler implements StreamRecordHandler {

    private final SparkInitializer sparkInitializer;
    private final AppConfig config;

    @Autowired
    public SparkHandler(
            SparkInitializer sparkInitializer,
            AppConfig config
    ) {
        this.sparkInitializer = sparkInitializer;
        this.config = config;
    }

    @PostConstruct
    public void startStreaming() {
        Executors.newCachedThreadPool().execute(() -> {
            List<String> topics = Collections.singletonList(config.getStatisticTopic());
            Map<String, Object> kafkaParams = config.getConsumerConfigs();
            SparkSession spark = sparkInitializer.getSession();
            try (JavaStreamingContext jssc = new JavaStreamingContext(config.getSparkConf(), Durations.seconds(3))) {
                JavaInputDStream<ConsumerRecord<String, String>> stream =
                        KafkaUtils.createDirectStream(
                                jssc,
                                LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                        );

                stream.map(ConsumerRecord::value)
                        .filter(s -> !s.isEmpty())
                        .foreachRDD((rdd, time) -> {
                            spark
                                    .read()
                                    .json(rdd).show();

                        });
                jssc.start();              // Start the computation
                jssc.awaitTermination();   // Wait for the computation to terminate
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void handle(String value) {

    }
}
