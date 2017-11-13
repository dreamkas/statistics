package ru.dreamkas.statstics.streaming.handlers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import ru.dreamkas.statstics.streaming.configuration.SparkInitializer;

@Component
public class SparkHandler implements StreamRecordHandler {

    private final SparkInitializer sparkInitializer;
    private SparkSession spark;

    @Autowired
    public SparkHandler(SparkInitializer sparkInitializer) {
        this.sparkInitializer = sparkInitializer;
        spark = sparkInitializer.getSession();
    }

    @Override
    public void handle(String value) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        try (JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3))) {
            JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
//            stream.foreachRDD((v1, v2) -> v1.foreach(record -> System.out.println(record.value())));

            stream.map(ConsumerRecord::value)
                .filter(s -> !s.isEmpty())
                .foreachRDD((rdd, time) -> {
                    Dataset<Row> json = spark
                        .read()
                        .json(rdd);

                    try {
                        aggregateFirmware(json)
                            .show();
//                            .write().saveAsTable("TestDump");

                    } catch (Exception ignored) {
                    }
                });
            jssc.start();              // Start the computation
            jssc.awaitTermination();   // Wait for the computation to terminate
        }
    }
}
