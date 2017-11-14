package ru.dreamkas.statstics.streaming.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AppConfig {
    @Value("${kafka.brokers}")
    private String brokers;
    @Value("${kafka.topics.statistics}")
    private String statisticTopic;

    @Value("${spark.master}")
    private String master;
    @Value("${spark.appName}")
    private String appName;

    public String getBrokers() {
        return brokers;
    }

    public String getStatisticTopic() {
        return statisticTopic;
    }

    public String getMaster() {
        return master;
    }

    public String getAppName() {
        return appName;
    }

    public Map<String, Object> getConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put("auto.offset.reset", "latest");
        props.put("enable.auto.commit", true);
        return props;
    }

    public Map<String, Object> getProducerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public SparkConf getSparkConf() {
        return new SparkConf().setMaster(master).setAppName(appName);
    }
}
