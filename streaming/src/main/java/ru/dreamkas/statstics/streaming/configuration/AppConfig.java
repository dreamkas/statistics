package ru.dreamkas.statstics.streaming.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import ru.dreamkas.statstics.streaming.constants.ConsumingGroups;

import java.util.HashMap;
import java.util.Map;

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
    @Value("${spark.hadoop.home}")
    private String hadoopHome;

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
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ConsumingGroups.STREAM);
        props.put("auto.offset.reset", "latest");
        props.put("enable.auto.commit", true);
        return props;
    }


    public Map<String, Object> getProducerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }


    public SparkConf getSparkConf() {
        return new SparkConf().setMaster(master).setAppName(appName);
    }

    public String getHadoopHome() {
        return hadoopHome;
    }
}
