package ru.dreamkas.statstics.streaming.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfiguration {

    private final AppConfig config;

    @Autowired
    public SparkConfiguration(AppConfig config) {
        this.config = config;
    }

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
            .setMaster(config.getMaster())
            .setAppName(config.getAppName());
    }


    @Bean
    public SparkSession sparkSession(JavaStreamingContext jssc) {
        return SparkSession
            .builder()
            .sparkContext(jssc.sparkContext().sc())
            .getOrCreate();
    }

    @Bean
    public JavaStreamingContext javaStreamingContext(SparkConf conf) {
        return new JavaStreamingContext(conf, new Duration(2000));
    }
}
