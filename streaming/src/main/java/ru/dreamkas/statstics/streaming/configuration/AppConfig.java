package ru.dreamkas.statstics.streaming.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
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
}
