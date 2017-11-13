package ru.dreamkas.statstics.streaming.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SparkInitializerImpl implements SparkInitializer {
    private SparkSession session;
    private SparkConf sparkConf;

    @Autowired
    public SparkInitializerImpl(AppConfig config) {
        sparkConf = new SparkConf().setMaster(config.getMaster()).setAppName(config.getAppName());
        session = SparkSession.builder().config(sparkConf).getOrCreate();
    }

    @Override
    public SparkSession getSession() {
        return session;
    }

    public SparkConf getSparkConf() {
        return sparkConf;
    }
}
