package ru.dreamkas.statstics.streaming.configuration;

import org.apache.spark.sql.SparkSession;

public interface SparkInitializer {
    SparkSession getSession();
}
