package ru.dreamkas.statstics.streaming.aggreagators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Aggregator {
    Dataset<Row> aggregate(Dataset<Row> json);
}
