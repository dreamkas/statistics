package ru.dreamkas.statstics.streaming.aggreagators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.col;

@Component
public class VersionsAggregatot implements Aggregator {
    @Override
    public Dataset<Row> aggregate(Dataset<Row> json) {
        Dataset<Row> result = null;
        try {
            result = json.select("version")
                .groupBy("version")
                .count()
                .filter(col("version").isNotNull())
                .orderBy(col("count").desc());
        } catch (Exception ignored) {
        }
        return result;
    }
}
