package ru.dreamkas.statstics.streaming.terminal;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface TerminalOperation {
    void finish(Dataset<Row> result);
}
