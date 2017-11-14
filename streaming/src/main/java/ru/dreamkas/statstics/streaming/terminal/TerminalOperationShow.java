package ru.dreamkas.statstics.streaming.terminal;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("development")
public class TerminalOperationShow implements TerminalOperation {
    @Override
    public void finish(Dataset<Row> result) {
        result.show();
    }
}
