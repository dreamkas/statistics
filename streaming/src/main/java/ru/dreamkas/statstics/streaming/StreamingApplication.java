package ru.dreamkas.statstics.streaming;

import java.util.concurrent.Executors;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import ru.dreamkas.statstics.streaming.handlers.SparkHandler;

@SpringBootApplication
public class StreamingApplication {

    @Autowired
    public StreamingApplication(SparkHandler sparkHandler, JavaStreamingContext jssc) {
        Executors.newCachedThreadPool().execute(() -> {
            try {
                sparkHandler.startStreaming();
                jssc.start();
                jssc.awaitTermination();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });

    }

    public static void main(String[] args) {
        SpringApplication.run(StreamingApplication.class, args);
    }
}
