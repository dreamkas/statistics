package ru.dreamkas.statstics.streaming;

import java.util.concurrent.Executors;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import ru.dreamkas.statstics.streaming.handlers.SparkHandler;

@SpringBootApplication
public class StreamingApplication {

    private final SparkHandler sparkHandler;
    private final JavaStreamingContext jssc;

    @Autowired
    public StreamingApplication(SparkHandler sparkHandler, JavaStreamingContext jssc) {
        this.sparkHandler = sparkHandler;
        this.jssc = jssc;
    }

    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(StreamingApplication.class, args);
        StreamingApplication app = context.getBean(StreamingApplication.class);
        app.start();
    }

    public void start() throws InterruptedException {
        sparkHandler.startStreaming();
        jssc.start();
        jssc.awaitTermination();
    }
}
