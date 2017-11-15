package ru.dreamkas.statstics.streaming;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Service;

import ru.dreamkas.statstics.streaming.handlers.SparkHandler;

@Service
public class StreamingApplication {

    private final SparkHandler sparkHandler;
    private final JavaStreamingContext jssc;

    @Autowired
    public StreamingApplication(SparkHandler sparkHandler, JavaStreamingContext jssc) {
        this.sparkHandler = sparkHandler;
        this.jssc = jssc;
    }

    public static void main(String[] args) throws InterruptedException {
        ApplicationContext context = new AnnotationConfigApplicationContext(StreamingApplication.class.getPackage().getName());
        StreamingApplication app = context.getBean(StreamingApplication.class);
        app.start();
    }

    public void start() throws InterruptedException {
        sparkHandler.startStreaming();
        jssc.start();
        jssc.awaitTermination();
    }
}
