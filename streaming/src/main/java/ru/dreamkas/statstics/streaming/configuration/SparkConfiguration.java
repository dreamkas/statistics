package ru.dreamkas.statstics.streaming.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfiguration {

    private final AppConfig config;

    @Autowired
    public SparkConfiguration(AppConfig config) {
        this.config = config;
        String hadoopHome = config.getHadoopHome();
        if (hadoopHome != null) {
            System.setProperty("hadoop.home.dir", hadoopHome);
        }
    }

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
                .setMaster(config.getMaster()) //The name of application. This will appear in the UI and in log data.
                //conf.set("spark.ui.port", "7077");    //Port for application's dashboard, which shows memory and workload data.
//        sparkConf.set("dynamicAllocation.enabled", "false");  //Which scales the number of executors registered with this application up and down based on the workload
                //conf.set("spark.cassandra.connection.host", "localhost"); //Cassandra Host Adddress/IP
//        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");  //For serializing objects that will be sent over the network or need to be cached in serialized form.
//        sparkConf.set("spark.driver.allowMultipleContexts", "true");
                .setAppName(config.getAppName());
    }

    @Bean
    public JavaSparkContext javaSparkContext(SparkConf conf) {
        return new JavaSparkContext(new SparkContext(conf));
    }

    @Bean
    public SparkSession sparkSession(JavaSparkContext jsc) {
        return SparkSession
                .builder()
                .sparkContext(jsc.sc())
                .appName("Java Spark SQL basic example")
                .getOrCreate();

    }

    @Bean
    public JavaStreamingContext javaStreamingContext(SparkConf conf) {
        return new JavaStreamingContext(conf, new Duration(2000));
    }
}
