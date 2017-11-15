package ru.dreamkas.statistics.restpipe;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@EnableKafka
public class KafkaConfiguration {

    private final AppConfig config;

    @Autowired
    public KafkaConfiguration(AppConfig config) {
        this.config = config;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(ConsumerFactory<Integer, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(config.getConsumerConfigs());
    }


    @Bean
    @Profile("development")
    public ProducerFactory<Integer, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(config.getProducerConfigs());
    }

    @Bean
    @Profile("development")
    public KafkaTemplate<Integer, String> kafkaTemplate(ProducerFactory<Integer, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
