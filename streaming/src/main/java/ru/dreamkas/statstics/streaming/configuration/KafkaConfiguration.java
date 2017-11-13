package ru.dreamkas.statstics.streaming.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

@Configuration
@EnableKafka
public class KafkaConfiguration {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(ConsumerFactory<Integer, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactory(AppConfig config) {
        return new DefaultKafkaConsumerFactory<>(config.getConsumerConfigs());
    }


    @Bean
    @Profile("development")
    public ProducerFactory<Integer, String> producerFactory(AppConfig config) {
        return new DefaultKafkaProducerFactory<>(config.getProducerConfigs());
    }

    @Bean
    @Profile("development")
    public KafkaTemplate<Integer, String> kafkaTemplate(ProducerFactory<Integer, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
