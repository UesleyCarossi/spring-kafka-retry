package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

@Slf4j
@Configuration
@SpringBootApplication
public class DemoApplication {

    private final String TOPIC = "topic";
    private final String RETRY_FIRST_TOPIC = "topic-retry-1";
    private final String RETRY_TOPICS_PATTERN = "topic-retry-[0-9]+";
    private final String TOPIC_DLT = "topic-dlt";

    private final FixedBackOff DONT_TRY_AGAIN = new FixedBackOff(0, 0);
    private final int ANY_PARTITION = -1;
    private final int NUMBER_OF_REPEAT_TOPICS = 4;

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic topicFirstRetry() {
        return TopicBuilder.name("topic-retry-1").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic topicSecondRetry() {
        return TopicBuilder.name("topic-retry-2").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic topicThirthRetry() {
        return TopicBuilder.name("topic-retry-3").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic topicFourthRetry() {
        return TopicBuilder.name("topic-retry-4").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic topicDLT() {
        return TopicBuilder.name(TOPIC_DLT).partitions(1).replicas(1).build();
    }

    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, Object> template) {
        return args -> template.send(TOPIC, "EHUSGURI");
    }

    private void process(ConsumerRecord<String, Object> record) {
        log.info("Valor do registro {}", record.value());
        throw new RuntimeException("OPS");
    }

    @KafkaListener(
            id = "try",
            topics = TOPIC,
            containerFactory = "mainKafkaListenerContainerFactory"
    )
    public void consume(@Payload ConsumerRecord<String, Object> record, Acknowledgment ack) {
        try {
            log.info("Processar registro {}", record.value());
            process(record);
        } finally {
            ack.acknowledge();
        }
    }

    @KafkaListener(
            id = "retry",
            topicPattern = RETRY_TOPICS_PATTERN,
            containerFactory = "retryKafkaListenerContainerFactory"
    )
    public void retry(@Payload ConsumerRecord<String, Object> record, Acknowledgment ack) {
        try {
            log.info("Reprocessar registro {}", record.value());
            process(record);
        } finally {
            ack.acknowledge();
        }
    }

    @Bean
    public ConsumerFactory<?, ?> consumerFactory(KafkaProperties properties) {
        return new DefaultKafkaConsumerFactory<>(properties.buildConsumerProperties());
    }

    @Bean
    public BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> mainResolver() {

        return new BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition>() {
            @Override
            public TopicPartition apply(ConsumerRecord<?, ?> r, Exception e) {

                String origem = r.topic();
                log.info("Tópico de origem {}", origem);

                String destino = RETRY_FIRST_TOPIC;
                log.info("Tópico destino do registro {}", destino);

                return new TopicPartition(destino, ANY_PARTITION);
            }
        };
    }

    @Bean
    public SeekToCurrentErrorHandler mainErrorHandler(
            @Qualifier("mainResolver") BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> resolver,
            KafkaTemplate<?, ?> template) {

        SeekToCurrentErrorHandler handler =
                new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(template, resolver), DONT_TRY_AGAIN);

        handler.addNotRetryableExceptions(RuntimeException.class);

        return handler;
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>>
    mainKafkaListenerContainerFactory(
            @Qualifier("mainErrorHandler") SeekToCurrentErrorHandler errorHandler,
            KafkaProperties properties,
            ConsumerFactory<String, Object> factory) {

        ConcurrentKafkaListenerContainerFactory<String, Object> listener =
                new ConcurrentKafkaListenerContainerFactory<>();

        listener.setConsumerFactory(factory);
        listener.setErrorHandler(errorHandler);

        listener.getContainerProperties().setAckMode(AckMode.MANUAL);
        listener.getContainerProperties().setSyncCommits(Boolean.TRUE);

        return listener;
    }

    @Bean
    public BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> retryResolver() {

        return new BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition>() {
            @Override
            public TopicPartition apply(ConsumerRecord<?, ?> r, Exception e) {

                String origem = r.topic();
                log.info("Tópico de origem {}", origem);

                String destino =
                        Optional.of(origem)
                                .filter(topico -> topico.matches(RETRY_TOPICS_PATTERN))
                                .map(t -> t.substring(t.lastIndexOf("-")))
                                .map(n -> n.split("-"))
                                .map(n -> n[1])
                                .map(Integer::parseInt)
                                .filter(n -> n < NUMBER_OF_REPEAT_TOPICS)
                                .map(n -> origem.substring(0, origem.lastIndexOf("-")) + "-" + (n + 1))
                                .orElse(TOPIC_DLT);
                log.info("Tópico destino do registro {}", destino);

                return new TopicPartition(destino, ANY_PARTITION);
            }
        };
    }

    @Bean
    public SeekToCurrentErrorHandler retryErrorHandler(
            @Qualifier("retryResolver") BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> resolver,
            KafkaTemplate<?, ?> template) {
        return new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(template, resolver), DONT_TRY_AGAIN);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>>
    retryKafkaListenerContainerFactory(
            @Qualifier("retryErrorHandler") SeekToCurrentErrorHandler errorHandler,
            KafkaProperties properties,
            ConsumerFactory<String, Object> factory) {

        ConcurrentKafkaListenerContainerFactory<String, Object> listener =
                new ConcurrentKafkaListenerContainerFactory<>();

        final Map<String, Object> retryConfigs = new HashMap<String, Object>() {{
            put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "52428800");
            put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "10000");
        }};
        factory.updateConfigs(retryConfigs);

        listener.setConsumerFactory(factory);
        listener.setErrorHandler(errorHandler);

        listener.getContainerProperties().setAckMode(AckMode.MANUAL);
        listener.getContainerProperties().setSyncCommits(Boolean.TRUE);

        return listener;
    }

}
