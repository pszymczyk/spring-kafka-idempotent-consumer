package com.kafkaworkhsop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class SimpleIdempotentConsumer {

    private static final Logger log = LoggerFactory.getLogger(SimpleIdempotentConsumer.class);

    private final JdbcTemplate jdbcTemplate;
    private final SimpleDomainService simpleDomainService;

    public SimpleIdempotentConsumer(JdbcTemplate jdbcTemplate, SimpleDomainService simpleDomainService) {
        this.jdbcTemplate = jdbcTemplate;
        this.simpleDomainService = simpleDomainService;
    }

    @KafkaListener(topics = "source-topic", groupId = "demo-application", filter = "skipDuplicatesFilter")
    @Transactional
    void simpleListener(@Header("event-id") String eventId, @Payload String value) {
        log.info("Received event with id {} for the first time.", eventId);
        simpleDomainService.someVeryImportantBusinessLogic(value);
        jdbcTemplate.update("INSERT INTO public.processed_events (event_id) VALUES (?)", eventId);
        log.info("Saved event with id {} as processed.", eventId);
    }
}
