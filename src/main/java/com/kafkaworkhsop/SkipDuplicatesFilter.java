package com.kafkaworkhsop;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

@Component
public class SkipDuplicatesFilter implements RecordFilterStrategy<String, String> {

    private static final Logger log = LoggerFactory.getLogger(SkipDuplicatesFilter.class);

    private final JdbcTemplate jdbcTemplate;

    public SkipDuplicatesFilter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean filter(ConsumerRecord<String, String> consumerRecord) {
        String eventId = new String(consumerRecord.headers().lastHeader("event-id").value(), StandardCharsets.UTF_8);
        log.info("Checking is event with id {} already processed.", eventId);
        int count = Optional.ofNullable(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM public.processed_events WHERE event_id=?", Integer.class, eventId))
            .orElse(0);

        boolean duplicateFound = count > 0;
        if (duplicateFound) {
            log.info("Event with id {} already processed.", eventId);
        }
        return duplicateFound;
    }
}
