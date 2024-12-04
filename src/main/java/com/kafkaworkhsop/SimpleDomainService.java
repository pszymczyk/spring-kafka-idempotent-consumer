package com.kafkaworkhsop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class SimpleDomainService {

    private static final Logger log = LoggerFactory.getLogger(SimpleDomainService.class);

    private final JdbcTemplate jdbcTemplate;

    public SimpleDomainService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void someVeryImportantBusinessLogic(String event) {
        log.info("Handling some event {}.", event);
        int count = Optional.ofNullable(jdbcTemplate.queryForObject("SELECT events_count_value FROM public.events_count", Integer.class))
            .orElse(0);
        log.info("Already processed {} events.", count);
        jdbcTemplate.update("INSERT into public.events_count (events_count_value) VALUES (?)", ++count);
        log.info("Updated events count ++.");
    }
}
