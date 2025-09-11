package com.anz.fastpayment.id.service.impl;

import com.anz.fastpayment.id.service.SequenceAllocator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Allocates blocks of IDs using a database-backed sequence table.
 * This ensures uniqueness across pods. No per-ID storage; only sequence value advances.
 */
@Component
public class JdbcSequenceAllocator implements SequenceAllocator {

    private final JdbcTemplate jdbcTemplate;

    public JdbcSequenceAllocator(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        init();
    }

    private void init() {
        // Create a simple sequence table if not exists
        // Works with H2/Postgres/Spanner emulator via generic DDL
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS id_sequence (name VARCHAR(64) PRIMARY KEY, seq_value BIGINT NOT NULL)");
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM id_sequence WHERE name='global'", Integer.class);
        if (count == null || count == 0) {
            jdbcTemplate.update("INSERT INTO id_sequence(name, seq_value) VALUES('global', 0)");
        }
    }

    @Override
    @Transactional
    public long allocateBlock(int blockSize) {
        // Atomically advance and return start of the new block
        // update returns rows affected; we read old value via RETURNING when possible
        Long before = jdbcTemplate.queryForObject("SELECT seq_value FROM id_sequence WHERE name='global' FOR UPDATE", Long.class);
        // Start of the newly allocated block is the current stored value
        long start = before == null ? 0L : before;
        long newVal = (before == null ? 0L : before) + blockSize;
        jdbcTemplate.update("UPDATE id_sequence SET seq_value=? WHERE name='global'", newVal);
        return start;
    }
}


