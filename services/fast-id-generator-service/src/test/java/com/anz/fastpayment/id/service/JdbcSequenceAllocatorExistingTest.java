package com.anz.fastpayment.id.service;

import com.anz.fastpayment.id.service.impl.JdbcSequenceAllocator;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;

class JdbcSequenceAllocatorExistingTest {

    private DataSource h2() {
        org.h2.jdbcx.JdbcDataSource ds = new org.h2.jdbcx.JdbcDataSource();
        ds.setURL("jdbc:h2:mem:initdb;DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    @Test
    void second_constructor_uses_existing_row_and_does_not_insert_again() {
        JdbcTemplate jt = new JdbcTemplate(h2());
        // First allocator creates table and inserts row
        JdbcSequenceAllocator a1 = new JdbcSequenceAllocator(jt);
        long start1 = a1.allocateBlock(5);
        assertEquals(0L, start1);
        // Second allocator should initialize without inserting another row and continue sequence
        JdbcSequenceAllocator a2 = new JdbcSequenceAllocator(jt);
        long start2 = a2.allocateBlock(5);
        assertEquals(5L, start2);
    }
}
