package com.anz.fastpayment.id.service;

import com.anz.fastpayment.id.service.impl.JdbcSequenceAllocator;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;

class JdbcSequenceAllocatorTest {

    private DataSource newH2() {
        org.h2.jdbcx.JdbcDataSource ds = new org.h2.jdbcx.JdbcDataSource();
        ds.setURL("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    @Test
    void allocateBlock_advances_sequence_and_returns_start() {
        JdbcTemplate jt = new JdbcTemplate(newH2());
        JdbcSequenceAllocator alloc = new JdbcSequenceAllocator(jt);
        long start1 = alloc.allocateBlock(10);
        long start2 = alloc.allocateBlock(10);
        assertEquals(0L, start1);
        assertEquals(10L, start2);
    }
}
