package com.anz.fastpayment.id.service;

import com.anz.fastpayment.id.service.impl.JdbcSequenceAllocator;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JdbcSequenceAllocatorNullCountTest {

    @Test
    void init_inserts_when_count_is_null() {
        JdbcTemplate jt = mock(JdbcTemplate.class);
        // CREATE TABLE: do nothing
        // SELECT COUNT(*) returns null
        when(jt.queryForObject(startsWith("SELECT COUNT(*)"), eq(Integer.class))).thenReturn(null);
        // Next calls during allocate
        when(jt.queryForObject(startsWith("SELECT seq_value"), eq(Long.class))).thenReturn(null);
        JdbcSequenceAllocator alloc = new JdbcSequenceAllocator(jt);
        // Verify insert executed due to null count
        verify(jt, atLeastOnce()).update(eq("INSERT INTO id_sequence(name, seq_value) VALUES('global', 0)"));
        // Allocate to ensure code path continues
        assertEquals(0L, alloc.allocateBlock(3));
    }
}
