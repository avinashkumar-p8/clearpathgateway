package com.anz.fastpayment.id.service;

import com.anz.fastpayment.id.service.impl.JdbcSequenceAllocator;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JdbcSequenceAllocatorBranchesTest {

    @Test
    void first_allocation_when_no_row_value_returns_start_zero() {
        JdbcTemplate jt = mock(JdbcTemplate.class);
        // init path: row exists
        when(jt.queryForObject(anyString(), eq(Integer.class))).thenReturn(1);
        // first select returns null, so before==null
        when(jt.queryForObject(startsWith("SELECT seq_value"), eq(Long.class))).thenReturn(null);
        JdbcSequenceAllocator alloc = new JdbcSequenceAllocator(jt);
        long start = alloc.allocateBlock(7);
        assertEquals(0L, start);
        verify(jt).update(startsWith("UPDATE id_sequence SET seq_value="), any(Object.class));
    }

    @Test
    void subsequent_allocation_returns_previous_value_as_start() {
        JdbcTemplate jt = mock(JdbcTemplate.class);
        when(jt.queryForObject(anyString(), eq(Integer.class))).thenReturn(1);
        // simulate a previous value of 21
        when(jt.queryForObject(startsWith("SELECT seq_value"), eq(Long.class))).thenReturn(21L);
        JdbcSequenceAllocator alloc = new JdbcSequenceAllocator(jt);
        long start = alloc.allocateBlock(9);
        assertEquals(21L, start);
        verify(jt).update(startsWith("UPDATE id_sequence SET seq_value="), any(Object.class));
    }
}
