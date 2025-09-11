package com.anz.fastpayment.id.service;

import com.anz.fastpayment.id.service.impl.JdbcSequenceAllocator;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JdbcSequenceAllocatorErrorTest {

    @Test
    void allocateBlock_propagates_sql_exception() {
        JdbcTemplate jt = mock(JdbcTemplate.class);
        // constructor init
        when(jt.queryForObject(anyString(), eq(Integer.class))).thenReturn(1);
        // simulate select failure during allocate
        when(jt.queryForObject(startsWith("SELECT seq_value"), eq(Long.class))).thenThrow(new org.springframework.dao.DataAccessResourceFailureException("read fail"));
        JdbcSequenceAllocator alloc = new JdbcSequenceAllocator(jt);
        assertThrows(org.springframework.dao.DataAccessResourceFailureException.class, () -> alloc.allocateBlock(10));
    }
}
