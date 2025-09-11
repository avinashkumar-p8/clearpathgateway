package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PuidGeneratorBranchesTest {

    @Test
    void returnsRemoteIdOnSuccess() {
        RestTemplate rt = mock(RestTemplate.class);
        when(rt.getForEntity(any(String.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>("G3I0000000000001", HttpStatus.OK));
        PuidGenerator gen = new PuidGenerator(rt, "http://id", "G3I");
        String id = gen.nextPuid();
        assertEquals("G3I0000000000001", id);
    }

    @Test
    void throwsWhenRemoteFails() {
        RestTemplate rt = mock(RestTemplate.class);
        when(rt.getForEntity(any(String.class), eq(String.class)))
                .thenThrow(new RuntimeException("boom"));
        PuidGenerator gen = new PuidGenerator(rt, "http://id", "G3I");
        assertThrows(IllegalStateException.class, gen::nextPuid);
    }
}
