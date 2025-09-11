package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PuidGeneratorMoreBranchesTest {

    @Test
    void throwsOn2xxBlankBody() {
        RestTemplate rt = mock(RestTemplate.class);
        when(rt.getForEntity(any(String.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>("   ", HttpStatus.OK));
        PuidGenerator gen = new PuidGenerator(rt, "http://id", "G3I");
        assertThrows(IllegalStateException.class, gen::nextPuid);
    }

    @Test
    void throwsOnNon2xx() {
        RestTemplate rt = mock(RestTemplate.class);
        when(rt.getForEntity(any(String.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>("G3I123", HttpStatus.BAD_GATEWAY));
        PuidGenerator gen = new PuidGenerator(rt, "http://id", "G3I");
        assertThrows(IllegalStateException.class, gen::nextPuid);
    }

    @Test
    void throwsOn2xxNullBody() {
        RestTemplate rt = mock(RestTemplate.class);
        when(rt.getForEntity(any(String.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>(null, HttpStatus.OK));
        PuidGenerator gen = new PuidGenerator(rt, "http://id", "G3I");
        assertThrows(IllegalStateException.class, gen::nextPuid);
    }
}
