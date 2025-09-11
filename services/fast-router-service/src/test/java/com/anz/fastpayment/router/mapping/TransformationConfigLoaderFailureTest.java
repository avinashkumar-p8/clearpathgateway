package com.anz.fastpayment.router.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class TransformationConfigLoaderFailureTest {

    @Test
    void fallsBackToEmptyRootOnIOException() throws Exception {
        ObjectMapper real = new ObjectMapper();
        ObjectMapper spyOm = spy(real);
        doThrow(new IOException("boom")).when(spyOm).readTree(any(java.io.InputStream.class));
        TransformationConfigLoader l = new TransformationConfigLoader(spyOm);
        assertNotNull(l.getRoot());
        assertFalse(l.getRoot().has("version"));
    }
}
