package com.anz.fastpayment.router.service;

import com.google.cloud.spring.data.spanner.core.SpannerTemplate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DuplicateCheckerTest {
    @Test
    void insert_new_key_returns_not_duplicate() {
        SpannerTemplate spannerTemplate = mock(SpannerTemplate.class);
        DuplicateChecker dc = new DuplicateChecker(spannerTemplate);
        boolean dup = dc.isDuplicateAndRecord("pacs.008.001.13", "E2E-1", "<xml/>");
        assertFalse(dup);
        verify(spannerTemplate, times(1)).insert(any());
    }
}


