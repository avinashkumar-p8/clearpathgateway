package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.DedupeKeyRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.*;

class DuplicateCheckerTest {
    @Test
    void placeholder_always_allows_processing() {
        ObjectProvider<DedupeKeyRepository> provider = new ObjectProvider<>() {
            @Override public DedupeKeyRepository getObject(Object... args) { return null; }
            @Override public DedupeKeyRepository getIfAvailable() { return null; }
            @Override public DedupeKeyRepository getIfUnique() { return null; }
            @Override public DedupeKeyRepository getObject() { return null; }
        };
        DuplicateChecker dc = new DuplicateChecker(provider);
        boolean dup = dc.isDuplicateAndRecord("pacs.008.001.13", "E2E-1", "<xml/>");
        assertFalse(dup);
    }
}


