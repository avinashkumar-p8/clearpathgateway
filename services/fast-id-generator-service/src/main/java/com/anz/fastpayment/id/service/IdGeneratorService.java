package com.anz.fastpayment.id.service;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class IdGeneratorService {

    private static final String PUID_PREFIX = "G31";
    private static final String MUID_PREFIX = "MSG";
    private static final int BLOCK_SIZE = 1000;

    private final SequenceAllocator allocator;

    // In-memory block tracking; no per-ID storage
    private final AtomicLong current = new AtomicLong(0);
    private volatile long blockEndInclusive = -1L;

    public IdGeneratorService(SequenceAllocator allocator) {
        this.allocator = allocator;
    }

    public String nextPuid(String ignoredChannel) {
        long n = nextNumeric();
        return PUID_PREFIX + to13Digits(n);
    }

    public String nextMuid() {
        long n = nextNumeric();
        return MUID_PREFIX + to13Digits(n);
    }

    public synchronized List<String> nextPuidBlock(String ignoredChannel, int size) {
        if (size < 1) size = 1;
        List<String> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(nextPuid(null));
        }
        return list;
    }

    private String to13Digits(long n) {
        if (n < 0) n = Math.abs(n);
        String s = Long.toString(n);
        if (s.length() > 13) s = s.substring(s.length() - 13);
        return String.format("%013d", Long.parseLong(s));
    }

    private long nextNumeric() {
        for (;;) {
            long curr = current.get();
            if (curr <= blockEndInclusive && current.compareAndSet(curr, curr + 1)) {
                return curr;
            }
            synchronized (this) {
                if (current.get() > blockEndInclusive) {
                    long start = allocator.allocateBlock(BLOCK_SIZE);
                    current.set(start);
                    blockEndInclusive = start + BLOCK_SIZE - 1;
                }
            }
        }
    }
}


