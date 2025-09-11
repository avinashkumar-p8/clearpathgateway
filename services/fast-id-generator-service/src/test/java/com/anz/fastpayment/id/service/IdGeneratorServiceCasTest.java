package com.anz.fastpayment.id.service;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class IdGeneratorServiceCasTest {

    @Test
    void concurrent_generation_exercises_cas_failure_path() throws Exception {
        // Deterministic allocator starting at 100
        SequenceAllocator alloc = blockSize -> 100L;
        IdGeneratorService svc = new IdGeneratorService(alloc);
        // Prime a block and set fields to mid-block
        // First call allocates the block
        svc.nextPuid("G31");

        // Reflectively set current and block end
        Field currentF = IdGeneratorService.class.getDeclaredField("current");
        currentF.setAccessible(true);
        Field endF = IdGeneratorService.class.getDeclaredField("blockEndInclusive");
        endF.setAccessible(true);
        AtomicLong curr = (AtomicLong) currentF.get(svc);
        curr.set(105L);
        endF.setLong(svc, 110L);

        CountDownLatch ready = new CountDownLatch(1);
        String[] out = new String[2];
        Thread t1 = new Thread(() -> { try { ready.await(); out[0] = svc.nextPuid("G31"); } catch (Exception ignored) {} });
        Thread t2 = new Thread(() -> { try { ready.await(); out[1] = svc.nextPuid("G31"); } catch (Exception ignored) {} });
        t1.start(); t2.start(); ready.countDown();
        t1.join(); t2.join();
        assertNotNull(out[0]);
        assertNotNull(out[1]);
        assertNotEquals(out[0], out[1]);
    }
}
