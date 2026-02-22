package io.py3kl.fleet_rate_limiter.impl;

import io.py3kl.fleet_rate_limiter.DistributedKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DefaultDistributedHighThroughputRateLimiterTest {

    private static final String DEFAULT_KEY = "xzy";

    @Mock
    DistributedKeyValueStore keyValueStore;

    DefaultDistributedHighThroughputRateLimiter limiter;

    @BeforeEach
    void setup() {
        limiter = new DefaultDistributedHighThroughputRateLimiter(keyValueStore);
    }

    @Test
    void expectsIsAllowedThrowsOnInvalidKey() {
        assertThrows(IllegalArgumentException.class, () -> limiter.isAllowed(null, 1));
        assertThrows(IllegalArgumentException.class, () -> limiter.isAllowed("", 1));
    }

    @Test
    void expectsIsAllowedThrowsOnInvalidLimit() {
        assertThrows(IllegalArgumentException.class, () -> limiter.isAllowed(DEFAULT_KEY, 0));
        assertThrows(IllegalArgumentException.class, () -> limiter.isAllowed(DEFAULT_KEY, -10));
    }

    @Test
    void expectsComputeBlockSizeRespectsMinCeilAndMax() {
        assertEquals(1, DefaultDistributedHighThroughputRateLimiter.computeBlockSize(1));
        assertEquals(1, DefaultDistributedHighThroughputRateLimiter.computeBlockSize(3));
        assertEquals(500 * DefaultDistributedHighThroughputRateLimiter.RELAX_THRESHOLD_RATE, DefaultDistributedHighThroughputRateLimiter.computeBlockSize(500));
        assertEquals(10_000, DefaultDistributedHighThroughputRateLimiter.computeBlockSize(600_000));
    }

    @Test
    void expectsIsAllowedCallsDistributedStoreWithBlockSizeAndExpiration() throws Exception {
        int limit = 500;
        int blockSize = DefaultDistributedHighThroughputRateLimiter.computeBlockSize(limit);

        when(keyValueStore.incrementByAndExpire(eq(DEFAULT_KEY), eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)))
            .thenReturn(CompletableFuture.completedFuture(blockSize));

        assertTrue(limiter.isAllowed(DEFAULT_KEY, limit).join());

        verify(keyValueStore, times(1)).incrementByAndExpire(
            eq(DEFAULT_KEY),
            eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)
        );
        verifyNoMoreInteractions(keyValueStore);
    }

    @Test
    void expectsIsAllowedUsesLocalPermitsToAvoidExtraNetworkCalls() throws Exception {
        int limit = 500;
        int numberOfFlightsExpected = 5;
        int blockSize = DefaultDistributedHighThroughputRateLimiter.computeBlockSize(limit);

        when(keyValueStore.incrementByAndExpire(eq(DEFAULT_KEY), eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)))
            .thenReturn(CompletableFuture.completedFuture(blockSize));

        assertTrue(limiter.isAllowed(DEFAULT_KEY, limit).join());

        for (int i = 0; i < blockSize * numberOfFlightsExpected; i++) {
            assertTrue(limiter.isAllowed(DEFAULT_KEY, limit).join());
        }

        verify(keyValueStore, times(numberOfFlightsExpected)).incrementByAndExpire(
            eq(DEFAULT_KEY),
            eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)
        );
        verifyNoMoreInteractions(keyValueStore);
    }

    @Test
    void expectsIsAllowedTriggersSecondReservationAfterLocalPermitsExhausted() throws Exception {
        int limit = 500;
        int blockSize = DefaultDistributedHighThroughputRateLimiter.computeBlockSize(limit);

        when(keyValueStore.incrementByAndExpire(eq(DEFAULT_KEY), eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)))
            .thenReturn(CompletableFuture.completedFuture(blockSize))      // count = 10
            .thenReturn(CompletableFuture.completedFuture(blockSize * 2)); // count = 20

        assertTrue(limiter.isAllowed(DEFAULT_KEY, limit).join());
        for (int i = 0; i < blockSize; i++) {
            assertTrue(limiter.isAllowed(DEFAULT_KEY, limit).join());
        }

        assertTrue(limiter.isAllowed(DEFAULT_KEY, limit).join());

        verify(keyValueStore, times(2)).incrementByAndExpire(
            eq(DEFAULT_KEY),
            eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)
        );
    }

    @Test
    void expectsIsAllowedDeniesWhenPreviousCountIsAtOrAboveLimit() throws Exception {
        int limit = 500;
        int blockSize = DefaultDistributedHighThroughputRateLimiter.computeBlockSize(limit);

        // previousCount = count - blockSize = 500 => not allowed
        when(keyValueStore.incrementByAndExpire(eq(DEFAULT_KEY), eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)))
            .thenReturn(CompletableFuture.completedFuture(limit + blockSize));

        assertFalse(limiter.isAllowed(DEFAULT_KEY, limit).join());

        verify(keyValueStore, times(1)).incrementByAndExpire(
            eq(DEFAULT_KEY),
            eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)
        );
    }

    @Test
    void expectsIsAllowedSharesSingleInFlightReservationAcrossConcurrentCalls() throws Exception {
        int limit = 500;
        int blockSize = DefaultDistributedHighThroughputRateLimiter.computeBlockSize(limit);

        var storeFuture = new CompletableFuture<Integer>();

        when(keyValueStore.incrementByAndExpire(eq(DEFAULT_KEY), eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)))
            .thenReturn(storeFuture);

        var f1 = limiter.isAllowed(DEFAULT_KEY, limit);
        var f2 = limiter.isAllowed(DEFAULT_KEY, limit);

        verify(keyValueStore, times(1)).incrementByAndExpire(
            eq(DEFAULT_KEY),
            eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)
        );

        storeFuture.complete(blockSize);

        assertTrue(f1.join());
        assertTrue(f2.join());
    }

    @Test
    void expectsIsAllowedRetriesAfterStoreFutureFails() throws Exception {
        int limit = 500;
        int blockSize = DefaultDistributedHighThroughputRateLimiter.computeBlockSize(limit);

        var failed = new CompletableFuture<Integer>();
        failed.completeExceptionally(new RuntimeException("boom"));

        when(keyValueStore.incrementByAndExpire(eq(DEFAULT_KEY), eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)))
            .thenReturn(failed)
            .thenReturn(CompletableFuture.completedFuture(blockSize));

        assertThrows(CompletionException.class, () -> limiter.isAllowed(DEFAULT_KEY, limit).join());
        assertTrue(limiter.isAllowed(DEFAULT_KEY, limit).join());

        verify(keyValueStore, times(2)).incrementByAndExpire(
            eq(DEFAULT_KEY),
            eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)
        );
    }

    @Test
    void expectsIsAllowedMultithreadedWithThreadPoolUsesSingleReservationWhenBatchCoversAllCalls() throws Exception {
        var limiter = new DefaultDistributedHighThroughputRateLimiter(keyValueStore);

        int limit = 10_000; // with THRESHOLD_RATE=0.02 => blockSize ~= 200 (covers 50 calls)
        int blockSize = DefaultDistributedHighThroughputRateLimiter.computeBlockSize(limit);

        when(keyValueStore.incrementByAndExpire(
            eq(DEFAULT_KEY),
            eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)
        )).thenReturn(CompletableFuture.completedFuture(blockSize));

        int threads = 8;
        int tasks = 100;


        var startGate = new CountDownLatch(1);
        var doneGate = new CountDownLatch(tasks);

        var results = new ConcurrentLinkedQueue<Boolean>();

        try (var executor = Executors.newFixedThreadPool(threads)) {

            for (int i = 0; i < tasks; i++) {
                executor.submit(() -> {
                    try {
                        startGate.await();
                        results.add(limiter.isAllowed(DEFAULT_KEY, limit).join());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        doneGate.countDown();
                    }
                });
            }

            startGate.countDown();
            assertTrue(doneGate.await(3, TimeUnit.SECONDS), "Tasks did not finish in time");

            assertEquals(tasks, results.size());
            assertTrue(results.stream().allMatch(Boolean::booleanValue));

            verify(keyValueStore, times(1)).incrementByAndExpire(
                eq(DEFAULT_KEY),
                eq(blockSize),
                eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)
            );
            verifyNoMoreInteractions(keyValueStore);
        }
    }

    @Test
    void shouldHandleConcurrencySingleHostAndShouldAllowAtLeastLimitAndNotExplodeOverLimit() throws Exception {
        int limit = 400;
        int blockSize = DefaultDistributedHighThroughputRateLimiter.computeBlockSize(limit);

        var globalCount = new AtomicInteger(0);

        when(keyValueStore.incrementByAndExpire(
            eq(DEFAULT_KEY),
            eq(blockSize),
            eq(DefaultDistributedHighThroughputRateLimiter.EXPIRATION_TIME_SECONDS)
        )).thenAnswer(inv -> {
            int delta = inv.getArgument(1, Integer.class);
            int count = globalCount.addAndGet(delta);
            return CompletableFuture.completedFuture(count);
        });

        int totalCalls = 100_000;
        var tasks = new ArrayList<Callable<Boolean>>(totalCalls);
        for (int i = 0; i < totalCalls; i++) {
            tasks.add(() -> limiter.isAllowed(DEFAULT_KEY, limit).join());
        }

        try (var executor = Executors.newWorkStealingPool()) {

            int requestAllowedByLimiter = 0;
            for (Future<Boolean> f : executor.invokeAll(tasks)) {
                if (f.get()) requestAllowedByLimiter++;
            }

            assertTrue(requestAllowedByLimiter >= limit, "Must allow at least the configured limit");

            int overLimit = Math.max(0, requestAllowedByLimiter - limit);
            int relaxedMaxOverLimit = (int) Math.ceil(limit * DefaultDistributedHighThroughputRateLimiter.RELAX_THRESHOLD_RATE) + blockSize;

            float overLimitRate = 1f - (float) overLimit / relaxedMaxOverLimit;

            System.out.println("Over limit rate (" + overLimitRate + ") requests should be within the relaxed threshold rate. Over limit: " + overLimit + ", Relaxed Max Over Limit: " + relaxedMaxOverLimit);
            assertTrue(
                overLimitRate < DefaultDistributedHighThroughputRateLimiter.RELAX_THRESHOLD_RATE,
                "Over limit rate (" + overLimitRate + ") requests should be within the relaxed threshold rate. Over limit: " + overLimit + ", Relaxed Max Over Limit: " + relaxedMaxOverLimit
            );
        }
    }
}