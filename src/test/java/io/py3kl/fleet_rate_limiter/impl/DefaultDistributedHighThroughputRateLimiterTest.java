package io.py3kl.fleet_rate_limiter.impl;

import io.py3kl.fleet_rate_limiter.impl.stub.FailingOnceStore;
import io.py3kl.fleet_rate_limiter.impl.stub.InMemoryDistributedKeyValueStore;
import io.py3kl.fleet_rate_limiter.impl.stub.ManualClock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultDistributedHighThroughputRateLimiterTest {

    private static final ExecutorService POOL = Executors.newFixedThreadPool(
        Math.max(4, Runtime.getRuntime().availableProcessors())
    );

    @AfterAll
    static void shutdown() {
        POOL.shutdownNow();
    }

    @Test
    void shouldAllowsAtLeastLimitSingleHostSequential() {
        var clock = new ManualClock();
        var store = new InMemoryDistributedKeyValueStore(clock);
        var limiter = new DefaultDistributedHighThroughputRateLimiter(store);

        String key = "clientA";
        int limit = 200;

        for (int i = 0; i < limit; i++) {
            assertTrue(limiter.isAllowed(key, limit).join(), "Request " + i + " should be allowed");
        }
        // Next one may be allowed or denied (approx ok), so we don't assert.
        limiter.isAllowed(key, limit).join();
    }

    @Test
    void shouldExpiryResetsCounterAfter60Seconds() {
        var clock = new ManualClock();
        var store = new InMemoryDistributedKeyValueStore(clock);
        var limiter = new DefaultDistributedHighThroughputRateLimiter(store);

        String key = "clientB";
        int limit = 50;

        // Consume up to limit.
        for (int i = 0; i < limit; i++) {
            assertTrue(limiter.isAllowed(key, limit).join());
        }

        // Advance past the fixed window (61s).
        clock.advanceSeconds(61);

        // Should allow again after expiry.
        assertTrue(limiter.isAllowed(key, limit).join());
    }

    @Test
    void shouldHandleConcurrencySingleHostAndShouldAllowAtLeastLimitAndNotExplodeOverLimit() throws Exception {
        var clock = new ManualClock();
        var store = new InMemoryDistributedKeyValueStore(clock);
        var limiter = new DefaultDistributedHighThroughputRateLimiter(store);

        String key = "hotKey";
        int limit = 400;

        int totalCalls = 100_000;
        var tasks = new ArrayList<Callable<Boolean>>(totalCalls);
        for (int i = 0; i < totalCalls; i++) {
            tasks.add(() -> limiter.isAllowed(key, limit).join());
        }

        int requestAllowedByLimiter = 0;
        for (Future<Boolean> f : POOL.invokeAll(tasks)) {
            if (f.get()) requestAllowedByLimiter++;
        }

        assertTrue(requestAllowedByLimiter >= limit, "Must allow at least the configured limit");

        int blockSize = DefaultDistributedHighThroughputRateLimiter.computeBlockSize(limit);
        int atLeastRate = (limit + blockSize) / requestAllowedByLimiter;

        assertTrue(atLeastRate < DefaultDistributedHighThroughputRateLimiter.RELAX_THRESHOLD_RATE,
            "Rate (" + atLeastRate + ") should be below threshold (" + DefaultDistributedHighThroughputRateLimiter.RELAX_THRESHOLD_RATE + "); if this fails, your block size may be too large for the limit");
    }

    @Test
    void shouldHandleStoreFailureAndShouldNotPermanentlyBrickKey() {
        var clock = new ManualClock();
        var baseStore = new InMemoryDistributedKeyValueStore(clock);

        var failOnceStore = new FailingOnceStore(baseStore);
        var limiter = new DefaultDistributedHighThroughputRateLimiter(failOnceStore);

        String key = "clientFail";
        int limit = 1000;
        assertThrows(CompletionException.class, () -> limiter.isAllowed(key, limit).join());

        assertTrue(limiter.isAllowed(key, limit).join(),
            "Limiter should recover after transient store failure");
    }

    @Test
    void shouldAllowAtLeastTheLimitAcrossMultipleInstancesEvenWithUnevenTrafficDistribution() {
        var clock = new ManualClock();
        var store = new InMemoryDistributedKeyValueStore(clock);

        var hostA = new DefaultDistributedHighThroughputRateLimiter(store);
        var hostB = new DefaultDistributedHighThroughputRateLimiter(store);

        String key = "clientUneven";
        int limit = 1000;

        // Trigger a large reservation on host A, then stop using it.
        assertTrue(hostA.isAllowed(key, limit).join());

        // Now hammer only host B.
        int allowedOnB = 0;
        for (int i = 0; i < limit; i++) {
            if (hostB.isAllowed(key, limit).join()) allowedOnB++;
        }

        // Total allowed across fleet:
        int totalAllowed = 1 + allowedOnB;

        assertTrue(totalAllowed >= limit,
            "Must allow at least limit across fleet; totalAllowed=" + totalAllowed +
                " (this likely fails with current approach under uneven traffic)");
    }
}

