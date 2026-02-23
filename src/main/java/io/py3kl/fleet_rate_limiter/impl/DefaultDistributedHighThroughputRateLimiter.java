package io.py3kl.fleet_rate_limiter.impl;

import io.py3kl.fleet_rate_limiter.DistributedHighThroughputRateLimiter;
import io.py3kl.fleet_rate_limiter.DistributedHighThroughputRateLimiterException;
import io.py3kl.fleet_rate_limiter.DistributedKeyValueStore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class DefaultDistributedHighThroughputRateLimiter implements DistributedHighThroughputRateLimiter {

    public final static long EXPIRATION_TIME_SECONDS = 60; // 1 minute
    public final static float BLOCK_SIZE_RATE = 0.10f; // 10% of the limit as block size, with min and max bounds.
    public final static float RELAXATION_RATE = 0.10f; // 10% of the limit as relaxation threshold for local consumption.
    public final static int MIN_BLOCK_SIZE = 1;
    public final static int MAX_BLOCK_SIZE = 10_000;

    final DistributedKeyValueStore keyValueStore;
    final ConcurrentMap<String, RequestState> states = new ConcurrentHashMap<>();

    public DefaultDistributedHighThroughputRateLimiter(
        DistributedKeyValueStore keyValueStore
    ) {
        this.keyValueStore = keyValueStore;
    }

    @Override
    public CompletableFuture<Boolean> isAllowed(final String key, final int limit) {

        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }

        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be greater than 0");
        }

        var state = states.computeIfAbsent(key, providedKey -> new RequestState());

        return isAllowed(state, key, limit);
    }

    private CompletableFuture<Boolean> isAllowed(RequestState state, String key, int limit) {

        if (tryConsumeLocal(state)) {
            return CompletableFuture.completedFuture(true);
        }

        return consumeDistributed(state, key, limit)
            .thenApply(ignored -> tryConsumeLocal(state));
    }


    private CompletableFuture<ReservationResult> consumeDistributed(RequestState state, String key, int limit) {

        while (true) {

            var currentInFlight = state.inFlight.get();
            if (currentInFlight != null) {
                if (currentInFlight.isCompletedExceptionally()) {
                    state.inFlight.compareAndSet(currentInFlight, null);
                    continue;
                }
                return currentInFlight;
            }

            var newInFlight = new CompletableFuture<ReservationResult>();

            if (state.inFlight.compareAndSet(null, newInFlight)) {

                int blockSize = computeBlockSize(limit);

                CompletableFuture<Integer> currentCount;

                try {
                    currentCount = keyValueStore.incrementByAndExpire(key, blockSize, EXPIRATION_TIME_SECONDS);
                } catch (Exception ex) {
                    state.inFlight.compareAndSet(newInFlight, null);
                    newInFlight.completeExceptionally(
                        DistributedHighThroughputRateLimiterException.fromValueStore(ex));
                    return newInFlight;
                }

                currentCount.whenComplete((count, ex) -> {
                    try {
                        if (ex != null) {
                            newInFlight.completeExceptionally(
                                DistributedHighThroughputRateLimiterException.fromValueStore(ex));
                            return;
                        }

                        int relaxedLimit = computeRelaxedLimit(limit);
                        int previousCount = count - blockSize;
                        boolean allowed = previousCount < relaxedLimit;

                        if (allowed) {
                            int requestPermits = Math.min(blockSize, relaxedLimit - previousCount);
                            state.remainingPermits.addAndGet(requestPermits);
                        }

                        newInFlight.complete(new ReservationResult(allowed));
                    } finally {
                        state.inFlight.compareAndSet(newInFlight, null);
                    }
                });

                return newInFlight;
            }
        }
    }

    public static int computeBlockSize(int limit) {
        int fraction = Math.max(MIN_BLOCK_SIZE, (int) (limit * BLOCK_SIZE_RATE));
        return Math.min(MAX_BLOCK_SIZE, fraction);
    }

    static int computeRelaxedLimit(int limit) {
        return (int) Math.ceil(limit * (1 + RELAXATION_RATE));
    }

    private static boolean tryConsumeLocal(RequestState state) {
        while (true) {
            int currentPermits = state.remainingPermits.get();
            if (currentPermits <= 0) {
                return false;
            }
            if (state.remainingPermits.compareAndSet(currentPermits, currentPermits - 1)) {
                return true;
            }
        }
    }

    private static class RequestState {

        final AtomicInteger remainingPermits = new AtomicInteger(0);

        final AtomicReference<CompletableFuture<ReservationResult>> inFlight = new AtomicReference<>(null);
    }

    private record ReservationResult(boolean allowed) { }
}

