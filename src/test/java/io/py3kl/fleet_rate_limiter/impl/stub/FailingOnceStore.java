package io.py3kl.fleet_rate_limiter.impl.stub;

import io.py3kl.fleet_rate_limiter.DistributedKeyValueStore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class FailingOnceStore implements DistributedKeyValueStore {
    private final DistributedKeyValueStore delegate;
    private final AtomicBoolean failed = new AtomicBoolean(false);

    public FailingOnceStore(DistributedKeyValueStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletableFuture<Integer> incrementByAndExpire(String key, int delta, long expireTimeInSeconds) throws Exception {
        if (failed.compareAndSet(false, true)) {
            CompletableFuture<Integer> f = new CompletableFuture<>();
            f.completeExceptionally(new RuntimeException("simulated store failure"));
            return f;
        }
        return delegate.incrementByAndExpire(key, delta, expireTimeInSeconds);
    }
}
