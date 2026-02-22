package io.py3kl.fleet_rate_limiter;

import io.py3kl.fleet_rate_limiter.impl.DefaultDistributedHighThroughputRateLimiter;

import java.util.concurrent.CompletableFuture;

public interface DistributedHighThroughputRateLimiter {

    CompletableFuture<Boolean> isAllowed(String key, int limit);

    static DistributedHighThroughputRateLimiter createDefaultRaterLimiter(DistributedKeyValueStore keyValueStore) {
        return new DefaultDistributedHighThroughputRateLimiter(keyValueStore);
    }
}
