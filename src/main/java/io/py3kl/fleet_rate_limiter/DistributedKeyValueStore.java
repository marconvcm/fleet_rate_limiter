package io.py3kl.fleet_rate_limiter;

import java.util.concurrent.CompletableFuture;

public interface DistributedKeyValueStore {

    CompletableFuture<Integer> incrementByAndExpire(String key, int delta, long expireTimeInSeconds) throws Exception;
}
