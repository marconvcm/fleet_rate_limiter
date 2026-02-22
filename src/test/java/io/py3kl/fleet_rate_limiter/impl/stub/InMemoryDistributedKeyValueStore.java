package io.py3kl.fleet_rate_limiter.impl.stub;

import io.py3kl.fleet_rate_limiter.DistributedKeyValueStore;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDistributedKeyValueStore implements DistributedKeyValueStore {
    private final ConcurrentHashMap<String, InMemoryDistributedKeyValueStore.Entry> map = new ConcurrentHashMap<>();
    private final ManualClock clock;

    public InMemoryDistributedKeyValueStore(ManualClock clock) {
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public CompletableFuture<Integer> incrementByAndExpire(String key, int delta, long expireTimeInSeconds) {
        if (key == null || key.isEmpty()) throw new IllegalArgumentException("key");
        if (expireTimeInSeconds <= 0) throw new IllegalArgumentException("expirationSeconds");

        final long now = clock.nowMillis();
        final int[] out = new int[1];

        map.compute(key, (k, existing) -> {
            InMemoryDistributedKeyValueStore.Entry e = existing;
            if (e == null || e.isExpired(now)) {
                long expiresAt = now + expireTimeInSeconds * 1000L;
                e = new InMemoryDistributedKeyValueStore.Entry(0, expiresAt);
            }
            e.value += delta;
            out[0] = e.value;
            return e;
        });

        return CompletableFuture.completedFuture(out[0]);
    }

    static final class Entry {
        int value;
        final long expiresAtMillis;

        Entry(int value, long expiresAtMillis) {
            this.value = value;
            this.expiresAtMillis = expiresAtMillis;
        }

        boolean isExpired(long nowMillis) {
            return nowMillis >= expiresAtMillis;
        }
    }
}
