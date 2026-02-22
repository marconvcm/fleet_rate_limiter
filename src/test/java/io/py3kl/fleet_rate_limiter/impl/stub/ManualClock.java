package io.py3kl.fleet_rate_limiter.impl.stub;

import java.util.concurrent.atomic.AtomicLong;

public class ManualClock {

    private final AtomicLong nowMillis = new AtomicLong(0);

    public long nowMillis() {
        return nowMillis.get();
    }

    public void advanceSeconds(long seconds) {
        nowMillis.addAndGet(seconds * 1000L);
    }
}
