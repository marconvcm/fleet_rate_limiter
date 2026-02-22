package io.py3kl.fleet_rate_limiter;

public class DistributedHighThroughputRateLimiterException extends RuntimeException {

    public DistributedHighThroughputRateLimiterException(String message) {
        super(message);
    }

    public DistributedHighThroughputRateLimiterException(String message, Throwable cause) {
        super(message, cause);
    }

    public static DistributedHighThroughputRateLimiterException fromValueStore(Throwable t) {
        if (t instanceof DistributedHighThroughputRateLimiterException) {
            return (DistributedHighThroughputRateLimiterException) t;
        }
        return new DistributedHighThroughputRateLimiterException("Failed to access the distributed key-value store", t);
    }
}
