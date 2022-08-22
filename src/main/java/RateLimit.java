import java.time.Duration;

public class RateLimit {
    private final int maxCalls;
    private final Duration period;

    public RateLimit(int maxCalls, Duration period) {
        this.maxCalls = maxCalls;
        this.period = period;
    }

    public int getMaxCalls() {
        return maxCalls;
    }

    public Duration getPeriod() {
        return period;
    }
}
