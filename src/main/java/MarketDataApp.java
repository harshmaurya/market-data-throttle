import io.reactivex.rxjava3.internal.schedulers.SingleScheduler;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;

public class MarketDataApp {
    public static void main(String[] args) throws InterruptedException {

        var rateLimit = new RateLimit(100, Duration.ofSeconds(1));
        var processor = new MarketDataProcessor(data -> {
        }, rateLimit, new SingleScheduler(), Clock.systemDefaultZone());
        for (int i = 1; i <= 200; i++) {
            var marketData = new MarketData("1299.hk", BigDecimal.valueOf(i), BigDecimal.valueOf(i), LocalDateTime.now());
            processor.onMessage(marketData);
            Thread.sleep(5);
            var otherData = new MarketData(i + ".tyo", BigDecimal.valueOf(i), BigDecimal.valueOf(i), LocalDateTime.now());
            logMessage(": Sent data " + otherData);
            processor.onMessage(otherData);
        }

        Thread.sleep(100000);
    }

    private static void logMessage(String msg) {
        System.out.println(LocalDateTime.now() + ": " + msg);
    }
}
