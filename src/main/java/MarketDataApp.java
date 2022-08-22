import java.math.BigDecimal;
import java.time.LocalDateTime;

public class MarketDataApp {
    public static void main(String[] args) throws InterruptedException {
        var processor = new MarketDataProcessor(data -> {
            logMessage(": Received data " + data);
        });
        for (int i = 1; i <= 200; i++) {
            /*var marketData = new MarketData("1299.hk", BigDecimal.valueOf(i), BigDecimal.valueOf(i), LocalDateTime.now());
            logMessage(": Sent data " + marketData);
            processor.onMessage(marketData);*/
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
