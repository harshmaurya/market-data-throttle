import java.math.BigDecimal;
import java.time.LocalDateTime;

public class MarketData {
    private final BigDecimal bid;
    private final BigDecimal ask;
    private final String symbol;
    private final LocalDateTime updateTime;

    public MarketData(String symbol, BigDecimal bid, BigDecimal ask, LocalDateTime updateTime) {
        this.bid = bid;
        this.ask = ask;
        this.symbol = symbol;
        this.updateTime = updateTime;
    }

    public BigDecimal getBid() {
        return bid;
    }

    public BigDecimal getAsk() {
        return ask;
    }

    public String getSymbol() {
        return symbol;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    @Override
    public String toString() {
        return "MarketData{" +
                "bid=" + bid +
                ", ask=" + ask +
                ", symbol='" + symbol + '\'' +
                ", updateTime=" + updateTime +
                '}';
    }
}
