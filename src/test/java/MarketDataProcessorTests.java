import com.mercateo.test.clock.TestClock;
import io.reactivex.rxjava3.schedulers.TestScheduler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import java.math.BigDecimal;
import java.time.*;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class MarketDataProcessorTests {
    private TestClock testClock;
    private TestScheduler testScheduler;
    private MarketDataPublisher mockPublisher;
    private RateLimit rateLimit;

    @Before
    public void setUp() {
        testScheduler = new TestScheduler();
        mockPublisher = mock(MarketDataPublisher.class);
        testClock = TestClock.fixed(Instant.EPOCH, ZoneId.systemDefault());
        rateLimit = new RateLimit(100, Duration.ofSeconds(1));
        moveTimeFromBeginning(Duration.ZERO);
    }

    @Test
    public void messages_with_no_limit_breach_should_be_processed_correctly() {
        //Arrange
        var processor = createProcessor();
        moveTimeFromBeginning(Duration.ofMillis(1));
        int numberOfTimes = rateLimit.getMaxCalls() - 1;
        var lastSymbol = numberOfTimes + ".hk";

        //Act
        for (int i = 1; i <= numberOfTimes; i++) {
            processor.onMessage(new MarketData(i + ".hk", BigDecimal.ONE, BigDecimal.ONE, LocalDateTime.now(testClock)));
        }

        //Assert
        ArgumentCaptor<MarketData> captor = ArgumentCaptor.forClass(MarketData.class);
        verify(mockPublisher, times(numberOfTimes)).publishMarketData(captor.capture());
        var dataList = captor.getAllValues();
        assertEquals(lastSymbol, dataList.get(dataList.size() - 1).getSymbol());
    }

    @Test
    public void each_symbol_should_not_update_more_than_once_per_sliding_window() {
        //Arrange
        var processor = createProcessor();
        moveTimeFromBeginning(Duration.ofMillis(1));
        var numberOfTimes = 3;
        var lastPrice = BigDecimal.valueOf(numberOfTimes);

        //Act
        for (int i = 1; i <= numberOfTimes; i++) {
            processor.onMessage(new MarketData("test.hk", BigDecimal.valueOf(i), BigDecimal.valueOf(i), LocalDateTime.now(testClock)));
        }

        // Assert should publish first update instantaneously
        verify(mockPublisher, atLeastOnce()).publishMarketData(any());

        moveTimeFromBeginning(rateLimit.getPeriod().minus(Duration.ofMillis(1)));
        // Should not publish next update until before sliding window period
        verify(mockPublisher, atMost(1)).publishMarketData(any());

        moveTimeFromBeginning(rateLimit.getPeriod().plus(Duration.ofMillis(1)));
        // Should publish latest update after sliding window period
        ArgumentCaptor<MarketData> captor = ArgumentCaptor.forClass(MarketData.class);
        verify(mockPublisher, atLeast(2)).publishMarketData(captor.capture());
        var dataList = captor.getAllValues();
        assertEquals(lastPrice, dataList.get(dataList.size() - 1).getBid());
    }

    @Test
    public void number_of_publish_should_not_exceed_limit() {
        //Arrange
        var processor = createProcessor();
        moveTimeFromBeginning(Duration.ofMillis(1));
        var totalNumberOfCalls = rateLimit.getMaxCalls() + 1;

        //Act
        for (int i = 1; i <= totalNumberOfCalls; i++) {
            processor.onMessage(new MarketData(i + ".hk", BigDecimal.valueOf(i), BigDecimal.valueOf(i), LocalDateTime.now(testClock)));
        }

        // Should not publish next update until before sliding window period
        moveTimeFromBeginning(rateLimit.getPeriod().minusMillis(1));
        ArgumentCaptor<MarketData> firstCaptor = ArgumentCaptor.forClass(MarketData.class);
        verify(mockPublisher, atMost(rateLimit.getMaxCalls())).publishMarketData(firstCaptor.capture());

        // Should publish latest update after sliding window period
        moveTimeFromBeginning(rateLimit.getPeriod().plusMillis(2));
        ArgumentCaptor<MarketData> secondCaptor = ArgumentCaptor.forClass(MarketData.class);
        verify(mockPublisher, times(totalNumberOfCalls)).publishMarketData(secondCaptor.capture());
    }

    @Test
    public void number_of_publish_should_not_exceed_limit_per_sliding_window() {
        //Arrange
        var processor = createProcessor();
        moveTimeFromBeginning(Duration.ofMillis(1));
        int halfOfAllowedCalls = rateLimit.getMaxCalls() / 2;

        //Act
        // Send the first half of requests at the middle of first second
        var halfTime = rateLimit.getPeriod().dividedBy(2);
        moveTimeFromBeginning(halfTime);
        for (int i = 1; i <= halfOfAllowedCalls; i++) {
            processor.onMessage(new MarketData(i + ".hk", BigDecimal.valueOf(i), BigDecimal.valueOf(i), LocalDateTime.now(testClock)));
        }

        // Send the remaining half of requests at the start of next second
        moveTimeFromBeginning(rateLimit.getPeriod().plusMillis(1));
        for (int i = halfOfAllowedCalls + 1; i <= rateLimit.getMaxCalls(); i++) {
            processor.onMessage(new MarketData(i + ".hk", BigDecimal.valueOf(i), BigDecimal.valueOf(i), LocalDateTime.now(testClock)));
        }

        // Verify that we publish all the requests till this point
        ArgumentCaptor<MarketData> captor = ArgumentCaptor.forClass(MarketData.class);
        verify(mockPublisher, times(rateLimit.getMaxCalls())).publishMarketData(captor.capture());

        // Send another request
        var finalRequestNumber = rateLimit.getMaxCalls() + 1;
        processor.onMessage(new MarketData(finalRequestNumber + ".hk", BigDecimal.valueOf(finalRequestNumber), BigDecimal.valueOf(finalRequestNumber), LocalDateTime.now(testClock)));

        // Verify that we do not publish the last message yet
        ArgumentCaptor<MarketData> secondCaptor = ArgumentCaptor.forClass(MarketData.class);
        verify(mockPublisher, times(rateLimit.getMaxCalls())).publishMarketData(secondCaptor.capture());

        // Verify that we receive the final update at the next earliest opportunity
        moveTimeFromBeginning(rateLimit.getPeriod().plus(halfTime).plusMillis(1));
        ArgumentCaptor<MarketData> finalCaptor = ArgumentCaptor.forClass(MarketData.class);
        verify(mockPublisher, times(rateLimit.getMaxCalls() + 1)).publishMarketData(finalCaptor.capture());
        var dataList = finalCaptor.getAllValues();
        assertEquals(finalRequestNumber + ".hk", dataList.get(dataList.size() - 1).getSymbol());
    }

    @Test
    public void latest_message_should_be_published_for_out_of_order_inputs() {
        //Arrange
        var processor = createProcessor();
        moveTimeFromBeginning(Duration.ofMillis(1));
        var numberOfTimes = 3;

        //Act
        // The first update is in order
        processor.onMessage(new MarketData("test.hk", BigDecimal.valueOf(1), BigDecimal.valueOf(1), LocalDateTime.now(testClock).minusSeconds(10)));
        // The second and third updates are out of order
        for (int i = 2; i <= numberOfTimes; i++) {
            processor.onMessage(new MarketData("test.hk", BigDecimal.valueOf(i), BigDecimal.valueOf(i), LocalDateTime.now(testClock).minusSeconds(i)));
        }

        // Assert should publish first update instantaneously
        verify(mockPublisher, atLeastOnce()).publishMarketData(any());

        moveTimeFromBeginning(rateLimit.getPeriod().plus(Duration.ofMillis(2)));
        // Should publish latest update after sliding window period
        ArgumentCaptor<MarketData> captor = ArgumentCaptor.forClass(MarketData.class);
        verify(mockPublisher, atLeast(1)).publishMarketData(captor.capture());
        var dataList = captor.getAllValues();
        // The second message is a more recent update based on timestamp but arrived out of order
        var recentPrice = BigDecimal.valueOf(2);
        assertEquals(recentPrice, dataList.get(dataList.size() - 1).getBid());
    }

    @Test
    public void symbol_throttled_updates_should_not_slow_down_other_symbols_update() {
        //Arrange
        var processor = createProcessor();
        moveTimeFromBeginning(Duration.ofMillis(1));
        var totalNumberOfCalls = rateLimit.getMaxCalls() / 2;

        //Act
        for (int i = 1; i <= totalNumberOfCalls; i++) {
            processor.onMessage(new MarketData("test.hk", BigDecimal.valueOf(i), BigDecimal.valueOf(i), LocalDateTime.now(testClock)));
        }

        //Act
        for (int i = 1; i <= totalNumberOfCalls; i++) {
            processor.onMessage(new MarketData(i + ".hk", BigDecimal.valueOf(i), BigDecimal.valueOf(i), LocalDateTime.now(testClock)));
        }

        // Should publish the symbols other than test.hk even after symbol limit is reached for test.hk
        moveTimeFromBeginning(rateLimit.getPeriod());
        verify(mockPublisher, times(totalNumberOfCalls + 1)).publishMarketData(any());
    }


    // Moves the test clock and test scheduler to epoch + specified duration
    private void moveTimeFromBeginning(Duration duration) {
        testClock.set(Instant.EPOCH.plus(duration));
        var future = Instant.EPOCH.plus(duration).toEpochMilli();
        testScheduler.advanceTimeTo(future, TimeUnit.MILLISECONDS);
    }

    private MarketDataProcessor createProcessor() {
        return new MarketDataProcessor(mockPublisher, rateLimit, testScheduler, testClock);
    }
}
