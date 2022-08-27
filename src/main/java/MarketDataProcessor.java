import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class MarketDataProcessor {
    private final PublishSubject<MarketData> sourceDataStream = PublishSubject.create();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final RateLimit rateLimit;
    private final Scheduler scheduler;
    private final Clock clock;
    private final MarketDataPublisher publisher;

    public MarketDataProcessor(MarketDataPublisher publisher, RateLimit rateLimit, Scheduler scheduler, Clock clock) {
        this.publisher = publisher;
        this.rateLimit = rateLimit;
        this.scheduler = scheduler;
        this.clock = clock;
        initProcessor();
    }

    // Receive incoming market data
    public void onMessage(MarketData data) {
        logMessage("Received data" + data);
        sourceDataStream.onNext(data);
    }

    // Publish aggregated and throttled market data
    public void publishAggregatedMarketData(MarketData data) {
        // Do Nothing, assume implemented.
        publisher.publishMarketData(data); // Added for testing
        logMessage("Published data" + data);
    }

    private void initProcessor() {

        var symbolDuration = rateLimit.getPeriod();
        var symbolStream = getSymbolFilteredStream(symbolDuration);
        Disposable disposable = ObservableExtensions.getThrottledDataPerSlidingWindow(symbolStream, rateLimit, scheduler, clock)
                .subscribe(this::publishAggregatedMarketData, Throwable::printStackTrace);
        disposables.add(disposable);

        // If slight latency is acceptable, we can add a buffer to increase throughput
        /*var bufferTimeInMilliSeconds = 50;
        Observable<List<MarketData>> bufferedSource = sourceDataStream
                .buffer(bufferTimeInMilliSeconds, TimeUnit.MILLISECONDS, limit.getMaxCalls());*/
    }

    private Observable<MarketData> getSymbolFilteredStream(Duration maxSymbolFreq) {
        PublishSubject<MarketData> publishSubject = PublishSubject.create();
        Disposable subscribe = sourceDataStream
                .subscribeOn(scheduler)
                .groupBy(MarketData::getSymbol)
                .subscribe(symbolStream -> {
                    Disposable disposable = symbolStream
                            .scan((m1, m2) -> {
                                // skip the stale data as ordering is not guaranteed
                                return m2.getUpdateTime().isBefore(m1.getUpdateTime()) ? m1 : m2;
                            })
                            .distinctUntilChanged()
                            .throttleLatest(maxSymbolFreq.toMillis(), TimeUnit.MILLISECONDS, scheduler, true) // Makes sure that symbols limit is not breached
                            .subscribe(publishSubject::onNext, publishSubject::onError);
                    disposables.add(disposable);
                }, Throwable::printStackTrace);

        disposables.add(subscribe);
        return publishSubject.hide();
    }

    private void logMessage(String msg) {
        System.out.println(LocalDateTime.now(clock) + ": " + msg);
    }
}