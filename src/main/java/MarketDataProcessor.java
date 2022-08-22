import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.internal.schedulers.SingleScheduler;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MarketDataProcessor {
    private final PublishSubject<MarketData> sourceDataStream = PublishSubject.create();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final SingleScheduler scheduler = new SingleScheduler();
    private final MarketDataPublisher publisher;

    public MarketDataProcessor(MarketDataPublisher publisher) {
        this.publisher = publisher;
        initProcessor();
    }

    // Receive incoming market data
    public void onMessage(MarketData data) {
        sourceDataStream.onNext(data);
    }

    // Publish aggregated and throttled market data
    public void publishAggregatedMarketData(MarketData data) {
        // Do Nothing, assume implemented.
        publisher.publishMarketData(data); // Added for testing
    }

    private void initProcessor() {
        var rateLimit = new RateLimit(100, Duration.ofSeconds(1)); // This can come from configuration
        var maxSymbolFreq = Duration.ofSeconds(1);
        var symbolStream = getSymbolFilteredStream(maxSymbolFreq);
        Disposable disposable = getThrottledStream(symbolStream, rateLimit)
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
                .groupBy(MarketData::getSymbol)
                .subscribeOn(scheduler)
                .subscribe(symbolStream -> {
                    Disposable disposable = symbolStream
                            .scan((m1, m2) -> {
                                // Make sure that symbol limit is not reached and skip the stale data
                                if (m2.getUpdateTime().isAfter(m1.getUpdateTime())
                                        && m1.getUpdateTime().plus(maxSymbolFreq).isBefore(m2.getUpdateTime())) {
                                    return m2;
                                }
                                return m1;
                            })
                            .distinctUntilChanged()
                            .subscribe(publishSubject::onNext, publishSubject::onError);
                    disposables.add(disposable);
                });

        disposables.add(subscribe);
        return publishSubject.hide();
    }

    // TODO make generic
    private Observable<MarketData> getThrottledStream(Observable<MarketData> source, RateLimit rateLimit) {
        return Observable.<MarketData>create(emitter -> {
            List<LocalDateTime> requestHistory = new ArrayList<>(); // TODO handle purge
            LinkedHashMap<String, MarketData> queuedMessages = new LinkedHashMap<>(); //Acts as a queue + hashmap
            Disposable disposable = source.subscribe(marketData -> {
                if (!queuedMessages.isEmpty()) {
                    addOrReplace(queuedMessages, marketData);
                    return;
                }
                processOrScheduleMessage(marketData, rateLimit, emitter, requestHistory, queuedMessages);
            });
            emitter.setCancellable(disposable::dispose);
        }).share();
    }

    private void processOrScheduleMessage(MarketData marketData, RateLimit rateLimit, ObservableEmitter<MarketData> emitter, List<LocalDateTime> requestHistory, LinkedHashMap<String, MarketData> queuedMessages) {
        var currentTime = LocalDateTime.now();
        int count = getNumberOfMessagesInCurrentWindow(rateLimit, requestHistory, currentTime);
        if (count < rateLimit.getMaxCalls()) {
            emitter.onNext(marketData);
            requestHistory.add(LocalDateTime.now());
            if (queuedMessages.isEmpty()) return;
            var nextMsg = deQueue(queuedMessages);
            processOrScheduleMessage(nextMsg, rateLimit, emitter, requestHistory, queuedMessages);
        } else {
            addOrReplace(queuedMessages, marketData);
            var firstRequestTime = requestHistory.get(requestHistory.size() - rateLimit.getMaxCalls());
            var nextRun =  firstRequestTime.plus(rateLimit.getPeriod());
            logMessage("Rate limit reached: " + marketData + ". Will run at " + nextRun);
            var delay = Duration.between(currentTime, nextRun);
            scheduler.scheduleDirect(() -> {
                if (!queuedMessages.isEmpty()) {
                    var msg = deQueue(queuedMessages);
                    processOrScheduleMessage(msg, rateLimit, emitter, requestHistory, queuedMessages);
                }
            }, delay.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private MarketData deQueue(LinkedHashMap<String, MarketData> queuedMessages) {
        var entry = queuedMessages.entrySet().iterator().next();
        var data = entry.getValue();
        queuedMessages.remove(entry.getKey());
        return data;
    }


    private void addOrReplace(LinkedHashMap<String, MarketData> queuedMessages, MarketData marketData) {
        // Make sure that only latest market data is present in the queue
        queuedMessages.compute(marketData.getSymbol(), (s, data) -> {
            if (data == null) return marketData;
            return marketData.getUpdateTime().isAfter(data.getUpdateTime()) ? marketData : data;
        });
    }

    private int getNumberOfMessagesInCurrentWindow(RateLimit rateLimit, List<LocalDateTime> requestHistory, LocalDateTime currentTime) {
        var startTime = currentTime.minus(rateLimit.getPeriod());
        var count = 0;
        for (int i = requestHistory.size() - 1; i >= 0; i--) {
            var messageTime = requestHistory.get(i);
            if (messageTime.isBefore(startTime))
                break;
            count++;
        }
        return count;
    }

    private void logMessage(String msg) {
        System.out.println(LocalDateTime.now() + ": " + msg);
    }

}