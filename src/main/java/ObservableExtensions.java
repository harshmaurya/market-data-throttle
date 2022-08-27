import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ObservableExtensions {

    private ObservableExtensions() {

    }

    public static <T> Observable<T> getThrottledDataPerSlidingWindow(Observable<T> source, RateLimit rateLimit, Scheduler scheduler, Clock clock) {
        return Observable.<T>create(emitter -> {
            List<LocalDateTime> requestHistory = new ArrayList<>(); // TODO handle purge
            Queue<T> queuedMessages = new ArrayDeque<>(); //Acts as a queue + hashmap
            Disposable disposable = source
                    .subscribe(marketData -> {
                        if (!queuedMessages.isEmpty()) {
                            queuedMessages.add(marketData);
                            return;
                        }
                        processOrScheduleMessage(marketData, rateLimit, emitter, requestHistory, queuedMessages, scheduler, clock);
                    });
            emitter.setCancellable(disposable::dispose);
        }).share();
    }

    private static <T> void processOrScheduleMessage(T marketData, RateLimit rateLimit, ObservableEmitter<T> emitter, List<LocalDateTime> requestHistory, Queue<T> queuedMessages, Scheduler scheduler, Clock clock) {
        var currentTime = LocalDateTime.now(clock);
        int count = getNumberOfMessagesInCurrentWindow(rateLimit, requestHistory, currentTime);
        if (count < rateLimit.getMaxCalls()) {
            emitter.onNext(marketData);
            requestHistory.add(currentTime);
            if (queuedMessages.isEmpty()) return;
            var nextMsg = queuedMessages.remove();
            processOrScheduleMessage(nextMsg, rateLimit, emitter, requestHistory, queuedMessages, scheduler, clock);
        } else {
            queuedMessages.add(marketData);
            var firstRequestTime = requestHistory.get(requestHistory.size() - rateLimit.getMaxCalls());
            var nextRun = firstRequestTime.plus(rateLimit.getPeriod()).plus(Duration.ofNanos(1));
            var delay = Duration.between(currentTime, nextRun);
            scheduler.scheduleDirect(() -> {
                if (!queuedMessages.isEmpty()) {
                    var msg = queuedMessages.remove();
                    processOrScheduleMessage(msg, rateLimit, emitter, requestHistory, queuedMessages, scheduler, clock);
                }
            }, delay.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static int getNumberOfMessagesInCurrentWindow(RateLimit rateLimit, List<LocalDateTime> requestHistory, LocalDateTime currentTime) {
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
}
