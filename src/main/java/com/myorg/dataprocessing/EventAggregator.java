package com.myorg.dataprocessing;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class EventAggregator {

    private EventAggregator() {}

    public static Map<String, AggregatedStats> aggregate(Stream<Event> events) {
    var statsById = new ConcurrentHashMap<String, MutableStats>();
    var seenById = new ConcurrentHashMap<String, Set<Long>>();

        events.forEach(event -> {
            if (!isValid(event)) {
                return;
            }

            var seenSet = seenById.computeIfAbsent(
                    event.id(),
                    k -> ConcurrentHashMap.newKeySet()
            );

            if (!seenSet.add(event.timestamp())) {
                return; // for duplicate timestamp for the same id, we ignore the event
            }

            statsById
                    .computeIfAbsent(event.id(), k -> new MutableStats())
                    .add(event);
        });

        return statsById.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().toImmutable()
                ));
    }

    private static boolean isValid(Event event) {
        if (event == null || event.id() == null || event.id().isBlank()) {
            return false;
        }
        double value = event.value();
        return !Double.isNaN(value) && !Double.isInfinite(value) && value >= 0.0;
    }

    private static final class MutableStats {
    private final LongAdder count = new LongAdder();
    private final DoubleAdder sum = new DoubleAdder();
    private final LongAccumulator min =
        new LongAccumulator(Long::min, Long.MAX_VALUE);
    private final LongAccumulator max =
        new LongAccumulator(Long::max, Long.MIN_VALUE);

        void add(Event event) {
            count.increment();
            sum.add(event.value());
            min.accumulate(event.timestamp());
            max.accumulate(event.timestamp());
        }

        AggregatedStats toImmutable() {
            long c = count.sum();
            double avg = c == 0 ? 0.0 : sum.sum() / c;
            return new AggregatedStats(c, min.get(), max.get(), avg);
        }
    }

}