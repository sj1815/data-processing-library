package com.myorg.dataprocessing;

import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

class EventAggregatorTest {

    @Test
    void aggregate_emptyStream_returnsEmptyMap() {
        Map<String, AggregatedStats> result = EventAggregator.aggregate(Stream.empty());
        assertTrue(result.isEmpty());
    }

    @Test
    void aggregate_singleEvent() {
        Event e = new Event("a", 100L, 10.0);
        Map<String, AggregatedStats> result = EventAggregator.aggregate(Stream.of(e));

        AggregatedStats stats = result.get("a");
        assertNotNull(stats);
        assertEquals(1, stats.count());
        assertEquals(100L, stats.minTimestamp());
        assertEquals(100L, stats.maxTimestamp());
        assertEquals(10.0, stats.average(), 0.0001);
    }

    @Test
    void aggregate_filtersInvalidEvents() {
        Event bad1 = new Event("a", 1L, -5.0);
        Event bad2 = new Event("a", 2L, Double.NaN);
        Event good = new Event("a", 3L, 7.0);

        Map<String, AggregatedStats> result =
                EventAggregator.aggregate(Stream.of(bad1, bad2, good));

        AggregatedStats stats = result.get("a");
        assertNotNull(stats);
        assertEquals(1, stats.count());
        assertEquals(7.0, stats.average(), 0.0001);
    }

    @Test
    void aggregate_deduplicatesByIdAndTimestamp() {
        Event e1 = new Event("x", 10L, 1.0);
        Event e2 = new Event("x", 10L, 999.0); // duplicate timestamp
        Event e3 = new Event("x", 20L, 3.0);

        Map<String, AggregatedStats> result =
                EventAggregator.aggregate(Stream.of(e1, e2, e3));

        AggregatedStats stats = result.get("x");
        assertEquals(2, stats.count());
        assertEquals(10L, stats.minTimestamp());
        assertEquals(20L, stats.maxTimestamp());
        assertEquals(2.0, stats.average(), 0.0001);
    }

    @Test
    void aggregate_handlesOutOfOrderTimestamps() {
        Event e1 = new Event("b", 300L, 2.0);
        Event e2 = new Event("b", 100L, 4.0);
        Event e3 = new Event("b", 200L, 6.0);

        Map<String, AggregatedStats> result =
                EventAggregator.aggregate(Stream.of(e1, e2, e3));

        AggregatedStats stats = result.get("b");
        assertEquals(3, stats.count());
        assertEquals(100L, stats.minTimestamp());
        assertEquals(300L, stats.maxTimestamp());
        assertEquals(4.0, stats.average(), 0.0001);
    }

    @Test
    void aggregate_parallelStream() {
        Stream<Event> events = Stream.of(
                new Event("p", 1L, 1.0),
                new Event("p", 2L, 2.0),
                new Event("p", 3L, 3.0),
                new Event("q", 1L, 4.0)
        ).parallel();

        Map<String, AggregatedStats> result = EventAggregator.aggregate(events);

        assertEquals(2, result.size());
        assertEquals(2.0, result.get("p").average(), 0.0001);
        assertEquals(1, result.get("q").count());
    }
}
