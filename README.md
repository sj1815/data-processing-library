# Data Processing Library

## Overview
This library aggregates a stream of domain events by `id`, producing:
- total count of valid events
- minimum and maximum timestamp
- arithmetic mean of values

The implementation is stream-based, thread-safe, and deduplicates events by `(id, timestamp)`.

## Build & Test

```zsh
mvn test
```

## Design Decisions
- **Concurrency**: Uses `ConcurrentHashMap` with `LongAdder`, `DoubleAdder`, and `LongAccumulator` to support parallel streams safely.
- **Deduplication**: Tracks seen timestamps per `id` using a concurrent set to ignore duplicate `(id, timestamp)` events.
- **Validation**: Discards invalid events where `id` is null/blank or `value` is NaN, infinite, or negative.
- **Memory**: Avoids full materialization of the stream; only maintains per-id stats and dedupe state.

## Complexity
- **Time**: $O(n)$ for $n$ events
- **Memory**: $O(k + d)$ where $k$ is number of unique ids and $d$ is total number of distinct timestamps stored for deduplication

## Assumptions
- The stream is finite.
- Deduplication is defined as identical `id + timestamp` pairs.
