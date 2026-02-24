package com.myorg.dataprocessing;

public record AggregatedStats(
        long count,
        long minTimestamp,
        long maxTimestamp,
        double average
) {}