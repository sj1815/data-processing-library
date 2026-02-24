package com.myorg.dataprocessing;

public record Event(
        String id,
        long timestamp,
        double value
) {}
