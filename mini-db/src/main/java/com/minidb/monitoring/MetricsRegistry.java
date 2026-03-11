package com.minidb.monitoring;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

public class MetricsRegistry {
    private static MeterRegistry instance = createDefaultRegistry();

    private static MeterRegistry createDefaultRegistry() {
        try {
            // Try to create a JMX registry
            return new JmxMeterRegistry(JmxConfig.DEFAULT, null);
        } catch (Exception e) {
            // If JMX is not available, fall back to a simple registry
            return new SimpleMeterRegistry();
        }
    }

    public static MeterRegistry getInstance() {
        return instance;
    }

    public static void setInstance(MeterRegistry newInstance) {
        instance = newInstance;
    }

    public static void useSimpleRegistry() {
        instance = new SimpleMeterRegistry();
    }
}
