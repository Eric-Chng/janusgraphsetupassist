package org.janusgraph.diskstorage.foundationdb.metrics;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

import java.util.Enumeration;

/**
 * Created by eBay NuGraph team
 */
public class MetricsConfiguration {

    /**
     * return the default registry
     */
    public static CollectorRegistry getRegistry(){
        return CollectorRegistry.defaultRegistry;
    }

    public static Enumeration<Collector.MetricFamilySamples> getSamples() {
        return CollectorRegistry.defaultRegistry.metricFamilySamples();
    }
}
