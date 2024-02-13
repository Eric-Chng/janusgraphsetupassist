package org.janusgraph.metrics;

import io.prometheus.client.exporter.common.TextFormat;
import org.apache.commons.io.output.StringBuilderWriter;
import org.janusgraph.FoundationDBContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager;
import org.janusgraph.diskstorage.foundationdb.metrics.MetricsConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * to test the FoundationDB storage-plugin metrics
 */
@Testcontainers
public class FoundationDBMetricsTest {
    private static final Logger log = LoggerFactory.getLogger(FoundationDBMetricsTest.class);

    @Container
    public static FoundationDBContainer container = new FoundationDBContainer();

    public static FoundationDBStoreManager openStorageManager() throws BackendException {
        FoundationDBStoreManager sm = new FoundationDBStoreManager(container.getFoundationDBConfiguration());
        return sm;
    }


    public enum MetricType {
        PROMETHEUS_GAUGE,
        PROMETHEUS_COUNTER,
        PROMETHEUS_HISTOGRAM,
        PROMETHEUS_SUMMARY,
        PROMETHEUS_UNKNOWN;
    }

    public static class ParsedMetric{
        private final String fullMetricName;
        private final MetricType metricType;

        ParsedMetric (String metricName, String typeName) {
            fullMetricName = metricName;
            switch (typeName) {
                case "gauge":
                    metricType = MetricType.PROMETHEUS_GAUGE;
                    break;
                case "counter":
                    metricType = MetricType.PROMETHEUS_COUNTER;
                    break;
                case "histogram":
                    metricType = MetricType.PROMETHEUS_HISTOGRAM;
                    break;
                case "summary":
                    metricType = MetricType.PROMETHEUS_SUMMARY;
                    break;
                default:
                    metricType = MetricType.PROMETHEUS_UNKNOWN;
                    break;
            }
        }

        ParsedMetric (String metricName, MetricType type) {
            fullMetricName = metricName;
            metricType = type;
        }
    }

    private String[] retrievedLines;

    private HashMap <String, ParsedMetric> parseRetrievedMetrics() {
        HashMap<String, ParsedMetric> parsedResults = new HashMap<> ();


        for (String line : retrievedLines) {
            //parse each line
            String[] result = line.split("\\s");
            if (result.length == 4) {
                if ("#".equals(result[0]) && "TYPE".equals(result[1])) {
                    ParsedMetric metric= new ParsedMetric(result[2], result[3]);
                    parsedResults.put (metric.fullMetricName, metric);
                }
            }

        }

        return parsedResults;
    }

    @Before
    public void setup() throws IOException {
        try {
          FoundationDBStoreManager sm = openStorageManager();
          log.info("create foundationdb store manager and the required metrics");
        }
        catch (Exception ex) {

        }

        Writer writer = new StringBuilderWriter();
        TextFormat.write004(writer, MetricsConfiguration.getSamples());

        String output = writer.toString();

        retrievedLines = output.split("\\r?\\n");
    }

    @Test
    public void testFDBVersionMetric() {
        Assert.assertTrue (retrievedLines.length > 0);
        HashMap <String, ParsedMetric> parsedResults = parseRetrievedMetrics();

        //metrics and their types from service-method metric group.
        HashMap <String, ParsedMetric> expectedResults = new HashMap<>();
        expectedResults.put("nugraph_fdbstorage_fdb_version",
                new ParsedMetric("nugraph_fdbstorage_fdb_version",
                        MetricType.PROMETHEUS_GAUGE));

        //scan through the parsedResults
        Iterator<Map.Entry<String, ParsedMetric>> entries = expectedResults.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, ParsedMetric> entry = entries.next();
            ParsedMetric retrievedMetricType =  parsedResults.get(entry.getKey());
            Assert.assertTrue(retrievedMetricType.fullMetricName.equals(entry.getValue().fullMetricName));
            Assert.assertTrue (retrievedMetricType.metricType == entry.getValue().metricType);
        }
    }


    @Test
    public void testTransactionRelatedMetric() {
        Assert.assertTrue (retrievedLines.length > 0);
        HashMap <String, ParsedMetric> parsedResults = parseRetrievedMetrics();

        //metrics and their types from service-method metric group.
        HashMap <String, ParsedMetric> expectedResults = new HashMap<>();
        expectedResults.put("nugraph_fdbstorage_transaction_total",
                new ParsedMetric("nugraph_fdbstorage_transaction_total",
                        MetricType.PROMETHEUS_COUNTER));
        expectedResults.put("nugraph_fdbstorage_transaction_opened_total",
                new ParsedMetric("nugraph_fdbstorage_transaction_opened_total",
                        MetricType.PROMETHEUS_COUNTER));
        expectedResults.put("nugraph_fdbstorage_transaction_closed_total",
                new ParsedMetric("nugraph_fdbstorage_transaction_closed_total",
                        MetricType.PROMETHEUS_COUNTER));

        expectedResults.put("nugraph_fdbstorage_transaction_read_version",
                new ParsedMetric("nugraph_fdbstorage_transaction_read_version",
                        MetricType.PROMETHEUS_GAUGE));

        expectedResults.put("nugraph_fdbstorage_failed_transaction_total",
                new ParsedMetric("nugraph_fdbstorage_failed_transaction_total",
                        MetricType.PROMETHEUS_COUNTER));

        expectedResults.put("nugraph_fdbstorage_transaction_latency_millisecs",
                new ParsedMetric("nugraph_fdbstorage_transaction_latency_millisecs",
                        MetricType.PROMETHEUS_HISTOGRAM));

        //scan through the parsedResults
        Iterator<Map.Entry<String, ParsedMetric>> entries = expectedResults.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, ParsedMetric> entry = entries.next();
            ParsedMetric retrievedMetricType =  parsedResults.get(entry.getKey());
            Assert.assertTrue(retrievedMetricType.fullMetricName.equals(entry.getValue().fullMetricName));
            Assert.assertTrue (retrievedMetricType.metricType == entry.getValue().metricType);
        }
    }


    @Test
    public void testStoragePluginMethodRelatedMetric() {
        Assert.assertTrue (retrievedLines.length > 0);
        HashMap <String, ParsedMetric> parsedResults = parseRetrievedMetrics();

        //metrics and their types from service-method metric group.
        HashMap <String, ParsedMetric> expectedResults = new HashMap<>();
        expectedResults.put("nugraph_fdbstorage_storage_plugin_method_call_total",
                new ParsedMetric("nugraph_fdbstorage_storage_plugin_method_call_total",
                        MetricType.PROMETHEUS_COUNTER));

        expectedResults.put("nugraph_fdbstorage_failed_storage_plugin_method_call_total",
                new ParsedMetric("nugraph_fdbstorage_failed_storage_plugin_method_call_total",
                        MetricType.PROMETHEUS_COUNTER));

        expectedResults.put("nugraph_fdbstorage_storage_plugin_method_call_latency_millisecs",
                new ParsedMetric("nugraph_fdbstorage_storage_plugin_method_call_latency_millisecs",
                        MetricType.PROMETHEUS_HISTOGRAM));

        //scan through the parsedResults
        Iterator<Map.Entry<String, ParsedMetric>> entries = expectedResults.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, ParsedMetric> entry = entries.next();
            ParsedMetric retrievedMetricType =  parsedResults.get(entry.getKey());
            Assert.assertTrue(retrievedMetricType.fullMetricName.equals(entry.getValue().fullMetricName));
            Assert.assertTrue (retrievedMetricType.metricType == entry.getValue().metricType);
        }
    }

    @Test
    public void testFdbMethodRelatedMetric() {
        Assert.assertTrue (retrievedLines.length > 0);
        HashMap <String, ParsedMetric> parsedResults = parseRetrievedMetrics();

        //metrics and their types from service-method metric group.
        HashMap <String, ParsedMetric> expectedResults = new HashMap<>();
        expectedResults.put("nugraph_fdbstorage_fdb_method_call_total",
                new ParsedMetric("nugraph_fdbstorage_fdb_method_call_total",
                        MetricType.PROMETHEUS_COUNTER));

        expectedResults.put("nugraph_fdbstorage_failed_fdb_method_call_total",
                new ParsedMetric("nugraph_fdbstorage_failed_fdb_method_call_total",
                        MetricType.PROMETHEUS_COUNTER));

        expectedResults.put("nugraph_fdbstorage_fdb_method_call_latency_millisecs",
                new ParsedMetric("nugraph_fdbstorage_fdb_method_call_latency_millisecs",
                        MetricType.PROMETHEUS_HISTOGRAM));

        //scan through the parsedResults
        Iterator<Map.Entry<String, ParsedMetric>> entries = expectedResults.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, ParsedMetric> entry = entries.next();
            ParsedMetric retrievedMetricType =  parsedResults.get(entry.getKey());
            Assert.assertTrue(retrievedMetricType.fullMetricName.equals(entry.getValue().fullMetricName));
            Assert.assertTrue (retrievedMetricType.metricType == entry.getValue().metricType);
        }
    }

}
