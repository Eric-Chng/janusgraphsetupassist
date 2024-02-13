package org.janusgraph.diskstorage.foundationdb.metrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

/**
 * Created by eBay NuGraph team
 *
 * Note: This is a singleton object as the involved metrics can only be registered to the metrics registry once, whereas
 * the FoundationDB Store Manager can be launched multiple times by JanusGraph during the initialization phase.
 */
public class FoundationDBMetricGroup {

    /**
     * the version of the foundationdb currently deployed
     */
    private Gauge fdbVersion;

    /**
     * the total number of the transactions completed,including the ones that failed.
     */
    private Counter totalTransactions;


    /**
     * the total number of the transactions that have been closed. This number should match the number of
     * total transactions opened. This is to check whether some applications did not close the transactions somehow.
     */
    private Counter totalTransactionsOpened;

    /**
     * the total number of the transactions that have been closed. This number should match the number of
     * total transactions opened. This is to check whether some applications did not close the transactions somehow.
     */
    private Counter totalTransactionsClosed;


    /**
     * to keep track of the read version that has been pre-fetched from the primary dc, if the current DC is the
     * standby DC.
     */
    private Gauge transactionReadVersion;


    /**
     * the total number of the transactions that have failed.
     */
    private Counter totalFailedTransactions;

    /**
     * the total transaction latency time, which can include many calls. Currently, we do not differentiate
     * the latencies are from the transactions that succeeded and the transactions that failed.
     */
    private Histogram transactionLatency;


    /**
     * the total number of the method calls invoked at the storage plugin interface level
     */
    private Counter totalStoragePluginMethodCalls;

    /**
     * the total number of the method calls failed when being invoked at the storage plugin interface level
     * including:
     *     (1)permanent failures and
     *     (2)temporary failures
     *
     * The total failures are the sum of the permanent failures and temporary failures.
     */
    private Counter totalFailedStoragePluginMethodCalls;

    /**
     * the latency spent on each individual methods defined at the storage plugin interface level
     *
     */
    private Histogram storagePluginMethodCallLatency;

    /**
     * The size of the queries included in a getSlices() method call
     */
    private Histogram storagePluginMethodSizeOfMultiQueries;


    /**
     * The total number of the FDB method calls invoked at the FDB Java Library level
     */
    private Counter  totalFdbMethodCalls;

    /**
     * The counter for the failure on the FDB method calls invoked at the FDB Java Library level
     */
    private Counter  totalFailedFdbMethodCalls;

    /**
     * the latency spent on the FDB method calls invoked at the FDB Java Library level
     */
    private Histogram fdbMethodCallLatency;


    //to turn it into a singleton
    private static volatile FoundationDBMetricGroup instance = null;


    public static FoundationDBMetricGroup getInstance() {
        if (instance == null) {
            synchronized (FoundationDBMetricGroup.class) {
                if (instance == null) {
                    instance = new FoundationDBMetricGroup(MetricsConfiguration.getRegistry());
                }
            }
        }

        return instance;
    }

    /**
     * This will be registered, in the Storage Manager.
     */
    private FoundationDBMetricGroup (CollectorRegistry registry) {

        if (registry == null) {
            throw new RuntimeException ("metrics collection registry is null");
        }

        //version
        fdbVersion = Gauge.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name ("fdb_version")
                .help("current FDB version")
                .labelNames("version")
                .create();

        registry.register(fdbVersion);


        /**
         * The Label Names related to the transactions:
         * (1) isolation: whether this is serializable or not
         * (2) operation: read (that is, read-only), and write (including read-write and write-only)
         *
         */
        totalTransactions = Counter.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("transaction_total")
                .help("Total number of the transactions that have been issued")
                .labelNames("isolation", "operation")
                .create();

        registry.register(totalTransactions);


        //at the time of transaction opened, we do not know whether this is read-only or read-write transaction.
        totalTransactionsOpened = Counter.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("transaction_opened_total")
                .help("Total number of the transactions that have been opened")
                .labelNames("isolation")
                .create();
        registry.register(totalTransactionsOpened);

        totalTransactionsClosed = Counter.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("transaction_closed_total")
                .help("Total number of the transactions that have been closed")
                .labelNames("isolation", "operation")
                .create();
        registry.register(totalTransactionsClosed);

        transactionReadVersion = Gauge.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("transaction_read_version")
                .help("The transaction read version used from version prefetcher")
                .labelNames("isolation")
                .create();
        registry.register(transactionReadVersion);

        totalFailedTransactions = Counter.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("failed_transaction_total")
                .help("Total number of the transactions that have failed")
                .labelNames("isolation", "operation")
                .create();

        registry.register(totalFailedTransactions);

        /**
         * separate failed transaction from succeeded transaction. Thus, we have a new label:
         * status: succeeded, failed.
         */
        transactionLatency = Histogram.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("transaction_latency_millisecs")
                .help("Transaction latency in milliseconds")
                .buckets(FoundationDBMetricsDefs.buckets)
                .labelNames("isolation", "operation", "status")
                .create();

        registry.register(transactionLatency);


        /**
         * The label names related to the method calls defined at the storage plugin level:
         *   operation: get, insert, getSlice, getSlices
         */
        totalStoragePluginMethodCalls = Counter.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("storage_plugin_method_call_total")
                .help("Total number of the method calls at the storage plugin interface level")
                .labelNames("operation")
                .create();

        registry.register(totalStoragePluginMethodCalls);

        /**
         * The label names related to the method calls that failed
         * (1) operation: get, insert, getSlice, getSlices
         * (2) failure_type: temporary, permanent
         */
        totalFailedStoragePluginMethodCalls = Counter.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("failed_storage_plugin_method_call_total")
                .help("Total number of the failed method calls at the storage plugin interface level")
                .labelNames("operation", "failure_type")
                .create();

        registry.register(totalFailedStoragePluginMethodCalls);

        /**
         * separate failed transaction from succeeded transaction. Thus, we have a new label:
         * status: succeeded, failed.
         */
        storagePluginMethodCallLatency = Histogram.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("storage_plugin_method_call_latency_millisecs")
                .help("Method call latency at the storage plugin interface level")
                .buckets(FoundationDBMetricsDefs.buckets)
                .labelNames("operation", "status")
                .create();

        registry.register(storagePluginMethodCallLatency);

        /**
         * the getSlices() size tracking using histogram
         *
         */
        storagePluginMethodSizeOfMultiQueries = Histogram.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("storage_plugin_size_of_multi_queries")
                .help("Size of multi-queries included in a getSlices call")
                .buckets(FoundationDBMetricsDefs.qbuckets)
                .labelNames("operation")
                .create();

        registry.register(storagePluginMethodSizeOfMultiQueries);


        /**
         * The label names related to the FDB method calls invoked at the FDB Java Library level
         *   operation: get, getRange, getMultiRange
         */
        totalFdbMethodCalls = Counter.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("fdb_method_call_total")
                .help("Total number of the method calls at the fdb library level")
                .labelNames("operation")
                .create();

        registry.register(totalFdbMethodCalls);


        /**
         * The label names related to the method calls that failed:
         * (1) operation: get, getRange, getMultiRange, getSlice, getSlices
         * (2) failure_retype: retryable, non_retryable.
         */
        totalFailedFdbMethodCalls = Counter.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("failed_fdb_method_call_total")
                .help("Total number of the failed method calls at the fdb library level")
                .labelNames("operation", "failure_type")
                .create();

        registry.register(totalFailedFdbMethodCalls);

        /**
         * separate failed transaction from succeeded transaction. Thus, we have a new label:
         * status: succeeded, failed.
         */
        fdbMethodCallLatency = Histogram.build().namespace(FoundationDBMetricsDefs.NAMESPACE)
                .subsystem(FoundationDBMetricsDefs.SUBSYSTEM)
                .name("fdb_method_call_latency_millisecs")
                .help("Method call latency at the fdb library level")
                .buckets(FoundationDBMetricsDefs.buckets)
                .labelNames("operation", "status")
                .create();

        registry.register(fdbMethodCallLatency);

    }


    /**
     * The version is recorded in the label while the Gauge value is always set to 1.
     */
    public void recordFDBVersion (String version) {
        fdbVersion.labels(version).set(1);
    }


    public void incTransactions(String isolation, String operation) {
        totalTransactions.labels(isolation, operation).inc();
    }


    public void incTransactionFailures (String isolation, String operation) {
        totalFailedTransactions.labels(isolation, operation).inc();
    }


    /**
     * For transaction openning, we only have isolation label to be provided.
     */
    public void incTransactionsOpened (String isolation) {
        totalTransactionsOpened.labels(isolation).inc();
    }

    /**
     * For transaction close, we can have the operation = read or write to be known, besides operation
     */
    public void incTransactionsClosed (String isolation, String operation) {
        totalTransactionsClosed.labels(isolation, operation).inc();
    }


    public void recordTransactionReadVersion(String isolation, long version) {
        transactionReadVersion.labels(isolation).set(version);
    }


    /**
     * (1) isolation: whether this is serializable or not
     * (2) operation: read, or write (including read or write)
     *
     * @param latency in milliseconds
     */
    public void recordTransactionLatency(String isolation, String operation, double latency, boolean succeeded) {
        if (succeeded) {
            transactionLatency.labels(isolation, operation, "succeeded").observe(latency);
        }
        else{
            transactionLatency.labels(isolation, operation, "failed").observe(latency);
        }
    }



    public void incTotalStoragePluginMethodCalls(String operation) {
        totalStoragePluginMethodCalls.labels(operation).inc();
    }

    /**
     * failure type is: temporary or permanent
     * @param operation
     * @param failureType
     */
    public void incTotalFailedStoragePluginMethodCalls(String operation, String failureType) {
        totalFailedStoragePluginMethodCalls.labels(operation, failureType).inc();
    }

    /**
     * @param latency in milliseconds
     */
    public void  recordStoragePluginMethodCallLatency (String operation, double latency, boolean succeeded) {
        if (succeeded) {
            storagePluginMethodCallLatency.labels(operation, "succeeded").observe(latency);
        }
        else {
            storagePluginMethodCallLatency.labels(operation, "failed").observe(latency);
        }
    }



    public void recordSizeOfMultiQueries (String operation, int size) {
        storagePluginMethodSizeOfMultiQueries.labels (operation).observe((double)size);
    }


    public void incTotalFdbMethodCalls(String operation){
        totalFdbMethodCalls.labels(operation).inc();
    }

    public void incTotalFailedFdbMethodCalls(String operation, String failureType) {
        totalFailedFdbMethodCalls.labels(operation, failureType).inc();
    }

    /**
     * @param latency in milliseconds
     */
    public void recordFdbMethodCallLatency (String operation, double latency, boolean succeeded) {
        if (succeeded) {
            fdbMethodCallLatency.labels(operation, "succeeded").observe(latency);
        }
        else {
            fdbMethodCallLatency.labels(operation, "failed").observe(latency);
        }
    }

}
