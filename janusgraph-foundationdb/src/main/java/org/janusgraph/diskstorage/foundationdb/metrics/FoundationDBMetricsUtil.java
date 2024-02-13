package org.janusgraph.diskstorage.foundationdb.metrics;

import com.apple.foundationdb.FDBException;
import org.janusgraph.diskstorage.foundationdb.FoundationDBTx;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;

/**
 * The utility functions to support method-level instrumentation.
 *
 */
public class FoundationDBMetricsUtil {

    /**
     * transaction related utility methods.
     */
    public static class TransactionScope {

        public static void recordTransactionLatency(
                FoundationDBMetricGroup metricGroup, String operation,
                FoundationDBTx.IsolationLevel isolationLevel,
                long startTransactionTime, boolean succeeded) {
            long duration = System.nanoTime() - startTransactionTime;
            metricGroup.recordTransactionLatency(
                     isolationLevel.name(), operation,
                    duration/ FoundationDBMetricsDefs.TO_MILLISECONDS_CONVERSION, succeeded);
        }

        /**
         * This increases both transaction failures and the regular transactions
         */
        public static void incTransactionFailures(FoundationDBMetricGroup metricGroup,
                                                  String operation,
                                                  FoundationDBTx.IsolationLevel isolationLevel) {
            metricGroup.incTransactionFailures(isolationLevel.name(), operation);
            metricGroup.incTransactions(isolationLevel.name(), operation);
        }

        public static void incTransactions(FoundationDBMetricGroup metricGroup,
                                           String operation,
                                           FoundationDBTx.IsolationLevel isolationLevel) {
            metricGroup.incTransactions(isolationLevel.name(), operation);
        }

        public static void incTransactionsOpened (FoundationDBMetricGroup metricGroup,
                                                  FoundationDBTx.IsolationLevel isolationLevel ) {
            metricGroup.incTransactionsOpened(isolationLevel.name());
        }

        public static void incTransactionsClosed (FoundationDBMetricGroup metricGroup,
                                                  String operation,
                                                  FoundationDBTx.IsolationLevel isolationLevel ) {
            metricGroup.incTransactionsClosed(isolationLevel.name(), operation);
        }

        public static void recordTransactionReadVersion(FoundationDBMetricGroup metricGroup,
                                                        String isolation, long version) {
            metricGroup.recordTransactionReadVersion(isolation, version);
        }
    }

    /**
     * Storage plugin method level utility
     */
    public static class StoragePluginMethodScope {

        public static void recordMethodLatency(
                FoundationDBMetricGroup metricGroup, long startTime,  String operation, boolean succeeded) {
            long duration = System.nanoTime() - startTime;
            metricGroup.recordStoragePluginMethodCallLatency(operation,
                    duration/ FoundationDBMetricsDefs.TO_MILLISECONDS_CONVERSION, succeeded);
        }

        public static void incMethodCallCounts (FoundationDBMetricGroup metricGroup, String operation) {
            metricGroup.incTotalStoragePluginMethodCalls(operation);
        }

        public static void incMethodCallFailureCounts (FoundationDBMetricGroup metricGroup,
                                                       String operation, Exception ex) {
            if (ex instanceof TemporaryBackendException) {
                metricGroup.incTotalFailedStoragePluginMethodCalls(operation, "retriable");
            }
            else if (ex instanceof PermanentBackendException) {
                metricGroup.incTotalFailedStoragePluginMethodCalls(operation, "non_retriable");
            }
            else if (ex instanceof FDBException) {
                FDBException e = (FDBException)ex;
                //rely on the application-level retry mechanism for the graph transaction.
                metricGroup.incTotalFailedStoragePluginMethodCalls(operation, "non_retriable");
            }
            else {
                metricGroup.incTotalFailedStoragePluginMethodCalls(operation, "non_retriable");
            }

            //also increase the method call counts
            metricGroup.incTotalStoragePluginMethodCalls(operation);
        }

        /**
         *
         * @param operation can include getSlicesNonAsync or getSlicesAsync.
         * @param size the size of the queries included in one operation
         */
        public static void recordSizeOfMultiQueries (FoundationDBMetricGroup metricGroup, String operation, int size) {
            metricGroup.recordSizeOfMultiQueries (operation, size);
        }

    }


    /**
     * at the level of FDB method invocation over the FDB Java Library
     */
    public static class FdbMethodScope {

        public static void recordMethodLatency(FoundationDBMetricGroup metricGroup,
                                               long startTime, String operation, boolean succeeded) {
            long duration = System.nanoTime() - startTime;
            metricGroup.recordFdbMethodCallLatency (
                    operation,
                    duration / FoundationDBMetricsDefs.TO_MILLISECONDS_CONVERSION, succeeded);
        }

        public static void incMethodCallCounts(
                FoundationDBMetricGroup metricGroup, String operation) {
            metricGroup.incTotalFdbMethodCalls(operation);
        }

        /**
         * increase the failure counts, while at the same time, to increase the total method counts as well.
         */
        public static void incMethodCallFailureCounts(
                FoundationDBMetricGroup metricGroup, String operation, boolean retriable) {
            if (retriable) {
                metricGroup.incTotalFailedFdbMethodCalls (operation, "retriable");
            }
            else {
                metricGroup.incTotalFailedFdbMethodCalls (operation, "non_retriable");
            }

            //also increase the total method calls count.
            metricGroup.incTotalFdbMethodCalls(operation);
        }
    }
}
