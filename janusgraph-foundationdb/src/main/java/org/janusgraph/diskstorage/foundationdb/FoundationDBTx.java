package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.ebay.nugraph.common.CallContext;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsDefs;
import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsDefs.*;
import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsUtil;
import org.janusgraph.diskstorage.foundationdb.utils.LogWithCallContext;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.janusgraph.diskstorage.foundationdb.FoundationDBTxErrorCode.GET_MULTI_RANGE_ERROR;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBTx extends AbstractStoreTransaction {
    private static final Logger log = LoggerFactory.getLogger(FoundationDBTx.class);

    private volatile Transaction tx;
    private final Database db;

    private List<Insert> inserts = Collections.synchronizedList(new ArrayList<>());
    private List<byte[]> deletions = Collections.synchronizedList(new ArrayList<>());

    private static final int MAX_RETRIES = 10;

    public static final int TRANSACTION_TOO_OLD_CODE = 1007;
    public static final int TRANSACTION_WAS_CANCELED = 1025;

    private final AtomicLong totalFetched;
    private final AtomicInteger totalIters;

    private Logger txLogger = LoggerFactory.getLogger("txprofile");

    public enum IsolationLevel {
        SERIALIZABLE,                   // Reads and writes are in one FDB transaction. Restart is NOT allowed
        READ_SNAPSHOT,                  // Transaction is read-only. Restart is NOT allowed
        READ_COMMITTED,                 // Transaction is read-only. Multiple restarts are allowed
        READ_COMMITTED_NO_WRITE,        // Whether transaction is read-only is not known at the start of the
                                        // transaction. If there is no write, transaction can restart multiple times.
                                        // Otherwise, if there is a write, transaction restart is not allowed. If
                                        // transaction was previously restarted, it is also aborted.
        READ_COMMITTED_WITH_WRITE       // Read and writes may be restarted multiple times
    }
    private final IsolationLevel isolationLevel;

    // When transaction is allowed to restart multiple times, each time it is called a "term".
    // Term id used to manage transaction restarts between terms
    private final AtomicInteger termId = new AtomicInteger(0);

    private final String transactionId;

    private long initialTxCrtTime = 0; // the initial Tx creation time;

    private FoundationDBStoreManager storeManager;
    private long startTransactionTime;

    public final long maxTraversalTimeoutMs;

    // Whether the transaction was rolled back or not
    // since we cannot rely on tx.cancel() to know whether a transaction was rolled back (it could be the FDB
    // transaction is restarted in a multi-thread scenario)
    private volatile boolean isRolledBack = false;

    private final List<TxGetSliceProfiler> profilers = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger wave = new AtomicInteger(0);

    private final boolean isJanusSysTx;

    public FoundationDBTx(Database db, Transaction t, String transactionId, BaseTransactionConfig config,
                          IsolationLevel isolationLevel, FoundationDBStoreManager storeManager,
                          CallContext.ReadMode requestedReadMode, long maxTraversalTimeoutMs) {
        super(config);
        this.tx = t;
        this.db = db;
        this.totalFetched = new AtomicLong(0);
        this.totalIters = new AtomicInteger(0);

        this.isJanusSysTx = getConfiguration().getGroupName() != null &&
                getConfiguration().getGroupName().startsWith("org.janusgraph.sys");

        switch (requestedReadMode) {
            case READ_SNAPSHOT:
                // Client specifies that the transaction is read-only and READ_SNAPSHOT
                this.isolationLevel = IsolationLevel.READ_SNAPSHOT;
                break;
            case READ_COMMITTED:
                // Client specifies that the transaction is read-only and READ_COMMITTED
                this.isolationLevel = IsolationLevel.READ_COMMITTED;
                break;
            case NOT_SPECIFIED:
            default:
                // Client does not specify a preference for read, use the keyspace-wise isolation level
                this.isolationLevel = isolationLevel;
                break;
        }

        //add the transaction id, to see the query source
        this.transactionId = transactionId;

        this.maxTraversalTimeoutMs = maxTraversalTimeoutMs;
        this.storeManager = storeManager;
        this.startTransactionTime = System.nanoTime();
        this.initialTxCrtTime = System.currentTimeMillis();

        if (log.isDebugEnabled()) {
            log.debug("Start transaction: id={}, isolationLevel={}, startTime={}", transactionId, this.isolationLevel,
                    initialTxCrtTime);
        }

        FoundationDBMetricsUtil.TransactionScope.incTransactionsOpened(storeManager.metricGroup, this.isolationLevel);
    }

    public static int getMaxRetries() {
        return MAX_RETRIES;
    }

    public int getTermId() {
        return termId.get();
    }

    /***
     * Get and log the FDBException from the throwable object.
     * Return an FDBException object if found, otherwise return null.
     * */
    private FDBException getFDBException(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof FDBException) {
                FDBException fe = (FDBException) current;
                if (log.isDebugEnabled()) {
                    String message= String.format("Catch FDBException code=%s, isRetryable=%s, isMaybeCommitted=%s, "
                                    + "isRetryableNotCommitted=%s, isSuccess=%s",
                            fe.getCode(), fe.isRetryable(),
                            fe.isMaybeCommitted(), fe.isRetryableNotCommitted(), fe.isSuccess());
                    LogWithCallContext.logDebug(log, message);
                }
                return fe;
            }
            current = current.getCause();
        }

        return null;
    }

    private void cancelAndClose(Transaction tx) {
        if (tx != null) {
            try {
                tx.cancel();
            } catch (Exception ex) {
                if (log.isErrorEnabled()) {
                    LogWithCallContext.logError(log, "Exception when cancel transaction: " + ex.getMessage());
                }
            } finally {
                try {
                    tx.close();
                } catch (Exception e) {
                    if (log.isErrorEnabled()) {
                        LogWithCallContext.logError(log, "Exception when closing transaction: " + e.getMessage());
                    }
                }
            }

            FoundationDBMetricsUtil.TransactionScope.incTransactionsClosed(
                    storeManager.metricGroup, getOpType(), isolationLevel);
        } else {
            log.warn("Got a null transaction object when closing");
        }
    }

    /**
     * Multiple threads may execute at the same term T and all restart due to FDB transaction timeout.
     * Only one thread can perform the actual transaction restart to start a new term T+1.
     * Other threads are blocked waiting for this thread to start this new term.
     * @param localTermId The term id the thread got before executing the operation causing exception to be thrown
     *                    and invoke this restart() method.
     */
    public void restart(int localTermId, Throwable lastThrowable) {
        checkTransactionRestartable(lastThrowable);
        int globalTermId = termId.get();
        if (localTermId == globalTermId) {
            int retries = 0;
            while (retries++ < MAX_RETRIES) {
                try {
                    // All threads with the same localTermId are blocked here, so only one can proceed
                    synchronized(termId) {
                        if (localTermId == termId.get()) {
                            if (log.isDebugEnabled()) {
                                log.debug("Thread {} to restart transaction", Thread.currentThread().getId());
                            }

                            // Start new transaction before closing the old one
                            Transaction oldTx = tx;
                            tx = db.createTransaction();

                            // Reapply mutations but do not clear them out just in case this transaction also
                            // times out and they need to be reapplied.
                            //
                            // @todo Note that at this point, the large transaction case (tx exceeds 10,000,000 bytes) is not handled.
                            inserts.forEach(insert -> tx.set(insert.getKey(), insert.getValue()));
                            deletions.forEach(delete -> tx.clear(delete));

                            // Reset the timer
                            this.startTransactionTime = System.nanoTime();
                            FoundationDBMetricsUtil.TransactionScope.incTransactionsOpened(storeManager.metricGroup, isolationLevel);

                            // Clean up old transaction
                            cancelAndClose(oldTx);

                            // Only increase the term id when the new transaction was established successfully
                            termId.incrementAndGet();
                        }
                    }
                    break;
                } catch (Exception e) {
                    log.error("Transaction restart throws an exception", e);

                    // Just retry
                    delayNextTxRestart(50);
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Thread id {} has localTermId {} < termId {}", Thread.currentThread().getId(),
                        localTermId, globalTermId);
            }

            delayNextTxRestart(50);
            // New term was likely released, so the transaction state should be good now.
        }
    }

    private void delayNextTxRestart(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException ex) {
            log.error("Thread interrupted during sleeping", ex);
        }
    }

    @Override
    public void rollback() throws BackendException {
        super.rollback();

        isRolledBack = true;

        String opType = getOpType();
        if (tx == null) {
            if (log.isWarnEnabled()) {
                log.warn("in execution mode: {} and when rollback, encounter FDB transaction object to be null", isolationLevel.name());
            }
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("{} rolled back, num_iters={}, num_fetched={}, num_inserts={}, num_deletes={}", this,
                    getTotalIters(), getTotalFetched(), inserts.size(), deletions.size());
        }

        cancelAndClose(tx);

        if ((LogWithCallContext.isLogAuditEnabled() || LogWithCallContext.isLogQueryEnabled())
                && (inserts.size() > 0 || deletions.size() > 0)) {
            String entityCount = String.format("num_inserts=%d, num_deletes=%d", inserts.size(), deletions.size());
            String result = "Transaction rolled back";
            if (LogWithCallContext.isLogAuditEnabled()) {
                LogWithCallContext.logAudit(entityCount, null, result);
            }
            if (LogWithCallContext.isLogQueryEnabled()) {
                LogWithCallContext.logQuery(String.format("%s: %s", result, entityCount));
            }
        }

        logTxProfile(false);

        // Rollback always indicating transaction failure.
        FoundationDBMetricsUtil.TransactionScope.recordTransactionLatency(
                storeManager.metricGroup, opType, isolationLevel, startTransactionTime, false);
        FoundationDBMetricsUtil.TransactionScope.incTransactionFailures(
                storeManager.metricGroup, opType, isolationLevel);
    }

    @Override
    public void commit() throws BackendException {
        boolean failing = true;
        int counter;

        String opType = getOpType();
        Throwable lastException = null;
        for (counter = 0; counter < MAX_RETRIES; counter++) {
            super.commit();

            if (tx == null) {
                if (log.isWarnEnabled()) {
                    log.warn("in execution mode: {} and when commit, encounter FDB transaction object to be null", isolationLevel.name());
                }
                return;
            }

            int localTxCtr = termId.get();
            try {
                if (isReadOnly()) {
                    // nothing to commit so skip it
                    tx.cancel();
                } else {
                    tx.commit().get();

                    if (LogWithCallContext.isLogAuditEnabled() || LogWithCallContext.isLogQueryEnabled()) {
                        String entityCount = String.format("num_inserts=%d, num_deletes=%d", inserts.size(), deletions.size());
                        String result = "Transaction committed";
                        if (LogWithCallContext.isLogAuditEnabled()) {
                            LogWithCallContext.logAudit(entityCount, null, result);
                        }
                        if (LogWithCallContext.isLogQueryEnabled()) {
                            LogWithCallContext.logQuery(String.format("%s: %s", result, entityCount));
                        }
                    }
                }

                tx.close();
                tx = null;
                if (log.isDebugEnabled()) {
                    log.debug("{} committed, num_iters: {}, num_fetched: {}, num_inserts={}, num_deletes={}", this,
                            getTotalIters(), getTotalFetched(), inserts.size(), deletions.size());
                }

                failing = false;
                break;
            } catch (IllegalStateException | ExecutionException e) {
                if (log.isErrorEnabled()) {
                    String message=
                            "commit encounter exceptions: " + e.getMessage() + " retries: " + counter + " maxRuns: " + MAX_RETRIES
                            + " will be re-started for inserts: " + inserts.size() + " deletes: " + deletions.size();
                    LogWithCallContext.logError(log, message, e);
                }

                //report metrics first before handle the exception (that can potentially throw another exception).
                lastException = e;
                FDBException lastFDBException = getFDBException(e);

                //need to put the following first, as otherwise restart will reset the timer.
                FoundationDBMetricsUtil.TransactionScope.recordTransactionLatency(
                        storeManager.metricGroup, opType, isolationLevel,
                        startTransactionTime, false);
                FoundationDBMetricsUtil.TransactionScope.incTransactionFailures(
                        storeManager.metricGroup, opType, isolationLevel);
                FoundationDBMetricsUtil.TransactionScope.incTransactionsClosed(
                        storeManager.metricGroup, opType, isolationLevel);

                // Only retry if not the last one
                if (counter+1 != MAX_RETRIES) {
                    try {
                        restart(localTxCtr, lastFDBException);
                    } catch (Throwable t) {
                        if (t instanceof FoundationDBTxException) {
                            if (log.isErrorEnabled()) {
                                String message = String.format("commit fails (not retry): inserts=%d, deletes=%d",
                                        inserts.size(), deletions.size());
                                LogWithCallContext.logError(log, message, t);
                            }
                            throw t;
                        } else {
                            if (log.isErrorEnabled()) {
                                String message = String.format("commit fails (will retry): inserts=%d, deletes=%d",
                                        inserts.size(), deletions.size());
                                LogWithCallContext.logError(log, message, t);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                cancelAndClose(tx);

                FoundationDBMetricsUtil.TransactionScope.recordTransactionLatency(
                        storeManager.metricGroup, opType, isolationLevel,
                        startTransactionTime, false);
                FoundationDBMetricsUtil.TransactionScope.incTransactionFailures(
                        storeManager.metricGroup, opType, isolationLevel);

                if (log.isErrorEnabled()) {
                    LogWithCallContext.logError(log, "commit encountered exception: " + e.getMessage());
                }

                throw new PermanentBackendException("Transaction failed to commit", e);
            }
        }

        if (failing) {
            //Note: we already record the counter and latency in the failure path.
            if (log.isErrorEnabled()) {
                String message = "commit has the final result with failing (should be true): " + failing + " maxRuns: " + MAX_RETRIES +
                        " inserts: " + inserts.size() + " deletes: " + deletions.size()
                        + " total re-starts: " + counter;
                LogWithCallContext.logError(log, message);
            }

            if (LogWithCallContext.isLogAuditEnabled() || LogWithCallContext.isLogQueryEnabled()) {
                String entityCount = String.format("num_inserts=%d, num_deletes=%d", inserts.size(), deletions.size());
                String result = "Transaction failed to commit";
                if (LogWithCallContext.isLogAuditEnabled()) {
                    LogWithCallContext.logAudit(entityCount, null, result);
                }
                if (LogWithCallContext.isLogQueryEnabled()) {
                    LogWithCallContext.logQuery(String.format("%s: %s", result, entityCount));
                }
            }

            logTxProfile(false);

            //Note: even if the commit is retriable and the TemporaryBackendException is thrown here, at the commit(.)
            //method of StandardJanusGraph class, the thrown exception will be translated to rollback(.) and then
            //throw further JanusGraphException to the application. Thus, it is better to just throw the
            //PermanentBackendException here. as at this late commit stage, there is no retry logic defined
            //at the StandardJanusGraph class.
            //if (isRetriable)
            //    throw new TemporaryBackendException("Max transaction count exceed but transaction is retriable");
            //else
            //    throw new PermanentBackendException("Max transaction reset count exceeded");

            // There is no need to capture metrics before throwing PermanentBackendException.
            // This is because when the control reaches here, the max retries have been exhausted,
            // and in each retry loop, the metrics have been recorded already.
            throw new PermanentBackendException("Transaction failed to commit", lastException);
        }

        logTxProfile(true);

        // Record metric for success path
        FoundationDBMetricsUtil.TransactionScope.recordTransactionLatency(
                storeManager.metricGroup, opType, isolationLevel, startTransactionTime, true);
        FoundationDBMetricsUtil.TransactionScope.incTransactions(
                storeManager.metricGroup, opType, isolationLevel);
        FoundationDBMetricsUtil.TransactionScope.incTransactionsClosed(
                storeManager.metricGroup, opType, isolationLevel);
    }

    private void logTxProfile(boolean isCommitted) {
        if (!isJanusSysTx && storeManager.isTxProfileEnable() &&
                ((txLogger != null && txLogger.isDebugEnabled()) || LogWithCallContext.isLogQueryEnabled())) {
            double durationMs = (System.nanoTime() - startTransactionTime) / Math.pow(10, 6);

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("{\"committed:\": %s, \"restarts\": %d, \"txLatencyMs\": %.2f, \"profile\": [",
                    isCommitted, termId.get(), durationMs));
            for (TxGetSliceProfiler profiler: profilers) {
                sb.append(profiler);
                sb.append(",");
            }
            if (sb.length() > 1) {
                sb.setLength(sb.length() - 1);
            }
            sb.append("]}");

            if (LogWithCallContext.isLogQueryEnabled()) {
                LogWithCallContext.logQuery(sb.toString());
            } else {
                if (txLogger != null && txLogger.isDebugEnabled()) {
                    txLogger.debug(String.format("TxId %s: %s", transactionId, sb));
                }
            }
        }
    }

    /**
     * We changed the implementation to not rely on both the inserts list and the deletions list to be empty. This is
     * because if the transaction has the inserts list and the deletions list to be empty, the FDB client library can
     * automatically detect that this transaction has its write-set to be empty, and then turn on the optimization to
     * declare that this transaction is a read-only transaction. We do not need to encode the same decision logic in
     * this method. The detail can be found at this FDB forum post:
     * https://forums.foundationdb.org/t/questions-regarding-fdb-transaction-conflict-on-two-concurrent-transactions/2913/9
     */
    private boolean isReadOnly() {
        return (isolationLevel == IsolationLevel.READ_COMMITTED || isolationLevel == IsolationLevel.READ_SNAPSHOT);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == tx ? "nulltx" : tx.toString());
    }

    public byte[] get(final byte[] key) {
        if (log.isDebugEnabled()) {
            String message = "get-key comes into the normal flow with key: " + Arrays.toString(key)
                    + " thread id: " + Thread.currentThread().getId()
                    + " transaction id: " + this.transactionId;
            LogWithCallContext.logDebug(log, message);
        }

        long callStartTime = System.nanoTime();

        Throwable lastException = null;
        FDBException lastFDBException = null;
        for (int i = 0; i < MAX_RETRIES; i++) {
            int localTxCtr = this.termId.get();
            try {
                byte[] value = this.tx.get(key).get();

                FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(storeManager.metricGroup, callStartTime,
                        FDB_TNX_METHOD_CALL.get, true);
                FoundationDBMetricsUtil.FdbMethodScope.incMethodCallCounts(storeManager.metricGroup,
                        FDB_TNX_METHOD_CALL.get);

                if (log.isDebugEnabled()) {
                    long callEndStartTime = System.nanoTime();
                    long total_time = callEndStartTime - callStartTime;
                    String message = "get-key takes: "
                            + total_time / FoundationDBMetricsDefs.TO_MILLISECONDS_CONVERSION + "(ms)"
                            + " thread id: " +  Thread.currentThread().getId()
                            + " transaction id: " + this.transactionId;
                    LogWithCallContext.logDebug(log, message);

                }

                return value;
            } catch (ExecutionException e) {
                if (log.isErrorEnabled()) {
                    LogWithCallContext.logError(log, "get encountered exception: " + e.getMessage());
                }

                lastException = e;
                lastFDBException = getFDBException(e);

                if (i+1 != MAX_RETRIES) {
                    try {
                        restart(localTxCtr, lastFDBException);
                    } catch (Throwable t) {
                        if (log.isDebugEnabled()) {
                            log.error("restart get() got exception: {}, number of retries: {}", t.getMessage(), i);
                        }
                        if (t instanceof FoundationDBTxException) {
                            FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(storeManager.metricGroup, callStartTime,
                                    FDB_TNX_METHOD_CALL.get, false);
                            FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(storeManager.metricGroup,
                                    FDB_TNX_METHOD_CALL.get, false);
                            throw t;
                        }
                    }
                } else {
                    break;
                }
            } catch (Exception e) {
                if (log.isErrorEnabled()) {
                    LogWithCallContext.logError(log, "get encountered exception: " + e.getMessage());
                }

                lastException = e;
                break;
            }
        }

        FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(storeManager.metricGroup, callStartTime,
                FDB_TNX_METHOD_CALL.get, false);
        FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(storeManager.metricGroup,
                FDB_TNX_METHOD_CALL.get, false);

        if (log.isErrorEnabled()) {
            String message = "get failed with final exception " + lastException;
            LogWithCallContext.logError(log, message);
        }

        String msg = "Max transaction reset count exceeded in get()";
        throw constructFoundationDBTxException(msg, lastFDBException, FoundationDBTxErrorCode.MAX_TRANSACTION_RESET);
    }

    private FoundationDBTxException constructFoundationDBTxException(String msg, FDBException lastFDBException, int defaultErrCode) {
        if (lastFDBException != null) {
            return FoundationDBTxException.withLastFDBException(msg, lastFDBException);
        } else {
            return FoundationDBTxException.withErrorCode(msg, defaultErrCode);
        }
    }

    public List<KeyValue> getRange(final FoundationDBRangeQuery query) {
        long callStartTime = System.nanoTime();
        Throwable lastException = null;
        FDBException lastFDBException = null;
        for (int i = 0; i < MAX_RETRIES; i++) {
            final int localTxCtr = termId.get();
            try {
                List<KeyValue> result = tx.getRange(query.getStartKeySelector(), query.getEndKeySelector(),
                        query.getLimit(), false, StreamingMode.WANT_ALL).asList().get();

                FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(storeManager.metricGroup, callStartTime,
                        FDB_TNX_METHOD_CALL.getRange, true);
                FoundationDBMetricsUtil.FdbMethodScope.incMethodCallCounts(storeManager.metricGroup,
                        FDB_TNX_METHOD_CALL.getRange);

                if (log.isDebugEnabled()) {
                    String message = "getRange comes into the normal flow with startkey: " +
                            Arrays.toString(query.getStartKeySelector().getKey()) + " endkey: "
                            + Arrays.toString(query.getEndKeySelector().getKey()) +
                            " limit: " + query.getLimit()
                            + " thread id: " + Thread.currentThread().getId()
                            + " transaction id: " + this.transactionId;
                    LogWithCallContext.logDebug(log, message);
                }

                return result == null ? Collections.emptyList() : result;
            } catch (ExecutionException e) {
                if (log.isErrorEnabled()) {
                    LogWithCallContext.logError(log, "getRange encountered exception: " + e.getMessage());
                }

                lastException = e;
                lastFDBException = getFDBException(e);

                if (i+1 != MAX_RETRIES) {
                    try {
                        restart(localTxCtr, lastFDBException);
                    } catch (Throwable t) {
                        if (log.isDebugEnabled()) {
                            log.error("restart getRange() got exception: {}, number of retries: {}", t.getMessage(), i);
                        }
                        if (t instanceof FoundationDBTxException) {
                            FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(storeManager.metricGroup, callStartTime,
                                    FDB_TNX_METHOD_CALL.getRange, false);
                            FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(storeManager.metricGroup,
                                    FDB_TNX_METHOD_CALL.getRange, false);
                            throw t;
                        }
                    }
                } else {
                    break;
                }
            } catch (Exception e) {
                if (log.isErrorEnabled()) {
                    LogWithCallContext.logError(log, "getRange encountered exception: " + e.getMessage());
                }

                lastException = e;
                break;
            }
        }

        FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(storeManager.metricGroup, callStartTime,
                FDB_TNX_METHOD_CALL.getRange, false);
        FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(storeManager.metricGroup,
                FDB_TNX_METHOD_CALL.getRange, false);

        if (log.isErrorEnabled()) {
            String message = "getRange failed with final exception " + lastException;
            LogWithCallContext.logError(log, message);
        }

        throw constructFoundationDBTxException("Max transaction reset count exceeded in getRange()", lastFDBException,
                FoundationDBTxErrorCode.MAX_TRANSACTION_RESET);
    }

    public AsyncIterator<KeyValue> getRangeIter(final byte[] startKey, final byte[] endKey, final int limit,
                                                TxGetSliceProfiler profiler) {
        FDBException lastFDBException = null;
        for (int i = 0; i< MAX_RETRIES; i++) {
            final int startTxId = termId.get();
            try {
                if (storeManager.isTxProfileEnable()) {
                    profiler.start();
                }
                Range range = new Range(startKey, endKey);
                AsyncIterator<KeyValue> iter = tx.getRange(range, limit, false, StreamingMode.WANT_ALL).iterator();
                iter.onHasNext();   // Trigger background fetching
                if (log.isDebugEnabled() && !isJanusSysTx) {
                    log.debug("Tx {}: GetRangeIter from range range begin={}, end={}, limit={}", transactionId,
                            byteArrayToHexString(range.begin), byteArrayToHexString(range.end), limit);
                }
                return iter;
            } catch (Exception e) {
                log.error("getRangeIter encountered exception: {}, with number of retries: {}", e.getMessage(), i);

                lastFDBException = getFDBException(e);

                // By having the condition: i+1 != maxRuns, we do not need to waste the call to "restart" when we have
                // set the transaction to be "serializable" mode, as i is initialized with 0.
                if (i+1 != MAX_RETRIES) {
                    try {
                        restart(startTxId, lastFDBException);
                    } catch (Throwable t) {
                        if (log.isDebugEnabled()) {
                            log.error("restart getRangeIter() got exception: {}, number of retries: {}", t.getMessage(),
                                    i);
                        }
                        if (t instanceof FoundationDBTxException) throw t;
                    }
                }
            }
        }

        throw constructFoundationDBTxException("Max transaction reset count exceeded in getRangeIter()", lastFDBException,
                FoundationDBTxErrorCode.MAX_TRANSACTION_RESET);
    }

    public AsyncIterator<KeyValue> getRangeIter(FoundationDBRangeQuery query) {
        Throwable lastException = null;
        FDBException lastFDBException = null;
        for (int i = 0; i< MAX_RETRIES; i++) {
            final int startTxId = termId.get();
            try {
                AsyncIterator<KeyValue> iter = tx.getRange(query    .getStartKeySelector(), query.getEndKeySelector(), query.getLimit(), false,
                        StreamingMode.WANT_ALL).iterator();
                iter.onHasNext();   // Trigger background fetching
                if (log.isDebugEnabled() && !isJanusSysTx) {
                    KeySelector startKeySelector = query.getStartKeySelector();
                    KeySelector endKeySelector = query.getEndKeySelector();
                    log.debug("Tx {}: GetRangeIter from selector startKeySelector={} endKeySelector={} limit={}",
                            transactionId,
                            byteArrayToHexString(startKeySelector.getKey()),
                            byteArrayToHexString(endKeySelector.getKey()),
                            query.getLimit());
                }
                return iter;
            } catch (Exception e) {
                log.error("getRangeIter encountered exception: {}, with number of retries: {}", e.getMessage(), i);

                lastException = e;
                lastFDBException = getFDBException(e);

                // By having the condition: i+1 != maxRuns, we do not need to waste the call to "restart" when we have
                // set the transaction to be "serializable" mode, as i is initialized with 0.
                if (i+1 != MAX_RETRIES) {
                    try {
                        restart(startTxId, lastException);
                    } catch (Throwable t) {
                        if (log.isDebugEnabled()) {
                            log.error("restart getRangeIter() got exception: {}, number of retries: {}", t.getMessage(), i);
                        }
                        if (t instanceof FoundationDBTxException) throw t;
                    }
                }
            }
        }

        throw constructFoundationDBTxException("Max transaction reset count exceeded in getRangeIter()", lastFDBException,
                FoundationDBTxErrorCode.MAX_TRANSACTION_RESET);
    }

    public AsyncIterator<KeyValue> resumeRangeIter(FoundationDBRangeQuery query) {
        // Avoid using KeySelector(byte[] key, boolean orEqual, int offset) directly as stated in KeySelector.java
        // that client code will not generally call this constructor.
        FDBException lastFDBException = null;
        for (int i = 0; i< MAX_RETRIES; i++) {
            final int startTxId = termId.get();
            try {
                AsyncIterator<KeyValue> iter = tx.getRange(query.getStartKeySelector(), query.getEndKeySelector(), query.getLimit(), false,
                        StreamingMode.WANT_ALL).iterator();
                iter.onHasNext();
                if (log.isDebugEnabled() && !isJanusSysTx) {
                    log.debug("Tx {}: ResumeRangeIter from range range begin={}, end={}, limit={}", transactionId,
                            byteArrayToHexString(query.getStartKeySelector().getKey()), byteArrayToHexString(query.getEndKeySelector().getKey()), query.getLimit());
                }
                return iter;
            } catch (Exception e) {
                log.error("resumeRangeIter encountered exception: {}, with number of retries: {}", e.getMessage(), i);

                lastFDBException = getFDBException(e);

                // By having the condition: i+1 != maxRuns, we do not need to waste the call to "restart" when we have
                // set the transaction to be "serializable" mode, as i is initialized with 0.
                if (i+1 != MAX_RETRIES) {
                    try {
                        restart(startTxId, lastFDBException);
                    } catch (Throwable t) {
                        if (log.isDebugEnabled()) {
                            log.error("restart resumeRangeIter() got exception: {}, number of retries: {}",
                                    t.getMessage(), i);
                        }
                        if (t instanceof FoundationDBTxException) throw t;
                    }
                }
            }
        }

        throw constructFoundationDBTxException("Max transaction reset count exceeded in resumeRangeIter()", lastFDBException,
                FoundationDBTxErrorCode.MAX_TRANSACTION_RESET);
    }

    public  Map<KVQuery, List<KeyValue>> getMultiRange(final Collection<FoundationDBRangeQuery> queries) {
        if (log.isDebugEnabled()) {
            String message = "multi-range query, total number of queries to be put inside: " + queries.size()
                    + " thread id: " + Thread.currentThread().getId()
                    + " transaction id: " + this.transactionId;
            LogWithCallContext.logDebug(log, message);
        }

        long callStartTime = System.nanoTime();

        Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();
        final List<FoundationDBRangeQuery> retries = new CopyOnWriteArrayList<>(queries);

        int counter = 0;
        for (int i = 0; i < MAX_RETRIES; i++) {
            counter++;

            if (retries.size() > 0) {
                List<FoundationDBRangeQuery> immutableRetries = new ArrayList<> (retries);

                final List<CompletableFuture<List<KeyValue>>> futures = new LinkedList<>();
                final int startTxId = termId.get();

                //Note: we introduce the immutable list for iteration purpose, rather than having the dynamic list
                //retries to be the iterator.
                for (FoundationDBRangeQuery q : immutableRetries) {
                    final KVQuery query = q.asKVQuery();

                    CompletableFuture<List<KeyValue>> f =
                            tx.getRange(q.getStartKeySelector(), q.getEndKeySelector(), query.getLimit()).asList()
                            .whenComplete((res, th) -> {
                                if (th == null) {
                                    if (log.isDebugEnabled()) {
                                        String message = "(before) get range succeeds with current size of retries: " + retries.size()
                                                + " thread id: " + Thread.currentThread().getId()
                                                + " transaction id: " + this.transactionId;
                                        LogWithCallContext.logDebug(log, message);
                                    }

                                    //Note: retries's object type is: Object[], not KVQuery.
                                    retries.remove(q);

                                    if (log.isDebugEnabled()) {
                                        String message = "(after) get range succeeds with current size of retries: " + retries.size()
                                                + " thread id: " + Thread.currentThread().getId()
                                                + " transaction id: " + this.transactionId;
                                        LogWithCallContext.logDebug(log, message);
                                    }

                                    if (res == null) {
                                        res = Collections.emptyList();
                                    }
                                    resultMap.put(query, res);

                                } else {
                                    Throwable t = th.getCause();
                                    if (t != null && t instanceof FDBException) {
                                        FDBException fe = (FDBException) t;
                                        if (log.isDebugEnabled()) {
                                            String message = String.format(
                                                    "Catch FDBException code=%s, isRetryable=%s, isMaybeCommitted=%s, "
                                                     + "isRetryableNotCommitted=%s, isSuccess=%s",
                                                    fe.getCode(), fe.isRetryable(),
                                                    fe.isMaybeCommitted(), fe.isRetryableNotCommitted(), fe.isSuccess());
                                            LogWithCallContext.logDebug(log, message);
                                        }
                                    }

                                    // Note: the restart here will bring the code into deadlock, as restart() is a
                                    // synchronized method and the thread to invoke this method is from a worker thread
                                    // that serves the completable future call, which is different from the thread that
                                    // invokes the getMultiRange call (this method) and getMultiRange is also a synchronized
                                    // call.
                                    //if (startTxId == txCtr.get())
                                    //    this.restart();
                                    resultMap.put(query, Collections.emptyList());

                                    if (log.isDebugEnabled()) {
                                        String message = "encounter exception with: " + th.getCause().getMessage();
                                        LogWithCallContext.logDebug(log, message);
                                    }
                                }
                            });

                    futures.add(f);
                }

                CompletableFuture<Void> allFuturesDone = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                //when some of the Future encounters exception, map will ignore it. we need count as the action!
                //allFuturesDone.thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
                try {
                    allFuturesDone.join();
                } catch (Exception ex) {
                    if (log.isErrorEnabled()) {
                        String message = "multi-range query encounters transient exception in some futures:" + ex.getCause().getMessage();
                        LogWithCallContext.logError(log, message);
                    }
                }

                if (log.isDebugEnabled()) {
                    String message = "get range succeeds with current size of retries: " + retries.size()
                            + " thread id: " + Thread.currentThread().getId()
                            + " transaction id: " + this.transactionId;
                    LogWithCallContext.logDebug(log, message);
                }

                if (retries.size() > 0) {
                    if (log.isDebugEnabled()) {
                        String message = "in multi-range query, retries size: " + retries.size()
                                + " thread id: " + Thread.currentThread().getId()
                                + " transaction id: " + this.transactionId
                                + " start tx id: " + startTxId
                                + " txCtr id: " + termId.get();
                        LogWithCallContext.logDebug(log, message);
                    }

                    if (i+1 != MAX_RETRIES) {
                        try {
                            this.restart(startTxId, null);
                        } catch (Throwable t) {
                            if (log.isDebugEnabled()) {
                                log.error("restart getMultiRange() got exception: {}, number of retries: {}",
                                        t.getMessage(), i);
                            }
                            if (t instanceof FoundationDBTxException) {
                                FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                                        storeManager.metricGroup,
                                        callStartTime, FDB_TNX_METHOD_CALL.getMultiRange, false);
                                FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(
                                        storeManager.metricGroup,
                                        FDB_TNX_METHOD_CALL.getMultiRange, false);
                                throw t;
                            }
                        }
                    }
                }
            } else {
                if (log.isDebugEnabled()) {
                    String message = "finish multi-range query's all of future-based query invocation with size: " + queries.size()
                            + " thread id: " + Thread.currentThread().getId()
                            + " transaction id: " + this.transactionId
                            + " actual number of retries is: " + (counter - 1);
                    LogWithCallContext.logDebug(log, message);
                }
                break;
            }

        }

        // Some queries are still getting errors, so report transaction failure
        if (retries.size() > 0) {
            FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                    storeManager.metricGroup,
                    callStartTime, FDB_TNX_METHOD_CALL.getMultiRange, false);
            FoundationDBMetricsUtil.FdbMethodScope.incMethodCallFailureCounts(
                    storeManager.metricGroup,
                    FDB_TNX_METHOD_CALL.getMultiRange, false);

            if (log.isErrorEnabled()) {
                String message = "after max number of retries: " + MAX_RETRIES
                        + " some range queries still fail and forced with empty returns"
                        + " thread id: " + Thread.currentThread().getId()
                        + " transaction id: " + this.transactionId;
                LogWithCallContext.logError(log, message);
            }

            throw FoundationDBTxException.withErrorCode("Encounter exception when invoking getRange calls in getMultiRange()",
                    GET_MULTI_RANGE_ERROR);
        }

        //the normal path
        FoundationDBMetricsUtil.FdbMethodScope.recordMethodLatency(
                storeManager.metricGroup,
                callStartTime, FDB_TNX_METHOD_CALL.getMultiRange, true);
        FoundationDBMetricsUtil.FdbMethodScope.incMethodCallCounts(
                storeManager.metricGroup, FDB_TNX_METHOD_CALL.getMultiRange);

        if (log.isDebugEnabled()) {
            long callEndTime = System.nanoTime();
            long totalTime = callEndTime - callStartTime;

            int cnt = 0;
            for (List<KeyValue> vals: resultMap.values()) {
                cnt += vals.size();
            }
            String message = "multi-range takes" +
                    totalTime / FoundationDBMetricsDefs.TO_MILLISECONDS_CONVERSION + "(ms)"
                    + " # queries: " + queries.size()
                    + " result size: " + cnt
                    + " thread id: " + Thread.currentThread().getId()
                    + " transaction id: " + this.transactionId;
            LogWithCallContext.logDebug(log, message);

        }

        return resultMap;
    }

    private void checkMutateAllowed() {
        if (isReadOnly()) {
            String msg = String.format("Transaction mode is %s but got a set() operation", isolationLevel);
            throw FoundationDBTxException.withErrorCode(msg, FoundationDBTxErrorCode.OPERATION_CONFLICT_WITH_ISOLATION_LEVEL);
        }

        if (isolationLevel == IsolationLevel.READ_COMMITTED_NO_WRITE && termId.get() > 0) {
            String msg = String.format("Transaction mode is %s but transaction was restarted and is trying a " +
                    "mutate operation", isolationLevel);
            throw FoundationDBTxException.withErrorCode(msg, FoundationDBTxErrorCode.OPERATION_CONFLICT_WITH_ISOLATION_LEVEL);
        }
    }

    protected boolean isErrorCodeRetriable(int errCode) {
        return (
                errCode == TRANSACTION_TOO_OLD_CODE ||
                (!isRolledBack && errCode == TRANSACTION_WAS_CANCELED)
        );
    }

    public void set(final byte[] key, final byte[] value) {
        checkMutateAllowed();

        for (int i = 0; i < MAX_RETRIES; ++i) {
            int startTermId = termId.get();
            try {
                tx.set(key, value);
                inserts.add(new Insert(key, value));
                break;
            } catch (IllegalStateException e) {
                log.error("in set() got IllegalStateException", e);

                try {
                    restart(startTermId, e);
                } catch (FoundationDBTxException ex) {
                    log.error("in set() trying to restart - Got a FoundationDBTxException, will not retry", ex);
                    throw ex;
                } catch (Throwable ex) {
                    log.error("in set() trying to restart - Got other exceptions, will retry", ex);
                    // Move on
                }
            } catch (FDBException e) {
                log.error("in set() got FDBException", e);

                if (isErrorCodeRetriable(e.getCode())) {
                    if (i+1 != MAX_RETRIES) {
                        try {
                            restart(startTermId, e);
                        } catch (FoundationDBTxException ex) {
                            log.error("in set() trying to restart - Got a FoundationDBTxException, will not retry", ex);
                            throw ex;
                        } catch (Throwable ex) {
                            log.error("in set() trying to restart - Got other exceptions, will retry", ex);
                            // Move on
                        }
                    } else {
                        throw FoundationDBTxException.withLastFDBException("Max transaction reset count exceeded in set()", e);
                    }
                } else {
                    String err = String.format("in set() - FDBException code %d is not restartable",
                            e.getCode());
                    log.error(err, e);
                    throw e;
                }
            } catch (Throwable e) {
                String err = String.format ("in set() - Throwable is not restartable");
                log.error(err, e);
                throw e;
            }
        }
    }

    public void clear(final byte[] key) {
        checkMutateAllowed();

        for (int i = 0; i < MAX_RETRIES; ++i) {
            int startTermId = termId.get();
            try {
                tx.clear(key);
                deletions.add(key);
                break;
            } catch (IllegalStateException e) {
                log.error("in clear() got IllegalStateException", e);

                try {
                    restart(startTermId, e);
                } catch (FoundationDBTxException ex) {
                    log.error("in clear() trying to restart - Got a FoundationDBTxException, will not retry", ex);
                    throw ex;
                } catch (Throwable ex) {
                    log.error("in clear() trying to restart - Got other exceptions, will retry", ex);
                    // Move on
                }
            } catch (FDBException e) {
                log.error("in clear() got FDBException", e);

                if (isErrorCodeRetriable(e.getCode())) {
                    if (i+1 != MAX_RETRIES) {
                        try {
                            restart(startTermId, e);
                        } catch (FoundationDBTxException ex) {
                            log.error("in clear() trying to restart - Got a FoundationDBTxException, will not retry", ex);
                            throw ex;
                        } catch (Throwable ex) {
                            log.error("in clear() trying to restart - Got other exceptions, will retry", ex);
                            // Move on
                        }
                    } else {
                        throw FoundationDBTxException.withLastFDBException("Max transaction reset count exceeded in clear()", e);
                    }
                } else {
                    String err = String.format("in clear() - FDBException code %d is not restartable",
                            e.getCode());
                    log.error(err, e);
                    throw e;
                }
            } catch (Throwable e) {
                String err = String.format ("in clear() - Throwable is not restartable");
                log.error(err, e);
                throw e;
            }
        }
    }

    public void fetched(int numEntries) {
        this.totalFetched.addAndGet(numEntries);
        this.totalIters.incrementAndGet();
    }

    public long getTotalFetched() {
        return totalFetched.get();
    }

    public int getTotalIters() {
        return totalIters.get();
    }

    private class Insert {
        private byte[] key;
        private byte[] value;

        public Insert(final byte[] key, final byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() { return this.key; }

        public byte[] getValue() { return this.value; }
    }

    private boolean containsWrite() {
        return (inserts.size() > 0 || deletions.size() > 0);
    }

    private String getOpType() {
        return containsWrite() ? "write": "read";
    }

    private void checkTransactionRestartable(Throwable lastThrowable) {
        String msg = "Unexpected error";
        int errCode = FoundationDBTxErrorCode.UNEXPECTED_ERROR;
        boolean restartable = true;

        if (isolationLevel == IsolationLevel.SERIALIZABLE || isolationLevel == IsolationLevel.READ_SNAPSHOT) {
            msg = "Transaction is not restartable due to transaction mode " + isolationLevel;
            errCode = FoundationDBTxErrorCode.TRANSACTION_NOT_RESTARTABLE;
            restartable = false;
        }

        if ((isolationLevel == IsolationLevel.READ_COMMITTED_NO_WRITE ||
                isolationLevel == IsolationLevel.READ_COMMITTED) && containsWrite()) {
            msg = "Transaction is not restartable because it contains writes and transaction mode " + isolationLevel;
            errCode = FoundationDBTxErrorCode.OPERATION_CONFLICT_WITH_ISOLATION_LEVEL;
            restartable = false;
        }

        long currentTime = System.currentTimeMillis();
        long tDiffInitialTx = currentTime - initialTxCrtTime;
        if (tDiffInitialTx > maxTraversalTimeoutMs) {
            log.error("Time difference between now and initial tx creation time: {} ms exceeded threshold: {} ms",
                    tDiffInitialTx, maxTraversalTimeoutMs);
            msg = "Time limit exceeded at tx restart: timeLimitExceeded (ms)=" + tDiffInitialTx + ", txCounter=" + termId.get();
            errCode = FoundationDBTxErrorCode.MAX_TRANSACTION_TIME_EXCEEDED;
            restartable = false;
        }

        if (!restartable) {
            FDBException fdbException = lastThrowable != null && lastThrowable instanceof FDBException ?
                    (FDBException) lastThrowable : null;
            throw constructFoundationDBTxException(msg, fdbException, errCode);
        }
    }

    public TxGetSliceProfiler addGetSliceProfiler() {
        if (!storeManager.isTxProfileEnable()) return null;

        TxGetSliceProfiler profiler = new TxGetSliceProfiler(wave.incrementAndGet());
        this.profilers.add(profiler);
        return profiler;
    }

    public TxGetSlicesProfiler addGetSlicesProfiler(int size) {
        if (!storeManager.isTxProfileEnable()) return null;

        TxGetSlicesProfiler profiler = new TxGetSlicesProfiler(wave.incrementAndGet(), size);
        this.profilers.add(profiler);
        return profiler;
    }

    public static class TxGetSliceProfiler {
        protected final int waveId;
        protected long elapsedTime;
        protected int resultSize;

        public TxGetSliceProfiler(int waveId) {
            this.waveId = waveId;
        }

        protected void start() {
            this.elapsedTime = System.currentTimeMillis();
        }

        protected void end() {
            this.elapsedTime = System.currentTimeMillis() - elapsedTime;
        }

        public void setResultSize(int resultSize) {
            this.resultSize = resultSize;
        }

        @Override
        public String toString() {
            return String.format("{\"op\":\"getSlice\",\"waveId\":%d,\"duration\":%d,\"result_size\":%d}", waveId, elapsedTime, resultSize);
        }
    }

    public static class TxGetSlicesProfiler extends TxGetSliceProfiler {
        final List<TxGetSliceProfiler> getSliceProfilers;

        public TxGetSlicesProfiler(int waveId, int size) {
            super(waveId);
            this.getSliceProfilers = new ArrayList<>(size);
        }

        public final TxGetSliceProfiler addProfile() {
            TxGetSliceProfiler profiler = new TxGetSliceProfiler(waveId);
            this.getSliceProfilers.add(profiler);
            return profiler;
        }

        public String toString() {
            int maxResultSize = 0, minResultSize = Integer.MAX_VALUE, sumResultSize = 0;
            long maxDuration = 0, minDuration = Long.MAX_VALUE;
            double avgDuration = 0;
            StringBuilder sb = new StringBuilder("[");
            for (TxGetSliceProfiler profiler: getSliceProfilers) {
                maxResultSize = Integer.max(maxResultSize, profiler.resultSize);
                minResultSize = Integer.min(minResultSize, profiler.resultSize);
                sumResultSize += profiler.resultSize;

                maxDuration = Long.max(maxDuration, profiler.elapsedTime);
                minDuration = Long.min(minDuration, profiler.elapsedTime);
                avgDuration += profiler.elapsedTime;

                sb.append(String.format("{\"resultSize\": %d, \"duration\": %d},", profiler.resultSize, profiler.elapsedTime));
            }

            if (getSliceProfilers.size() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append("]");

            if (getSliceProfilers.size() > 0) {
                avgDuration = avgDuration / getSliceProfilers.size();
            }

            return String.format("{\"op\":\"getSlices\",\"waveId\":%d,\"duration\":%d,\"iterCnt\":%d,\"iterDuration\":" +
                            "{\"min\":%d,\"max\":%d,\"avg\":%.2f},\"resultSize\":{\"min\":%d,\"max\":%d,\"sum\":%d},\"details\":%s}",
                    waveId, elapsedTime, getSliceProfilers.size(), minDuration, maxDuration, avgDuration,
                    minResultSize, maxResultSize, sumResultSize, sb.toString());
        }
    }

    private String byteArrayToHexString(byte[] byteArray) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : byteArray) {
            hexString.append(String.format("\\x%02x", b));
        }
        return hexString.toString();
    }

    public String getTransactionId() { return transactionId; }
}
