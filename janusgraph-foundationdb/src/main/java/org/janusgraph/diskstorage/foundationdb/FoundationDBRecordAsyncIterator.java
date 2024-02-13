// Copyright 2020 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class FoundationDBRecordAsyncIterator extends FoundationDBRecordIterator {
    private static final Logger log = LoggerFactory.getLogger(FoundationDBRecordAsyncIterator.class);

    protected final FoundationDBTx tx;
    protected final FoundationDBRangeQuery rangeQuery;
    protected int iteratorId;
    protected Callable<Void> resumeFunc;

    public FoundationDBRecordAsyncIterator(Subspace ds, FoundationDBTx tx, FoundationDBRangeQuery query,
                                           final AsyncIterator<KeyValue> result, KeySelector selector,
                                           FoundationDBTx.TxGetSliceProfiler profiler) {
        super(ds, result, selector, profiler);

        this.entries = result;
        this.tx = tx;
        this.rangeQuery = query;

        // Reserve it to use when tracking iterator in multi-query
        this.iteratorId = 0;
        this.resumeFunc = () -> {
            resumeIterator();
            return null;
        };
    }

    protected FDBException unwrapException(Throwable err) {
        Throwable curr = err;
        while (curr != null && !(curr instanceof FDBException))  {
            curr = curr.getCause();
        }
        if (curr != null) {
            return (FDBException) curr;
        } else {
            return null;
        }
    }

    /**
     * Perform fetching on the iterator without further failure (such as transaction too old checking), and thus it
     * is up to the caller to inspect the exception thrown and perform recovery.
     */
    protected void fetchNextNoChecking() {
        super.fetchNext();
    }

    protected void resumeIterator() {
        ((AsyncIterator<KeyValue>)entries).cancel();

        // If the transaction gets restarted, then the iterator will fail then it performs the actual fetching
        // at that time, the transaction object will be re-adjusted.
        try {
            if (log.isDebugEnabled()) {
                log.debug("Resume AsyncIterator, fetched = {}", fetched);
            }

            rangeQuery.update(lastKey, fetched);
            fetched = 0;
            lastKey = null;

            entries = tx.resumeRangeIter(rangeQuery);
        } catch (FoundationDBTxException te) {
            throw te;
        } catch (Exception ex) {
            throw new RuntimeException ("Encounter exception when to restart the async iterator", ex);
        }

        // Initiate record iterator again, but keep cursor "fetched" not changed.
        this.nextKeyValueEntry = null;
    }

    /**
     * To have better logging report:
     * (1) the last exception captured, in particular, if the exception is a FDBException such
     * as transaction-too-old
     * (2) the last cursor of the iterator, which allows us to figure out whether the query involved with
     * a large amount of data retrieval, such as > 10000 items due to super-node.
     */
    protected String getCurrentIteratorStatusReport(Exception lastException) {
        String report = "";
        if (lastException != null) {
            if (lastException instanceof FDBException) {
                FDBException te = (FDBException) lastException;
                report = String.format("last FDBException code: %d and message: %s, fetched-items: %d, fetched-iters:" +
                                " %d", te.getCode(), te.getMessage(), tx.getTotalFetched(), tx.getTotalIters());
            } else {
                report = String.format("last exception %s message: %s and fetched-items: %d, fetched-iters: %d",
                        lastException.getClass().getSimpleName(), lastException.getMessage(), tx.getTotalFetched(),
                        tx.getTotalIters());
            }
        }
        else{
           report = String.format("last exception is null and fetched-items: %d, fetched-iters: %d",
                   tx.getTotalFetched(), tx.getTotalIters());
        }

        return report;
    }

    protected void restartTransaction(int startTxId, Exception lastException) {
        try {
            // Restart the transaction
            tx.restart(startTxId, lastException);

            // The iterator resumes itself
            resumeFunc.call();
        } catch (FoundationDBTxException e) {
            String errMsg = "Encountered a FoundationDBTxException when restarting the iterator with "
                      + getCurrentIteratorStatusReport(lastException);
            log.error(errMsg, e);
            throw e;
        } catch (Exception e) {
            String errMsg = "Encountered exception when restarting the iterator with "
                    + getCurrentIteratorStatusReport(lastException);
            log.error(errMsg, e);
        }
    }

    @Override
    protected void fetchNext() {
        int txRestartCtr = 0;
        int restartMaxCount = FoundationDBTx.getMaxRetries();

        Exception lastException = null;
        while (txRestartCtr < restartMaxCount) {
            final int startTxId = tx.getTermId();
            try {
                fetchNextNoChecking();
                break;
            } catch (IllegalStateException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Got IllegalStateException (restartable)", e);
                }
                // This can happen when another thread started the new transaction, and thus old tx becomes null, while at the same time
                // the current thread is trying to access async iterator's getRange_internal method that is tied to the old tx.
                // As a result, "cannot access closed object" encountered.
                if (++txRestartCtr != restartMaxCount) {
                    lastException = e;
                    this.restartTransaction(startTxId, lastException);
                }
            } catch (FoundationDBTxException te) {
                log.error ("Got a FoundationDBTxException during fetch next", te);
                throw te;
            } catch (Throwable e) {
                FDBException fdbException = unwrapException(e);
                if (fdbException != null && tx.isErrorCodeRetriable(fdbException.getCode())) {
                    if (log.isDebugEnabled()) {
                        log.debug("Got FDBException (restartable)", e);
                    }
                    lastException = fdbException;
                    this.restartTransaction(startTxId, lastException);
                    // Do not increase retry count, the transaction keeps restart until the request timed out.
                } else {
                    String err = String.format("The throwable is not restartable at iterator id: [%d]", iteratorId);
                    log.error(err, e);
                    throw e;
                }
            }
        }

        if (txRestartCtr == restartMaxCount) {
            if (log.isDebugEnabled()) {
                log.debug("Last exception before giving up", lastException);
            }

            final String err;
            if (lastException != null) {
                err = String.format("Exceed max transaction restart count: [%d] at iterator id: [%d], last exception " +
                                "%s with message %s", restartMaxCount, iteratorId,
                        lastException.getClass().getSimpleName(), lastException.getMessage());
            } else {
                err = String.format("Exceed max transaction restart count: [%d] at iterator id: [%d], last exception " +
                                "is null", restartMaxCount, iteratorId);
            }
            throw new IllegalStateException(err);
        }
    }

    @Override
    public void close() {
        ((AsyncIterator<KeyValue>)entries).cancel();
        tx.fetched(fetched);
        super.close();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
