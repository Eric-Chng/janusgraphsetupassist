package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class FoundationDBRecordAsyncIteratorWithTracker extends FoundationDBRecordAsyncIterator {
    private static final Logger log = LoggerFactory.getLogger(FoundationDBRecordAsyncIteratorWithTracker.class);

    private final RecordIteratorTracker iteratorTracker;

    public FoundationDBRecordAsyncIteratorWithTracker(Subspace ds, FoundationDBTx tx,
                                                      FoundationDBRangeQuery rangeQuery,
                                                      final AsyncIterator<KeyValue> result, KeySelector selector,
                                                      int id, RecordIteratorTracker recordIteratorTracker,
                                                      FoundationDBTx.TxGetSliceProfiler subProfiler) {
        super(ds, tx, rangeQuery, result, selector, subProfiler);
        this.iteratorTracker = recordIteratorTracker;
        this.iteratorId = id;
        this.resumeFunc = () -> {
            // Resume all active iterators
            iteratorTracker.resumeActiveIterators();
            return null;
        };
    }

    @Override
    protected void fetchNextNoChecking() {
        iteratorTracker.updateActiveIterators();
        super.fetchNextNoChecking();
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = super.hasNext();
        if (!hasNext) {
            // Remove itself from iterator tracker, and to start new one
            iteratorTracker.updateActiveIterators(this);
            if (log.isDebugEnabled()) {
                log.debug("Tx {}: Iter id={} has result_size={}, result_byte_size={}", tx.getTransactionId(), iteratorId, resultSize, resultByteSize);
            }
        }
        return hasNext;
    }

    public static class RecordIteratorTracker {
        private int batchSize;
        private FoundationDBTx tx;

        // Keep track of the last iterator made active
        int lastIterMadeActivePtr = 0;

        // Keep references of the constructed iterators
        private List<FoundationDBRecordAsyncIteratorWithTracker> iterators;

        // Keep track of iterators that are being active
        private List<FoundationDBRecordAsyncIteratorWithTracker> activeIterators;

        private FoundationDBTx.TxGetSlicesProfiler profiler;

        public RecordIteratorTracker(FoundationDBTx tx, int totalQueries, int batchSize,
                                     FoundationDBTx.TxGetSlicesProfiler profiler) {
            this.tx = tx;
            this.batchSize = batchSize;
            this.profiler = profiler;

            iterators = new ArrayList<>(totalQueries);
            activeIterators = new LinkedList<>();

            if (this.profiler != null) {
                this.profiler.start();
            }
        }

        public FoundationDBRecordAsyncIteratorWithTracker addIterator(Subspace ds, FoundationDBRangeQuery rangeQuery,
                                                                      KeySelector selector, int id,
                                                                      FoundationDBTx.TxGetSlicesProfiler profiler) {
            FoundationDBTx.TxGetSliceProfiler subProfiler = null;
            if (profiler != null) {
                 subProfiler = profiler.addProfile();
            }
            FoundationDBRecordAsyncIteratorWithTracker iterator =
                    new FoundationDBRecordAsyncIteratorWithTracker(ds, tx, rangeQuery, null, selector, id, this, subProfiler);
            iterators.add(iterator);
            return iterator;
        }

        public synchronized void updateActiveIterators() {
            updateActiveIterators(null);
        }

        public synchronized void resumeActiveIterators() {
            for (int i = 0; i < activeIterators.size(); ++i) {
                activeIterators.get(i).resumeIterator();
            }
        }

        public synchronized void updateActiveIterators(FoundationDBRecordAsyncIteratorWithTracker toRemove) {
            // Remove the already-done iterator from the list of active iterators
            if (toRemove != null) {
                boolean removed = activeIterators.remove(toRemove);
                if (!removed) {
                    log.warn("Could not remove iterator id={} from active iterators. This should never happen",
                            toRemove.iteratorId);
                }
            }

            // Fill active iterators
            while (activeIterators.size() < batchSize && lastIterMadeActivePtr < iterators.size()) {
                FoundationDBRecordAsyncIteratorWithTracker iter = iterators.get(lastIterMadeActivePtr++);
                FoundationDBRangeQuery rangeQuery = iter.rangeQuery;
                if (iter.profiler != null) {
                    iter.profiler.start();
                }
                AsyncIterator<KeyValue> result = iter.tx.getRangeIter(rangeQuery);
                iter.entries = result;
                activeIterators.add(iter);

                if (log.isDebugEnabled()) {
                    log.debug("Make iter id {} active", iter.iteratorId);
                }
            }

            if (activeIterators.size() == 0 && lastIterMadeActivePtr == iterators.size()) {
                if (profiler != null) {
                    profiler.end();
                }

                if (log.isDebugEnabled()) {
                    log.debug("All iterators are iterated.");
                }
            }
        }
    }
}
