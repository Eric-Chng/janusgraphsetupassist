// Copyright 2018 Expero Inc.
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsDefs;
import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsUtil;
import org.janusgraph.diskstorage.foundationdb.utils.LogWithCallContext;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A FoundationDBKeyValueStore object does not manage transactions directly
 */
public class FoundationDBKeyValueStore implements OrderedKeyValueStore {
    private static final Logger log = LoggerFactory.getLogger(FoundationDBKeyValueStore.class);

    protected static final StaticBuffer.Factory<byte[]> ENTRY_FACTORY = (array, offset, limit) -> {
        final byte[] bArray = new byte[limit - offset];
        System.arraycopy(array, offset, bArray, 0, limit - offset);
        return bArray;
    };


    private final DirectorySubspace db;
    private final String name;
    private final FoundationDBStoreManager manager;
    private boolean isOpen;

    public FoundationDBKeyValueStore(String n, DirectorySubspace data, FoundationDBStoreManager m) {
        db = data;
        name = n;
        manager = m;
        isOpen = true;
    }

     @Override
    public String getName() {
        return name;
    }

    private static FoundationDBTx getTransaction(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        return ((FoundationDBTx) txh);//.getTransaction();
    }

    @Override
    public synchronized void close() throws BackendException {
        try {
            //if(isOpen) db.close();
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
        if (isOpen) manager.removeDatabase(this);
        isOpen = false;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        long callStartTime = System.nanoTime();

        FoundationDBTx tx = getTransaction(txh);
        try {
            byte[] databaseKey = db.pack(key.as(ENTRY_FACTORY));
            if (log.isDebugEnabled()) {
                LogWithCallContext.logDebug(log, String.format("db=%s, op=get, tx=%s", name, txh));
            }

            final byte[] entry = tx.get(databaseKey);

            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.get, true);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                    manager.metricGroup,
                    FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.get);

            if (entry != null) {
                return getBuffer(entry);
            } else {
                return null;
            }
        } catch (Exception e) {

            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.get, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.get, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, "op=get exception", e);
            }

            if (e instanceof BackendException) throw e;
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key,txh)!=null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        if (getTransaction(txh) == null) {
            if (log.isWarnEnabled()) {
                LogWithCallContext.logWarn(log, "Attempt to acquire lock with transactions disabled");
            }
        } //else we need no locking
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        if (manager.getMode() == FoundationDBStoreManager.NON_ASYNC) {
            return getSliceNonAsync(query, txh);
        } else {
            return getSliceAsync(query, txh);
        }
    }

    public RecordIterator<KeyValueEntry> getSliceNonAsync(KVQuery query, StoreTransaction txh) throws BackendException {
        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log, String.format("beginning db=%s, op=getSliceNonAsync, tx=%s", name, txh));
        }

        long callStartTime = System.nanoTime();

        final FoundationDBTx tx = getTransaction(txh);

        try {
            final List<KeyValue> result = tx.getRange(new FoundationDBRangeQuery(db, query));

            if (log.isDebugEnabled()) {
                LogWithCallContext.logDebug(log, String.format("db=%s, op=getSliceNonAsync, tx=%s, result-count=%d", name, txh, result.size()));
            }

            //normal path
            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSliceNonAsync, true);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                    manager.metricGroup,
                    FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSliceNonAsync);

            return new FoundationDBRecordIterator(db, result.iterator(), query.getKeySelector(), tx.addGetSliceProfiler());
        } catch (Exception e) {
            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSliceNonAsync, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSliceNonAsync, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, "op=getSliceNonAsync exception", e);
            }

            if (e instanceof BackendException) throw e;
            throw new PermanentBackendException(e);
        }
    }

    public RecordIterator<KeyValueEntry> getSliceAsync(KVQuery query, StoreTransaction txh) throws BackendException {
        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log, String.format("beginning db=%s, op=getSliceAsync, tx=%s", name, txh));
        }

        long callStartTime = System.nanoTime();

        final FoundationDBTx tx = getTransaction(txh);
        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final byte[] foundKey = db.pack(keyStart.as(ENTRY_FACTORY));
        final byte[] endKey = db.pack(keyEnd.as(ENTRY_FACTORY));

        try {
            final FoundationDBRangeQuery rangeQuery = new FoundationDBRangeQuery(db, query);
            FoundationDBTx.TxGetSliceProfiler profiler = tx.addGetSliceProfiler();
            final AsyncIterator<KeyValue> result = tx.getRangeIter(foundKey, endKey, query.getLimit(), profiler);

            // Though this is an async call, we still capture the latency overhead up to this point.
            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSliceAsync, true);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                    manager.metricGroup,
                    FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSliceAsync);

            return new FoundationDBRecordAsyncIterator(db, tx, rangeQuery, result, query.getKeySelector(), profiler);
        } catch (Exception e) {
            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSliceAsync, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSliceAsync, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, String.format("getSliceAsync db=%s, tx=%s exception", name, txh), e);
            }

            if (e instanceof BackendException) throw e;
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        if (manager.getMode() == FoundationDBStoreManager.NON_ASYNC) {
            return getSlicesNonAsync(queries, txh);
        } else {
            return getSlicesAsync(queries, txh);
        }
    }

    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlicesNonAsync(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log, String.format("beginning db=%s, op=getSlicesNonAsync, tx=%s", name, txh));
        }

        //record the total number of sizes
        int totalNumberOfQueries = queries.size();
        FoundationDBMetricsUtil.StoragePluginMethodScope.recordSizeOfMultiQueries(
                manager.metricGroup, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesNonAsync, totalNumberOfQueries);

        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log,
                    String.format("beginning db=%s, op=getSlicesNonAsync, tx=%s, in thread=%d", name, txh, Thread.currentThread().getId()));
        }

        long callStartTime = System.nanoTime();

        FoundationDBTx tx = getTransaction(txh);
        final Map<KVQuery, FoundationDBRangeQuery> fdbQueries = new HashMap<>();

        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log, "get slices invoking multi-range queries in thread = " + Thread.currentThread().getId());
        }

        try {
            for (final KVQuery q : queries) {
                fdbQueries.put(q, new FoundationDBRangeQuery(db, q));
            }

            final Map<KVQuery, List<KeyValue>> unfilteredResultMap = tx.getMultiRange(fdbQueries.values());

            final Map<KVQuery, RecordIterator<KeyValueEntry>> iteratorMap = new HashMap<>();
            for (Map.Entry<KVQuery, List<KeyValue>> kv : unfilteredResultMap.entrySet()) {
                iteratorMap.put(kv.getKey(),
                        new FoundationDBRecordIterator(db, kv.getValue().iterator(), kv.getKey().getKeySelector(), tx.addGetSliceProfiler()));
            }

            //normal flow
            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesNonAsync, true);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                    manager.metricGroup,
                    FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesNonAsync);

            return iteratorMap;
        } catch (Exception e) {

            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesNonAsync, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesNonAsync, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, "op=getSlicesNonAsync exception", e);
            }

            if (e instanceof BackendException) throw e;
            throw new PermanentBackendException(e);
        }
    }

    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlicesAsync(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log, String.format("beginning db=%s, op=getSlicesAsync, tx=%s", name, txh));
        }

        //record the total number of sizes
        int totalNumberOfQueries = queries.size();
        FoundationDBMetricsUtil.StoragePluginMethodScope.recordSizeOfMultiQueries(
                manager.metricGroup, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesAsync, totalNumberOfQueries);


        if (log.isDebugEnabled()) {
            LogWithCallContext.logDebug(log,
                    String.format("beginning db=%s, op=getSlicesAsync, tx=%s, in thread=%d", name, txh, Thread.currentThread().getId()));
        }

        long callStartTime = System.nanoTime();

        FoundationDBTx tx = getTransaction(txh);
        final Map<KVQuery, RecordIterator<KeyValueEntry>> resultMap = new HashMap<>();

        FoundationDBTx.TxGetSlicesProfiler profiler = tx.addGetSlicesProfiler(queries.size());
        final FoundationDBRecordAsyncIteratorWithTracker.RecordIteratorTracker tracker =
                new FoundationDBRecordAsyncIteratorWithTracker.RecordIteratorTracker(tx, queries.size(),
                        manager.getMaxActiveAsyncIterator(), profiler);
        try {
            for (int i = 0; i < queries.size(); ++i) {
                KVQuery query = queries.get(i);
                FoundationDBRangeQuery rangeQuery = new FoundationDBRangeQuery(db, query);
                FoundationDBRecordAsyncIteratorWithTracker iter = tracker.addIterator(db, rangeQuery,
                        query.getKeySelector(), i, profiler);
                resultMap.put(query, iter);
            }
        } catch (Exception e) {
            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesAsync, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesAsync, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, String.format("getSlicesAsync db=%s, tx=%s exception", name, txh), e);
            }
            throw new PermanentBackendException(e);
        }

        //normal flow
        FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                manager.metricGroup,
                callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesAsync, true);
        FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                manager.metricGroup, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.getSlicesAsync);

        return resultMap;
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, Integer ttl) throws BackendException {
        long callStartTime = System.nanoTime();

        FoundationDBTx tx = getTransaction(txh);
        try {
            if (log.isDebugEnabled()) {
                LogWithCallContext.logDebug(log,String.format("db=%s, op=insert, tx=%s", name, txh));
            }
            tx.set(db.pack(key.as(ENTRY_FACTORY)), value.as(ENTRY_FACTORY));
        } catch (Exception e) {

            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.insert, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.insert, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log, String.format("db=%s, op=insert, tx=%s throws exception", name, txh), e);
            }

            throw new PermanentBackendException(e);
        }

        FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                manager.metricGroup,
                callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.insert, true);
        FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                manager.metricGroup,
                FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.insert);
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        long callStartTime = System.nanoTime();

        FoundationDBTx tx = getTransaction(txh);
        try {
            if (log.isDebugEnabled()) {
                LogWithCallContext.logDebug(log, String.format("db=%s, op=delete, tx=%s", name, txh));
            }

            tx.clear(db.pack(key.as(ENTRY_FACTORY)));
        } catch (Exception e) {
            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    manager.metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.delete, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    manager.metricGroup,
                    FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.delete, e);

            if (log.isErrorEnabled()) {
                LogWithCallContext.logError(log,String.format("db=%s, op=delete, tx=%s throws exception", name, txh), e);
            }
            throw new PermanentBackendException(e);
        }

        FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                manager.metricGroup,
                callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.delete, true);
        FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                manager.metricGroup,
                FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.insert);
    }

    protected static StaticBuffer getBuffer(byte[] entry) {
        return new StaticArrayBuffer(entry);
    }
}
