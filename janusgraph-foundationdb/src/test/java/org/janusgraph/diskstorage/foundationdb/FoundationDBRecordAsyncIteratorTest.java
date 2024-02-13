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

import com.google.common.collect.Lists;
import org.janusgraph.FoundationDBContainer;
import org.janusgraph.diskstorage.AbstractKCVSTest;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreUtil;
import org.janusgraph.diskstorage.KeyValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class FoundationDBRecordAsyncIteratorTest extends AbstractKCVSTest {
    private static final Logger log = LoggerFactory.getLogger(FoundationDBRecordAsyncIteratorTest.class);

    private final int numKeys = 10000;
    private final int maxItemsPerKey = 1000;
    private final int TASK_COUNT = 4;
    private final int MAX_THREAD_POOL = 8;
    private final int maxTraversalTimeoutMs = 2_400_000;


    protected OrderedKeyValueStoreManager manager;
    protected StoreTransaction tx;
    protected OrderedKeyValueStore store;

    private ExecutorService executor;

    @Container
    public static FoundationDBContainer container = new FoundationDBContainer();

    public OrderedKeyValueStoreManager openStorageManager() throws BackendException {
        return new FoundationDBStoreManager(container.getFoundationDBConfiguration(maxTraversalTimeoutMs));
    }

    @BeforeEach
    public void setUp() throws Exception {
        StoreManager m = openStorageManager();
        m.clearStorage();
        m.close();
        open();

        executor = Executors.newFixedThreadPool(MAX_THREAD_POOL);
    }

    public void open() throws BackendException {
        manager = openStorageManager();
        String storeName = "testStore1";
        store = manager.openDatabase(storeName);
        tx = manager.beginTransaction(getTxConfig());
    }

    public StoreFeatures getStoreFeatures(){
        return manager.getFeatures();
    }

    @AfterEach
    public void tearDown() throws Exception {
        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            log.error("Abnormal executor shutdown");
            Thread.dumpStack();
        } else {
            log.debug("Test executor completed normal shutdown");
        }

        close();
    }

    public void close() throws BackendException {
        if (tx != null) tx.commit();
        store.close();
        manager.close();
    }

    public void clopen() throws BackendException {
        close();
        open();
    }


    public String[] generateValues() {
        return KeyValueStoreUtil.generateData(numKeys);
    }


    public void loadValues(String[] values) throws BackendException {
        for (int i = 0; i < numKeys; i++) {
            store.insert(KeyValueStoreUtil.getBuffer(i), KeyValueStoreUtil.getBuffer(values[i]), tx, null);
            if ((i > 0) && ( i%10000 == 0)){
                clopen();
            }
        }

        clopen();
    }

    public Set<Integer> deleteValues(int start, int every) throws BackendException {
        final Set<Integer> removed = new HashSet<>();
        for (int i = start; i < numKeys; i = i + every) {
            removed.add(i);
            store.delete(KeyValueStoreUtil.getBuffer(i), tx);
        }
        return removed;
    }

    public void checkValueExistence(String[] values) throws BackendException {
        checkValueExistence(values, new HashSet<>());
    }

    /**
     * Check all of the generated values can be retrieved based on the sequence of the keys
     */
    public void checkValueExistence(String[] values, Set<Integer> removed) throws BackendException {
        for (int i = 0; i < numKeys; i++) {
            boolean result = store.containsKey(KeyValueStoreUtil.getBuffer(i), tx);
            if (removed.contains(i)) {
                assertFalse(result);
            } else {
                assertTrue(result);
            }
        }
    }

    public void checkValues(String[] values) throws BackendException {
        checkValuesWithGets(values, tx, manager, store, numKeys);
        checkValuesWithSingleSlices(values, tx, manager, store, numKeys);
        checkValuesWithMultipleSlices(values, tx, manager, store, numKeys, maxItemsPerKey);
    }

    public static void delayTxWithThreadSleep (long delayInMilliseconds) {
        try {
            Thread.sleep(delayInMilliseconds);
        }
        catch (InterruptedException ex) {
            // Ignored
        }
    }

    public static void checkValuesWithGets(String[] values,
                                   StoreTransaction tx,
                                   OrderedKeyValueStoreManager manager,
                                   OrderedKeyValueStore store, int pNumKeys) throws BackendException {
        // Step 1. Check the sequence of strings stored earlier, one by one.
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < pNumKeys; i++) {
            StaticBuffer result = store.get(KeyValueStoreUtil.getBuffer(i), tx);
            assertEquals(values[i], KeyValueStoreUtil.getString(result));

            if ((i >0) && (i%1000 == 0)) {
                log.info("current key index is: {}, to purposely take 1 additional second delay in get-key reading", i);
                delayTxWithThreadSleep(1000);
            }
        }
        
        long endTime = System.currentTimeMillis();
        long durationForGets = endTime - startTime;
        double averagedGetTime = durationForGets/((double) pNumKeys);
        log.info("finish checking values with get with number of keys: {}, total duration for gets: {} ms and averaged get time: {} ms",
                pNumKeys, durationForGets, averagedGetTime);
    }

    public static void checkValuesWithSingleSlices(String[] values,
                                             StoreTransaction tx,
                                             OrderedKeyValueStoreManager manager,
                                             OrderedKeyValueStore store, int pNumKeys) throws BackendException {
        for (int i=0; i< pNumKeys; i++){
            int startIndex = i;
            int endIndex = i + 1;
            StaticBuffer startKey = KeyValueStoreUtil.getBuffer(startIndex);
            StaticBuffer endKey = KeyValueStoreUtil.getBuffer(endIndex);

            RecordIterator<KeyValueEntry> result = store.getSlice (new KVQuery(startKey, endKey, 2), tx);

            int count=0;
            while(result.hasNext()) {
                StaticBuffer value = result.next().getValue();
                int indexInValues = startIndex;
                assertEquals(values[indexInValues], KeyValueStoreUtil.getString(value));
                count++;
            }

            // one slice only have one count.
            int expectedCount = 1;
            assertEquals(count, expectedCount);

            // to simulate the upper janusgraph retrieval behavior
            // ToDo: the whole async iterator does not throw IOException, what are the other JanusGraph Storage Adaptors'
            // exception behavior to allow IOException to be thrown?
            try {
                result.close();
            }
            catch (IOException ex) {
                log.error("encountered failure when trying to close RecordIterator", ex);
            }

            if ((i >0) && (i%1000 == 0)) {
                log.info("current key index is: {}, to purposely take 1 additional second delay in single-slice reader", i);
                delayTxWithThreadSleep(1000);
            }
        }

    }


    // NOTE: we will have removal as a separate test cases later.
    public static void checkValuesWithMultipleSlices(String[] values,
                                   StoreTransaction tx,
                                   OrderedKeyValueStoreManager manager,
                                   OrderedKeyValueStore store, int pNumKeys, int pItemsPerKey ) throws BackendException {
        if (!manager.getFeatures().hasMultiQuery()) {
            log.warn("manager features does not support multi-query");
            return;
        }

        log.info("manager feature is with multi-query, to issue multi-query first");

        long startTime = System.currentTimeMillis();
        List<KVQuery>  queries = Lists.newArrayList();
        for (int i = 0; i < pNumKeys; i++) {
            int startIndex = i;
            int endIndex = i + pItemsPerKey;
            if (endIndex > pNumKeys) {
                endIndex = pNumKeys;
            }
            StaticBuffer startKey = KeyValueStoreUtil.getBuffer(startIndex);
            StaticBuffer endKey = KeyValueStoreUtil.getBuffer(endIndex);
            queries.add(new KVQuery(startKey, endKey, pItemsPerKey));
        }

        Map<KVQuery, RecordIterator<KeyValueEntry>> results = store.getSlices(queries, tx);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        log.info("issue multi-query takes duration: {} ms", duration);


        //Step 2. Check all at once (if supported)
        log.info("to check query results with multi-query. Purposely sleep for 10 seconds to have current transaction expired");
        delayTxWithThreadSleep(10000);

        for (int i = 0; i < pNumKeys; i++) {
            RecordIterator<KeyValueEntry> result = results.get(queries.get(i));
            assertNotNull(result);

            int startIndex = i;
            int endIndex = i + pItemsPerKey;
            if (endIndex > pNumKeys) {
                endIndex = pNumKeys;
            }

            StaticBuffer value;
            int count =0;
            while (result.hasNext()) {
                value = result.next().getValue();
                int indexInValues = startIndex + count;
                assertEquals(values[indexInValues], KeyValueStoreUtil.getString(value));
                count++;
            }

            int gap = endIndex - startIndex;
            assertEquals (gap, count);
            log.debug("key: {} with count: {} and gap: {}, invariant: count == gap", i, count, gap);

            // to simulate the upper janusgraph retrieval behavior
            // ToDo: the whole async iterator does not throw IOException, what are the other JanusGraph Storage Adaptors'
            // exception behavior to allow IOException to be thrown?
            try {
                result.close();
            }
            catch (IOException ex) {
                log.error("encountered failure when trying to close RecordIterator", ex);
            }

            if ((i >0) && (i%1000 == 0)) {
                log.info("current key index is: {}, to purposely take 1 additional second delay in multi-slice reading", i);
                delayTxWithThreadSleep(1000);
            }
        }

        log.info("completed checking with multi-query with number of keys: {}", pNumKeys);
    }

    // A simple test to make sure that the static-buffer based key and sequence number  conversion is correct.
    @Test
    public void testOutKeyAndSequenceNumberRelationship() {
        for (int i = 0; i < numKeys; i++) {
            int startIndex = i;
            int endIndex = i + maxItemsPerKey;
            StaticBuffer key = KeyValueStoreUtil.getBuffer(i);

            StaticBuffer nextKey = key;

            for (int j = 0; j< maxItemsPerKey; j++) {
               nextKey = BufferUtil.nextBiggerBuffer(nextKey);
            }

            // turn the next result into long
            long longValue = KeyColumnValueStoreUtil.bufferToLong(nextKey);
            long endIndexWithIdOffset = endIndex + KeyValueStoreUtil.idOffset;

            StaticBuffer endKey = KeyValueStoreUtil.getBuffer(endIndex);


            if (endIndexWithIdOffset != longValue ) {
                log.info("endIndexWithIdOffset: {} and longValue: {}", endIndexWithIdOffset, longValue);
            }

            assertEquals (endIndexWithIdOffset, longValue);
            //endKey should be identical to nextKey
            assertTrue (endKey.asByteBuffer().compareTo(nextKey.asByteBuffer()) == 0);

        }
    }

    // for each key, we have a sequence of results and check the sequence of the results are correct.
    @Test
    public void storeAndRetrieve() throws BackendException {
        String[] values = generateValues();
        log.info("Step 1: Loading values...");
        loadValues(values);

        log.info("Step 2: Checking values Existence...");
        checkValueExistence(values);

        log.info("Step 3: Checking values with get/single slice/multiple slices....");
        checkValues(values);
    }

    @Test
    public void concurrentCheckValuesOnKeyGetsWithMultipleTransactions() throws Exception {
        String[] values = generateValues();
        log.info("Step 1: Loading values...");
        loadValues(values);

        log.info("Step 2: concurrent running of multiple different transaction for key gets");

        long startTime = System.currentTimeMillis();

        CountDownLatch startLatch = new CountDownLatch(TASK_COUNT);
        CountDownLatch stopLatch = new CountDownLatch(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            // For each thread, create a new transaction. It will be closed in the reader thread.
            StoreTransaction localTx = manager.beginTransaction(getTxConfig());
            executor.execute(new KeyGetsReader(localTx, startLatch, stopLatch, values, manager, store, numKeys));
            startLatch.countDown();
        }
        stopLatch.await();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        log.info("Issue key gets takes duration: {} ms", duration);

        log.info("Step 3: done with multi-transaction multiple threads' key gets and validation with task count: {} and takes: {} ms", TASK_COUNT, duration);
    }

    @Test
    public void concurrentCheckValuesOnSingleSlicesWithMultipleTransactions() throws Exception {
        String[] values = generateValues();
        log.info("Step 1: Loading values...");
        loadValues(values);

        log.info("Step 2: concurrent running of multiple different transaction for single slices reading");

        long startTime = System.currentTimeMillis();

        CountDownLatch startLatch = new CountDownLatch(TASK_COUNT);
        CountDownLatch stopLatch = new CountDownLatch(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            // For each thread, create a new transaction. It will be closed in the reader thread.
            StoreTransaction localTx = manager.beginTransaction(getTxConfig());
            executor.execute(new SingleSlicesReader(localTx, startLatch, stopLatch, values, manager, store, numKeys));
            startLatch.countDown();
        }
        stopLatch.await();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        log.info("Issue single slices takes duration: {} ms", duration);

        log.info("Step 3: done with multi-transaction multiple threads' single slices reading and validation with task count: {} and takes: {} ms", TASK_COUNT, duration);
    }


    @Test
    public void concurrentCheckValuesOnMultiSlicesWithMultipleTransactions() throws Exception {
        String[] values = generateValues();
        log.info("Step 1: Loading values...");
        loadValues(values);

        log.info("Step 2: concurrent running of multiple different transaction for slices reading");

        long startTime = System.currentTimeMillis();

        CountDownLatch startLatch = new CountDownLatch(TASK_COUNT);
        CountDownLatch stopLatch = new CountDownLatch(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            // For each thread, create a new transaction. It will be closed in the reader thread.
            StoreTransaction localTx = manager.beginTransaction(getTxConfig());
            executor.execute(new MultiSlicesReader(localTx, startLatch, stopLatch, values, manager, store, numKeys, maxItemsPerKey));
            startLatch.countDown();
        }
        stopLatch.await();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        log.info("Issue multi-query takes duration: {} ms", duration);

        log.info("Step 3: done with multi-transaction multiple threads' slices reading and validation with task count: {} and takes: {} ms", TASK_COUNT, duration);
    }

    @Test
    public void concurrentCheckValuesOnKeyGetsWithSingleTransaction() throws Exception {
        String[] values = generateValues();
        log.info("Step 1: Loading values...");
        loadValues(values);

        log.info("Step 2: concurrent running of single transaction for get-key reading");

        long startTime = System.currentTimeMillis();

        CountDownLatch startLatch = new CountDownLatch(TASK_COUNT);
        CountDownLatch stopLatch = new CountDownLatch(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            // Re-use the single transaction that is still active from the loading phase.
            executor.execute(new KeyGetsReader(tx, startLatch, stopLatch, values, manager, store, numKeys));
            startLatch.countDown();
        }
        stopLatch.await();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        log.info("Issue get-key in single transaction takes duration: {} ms", duration);

        log.info("Step 3: done with single-transaction multiple threads' get-key reading and validation with task count: {} and takes: {} ms", TASK_COUNT, duration);
    }

    @Test
    public void concurrentCheckValuesOnSingleSlicesWithSingleTransaction() throws Exception {
        String[] values = generateValues();
        log.info("Step 1: Loading values...");
        loadValues(values);

        log.info("Step 2: concurrent running of single transaction for single-slice reading");

        long startTime = System.currentTimeMillis();

        CountDownLatch startLatch = new CountDownLatch(TASK_COUNT);
        CountDownLatch stopLatch = new CountDownLatch(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            // Re-use the single transaction that is still active from the loading phase.
            executor.execute(new SingleSlicesReader(tx, startLatch, stopLatch, values, manager, store, numKeys));
            startLatch.countDown();
        }
        stopLatch.await();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        log.info("Issue single-slice in single transaction takes duration: {} ms", duration);

        log.info("Step 3: done with single-transaction multiple threads' single-slice reading and validation with task count: {} and takes: {} ms", TASK_COUNT, duration);
    }

    @Test
    public void concurrentCheckValuesOnMultiSlicesWithSingleTransaction() throws Exception {
        String[] values = generateValues();
        log.info("Step 1: Loading values...");
        loadValues(values);

        log.info("Step 2: concurrent running of single transaction for multi-slice reading");

        long startTime = System.currentTimeMillis();

        CountDownLatch startLatch = new CountDownLatch(TASK_COUNT);
        CountDownLatch stopLatch = new CountDownLatch(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            // Re-use the single transaction that is still active from the loading phase.
            executor.execute(new MultiSlicesReader(tx, startLatch, stopLatch, values, manager, store, numKeys, maxItemsPerKey));
            startLatch.countDown();
        }
        stopLatch.await();

        tx.commit();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        log.info("Issue multi-query in single transaction takes duration: {} ms", duration);

        log.info("Step 3: done with single-transaction multiple threads' multi-slice reading and validation with task count: {} and takes: {} ms", TASK_COUNT, duration);
    }


    private static class KeyGetsReader extends BarrierRunnable {

        private final String[] valuesToCheck;
        private final StoreTransaction tx;
        private final OrderedKeyValueStoreManager manager;
        private final OrderedKeyValueStore store;
        private final int pNumKeys;

        public KeyGetsReader(StoreTransaction tx, CountDownLatch startLatch,
                            CountDownLatch stopLatch, String[] values,
                            OrderedKeyValueStoreManager manager,
                            OrderedKeyValueStore store, int pNumKeys) {
            super(tx, startLatch, stopLatch);
            this.valuesToCheck = values;
            this.tx = tx;
            this.manager = manager;
            this.store = store;
            this.pNumKeys = pNumKeys;
        }

        @Override
        protected void doRun() throws BackendException {
            checkValuesWithGets(valuesToCheck, tx, manager, store, pNumKeys);
        }
    }

    private static class SingleSlicesReader extends BarrierRunnable {

        private final String[] valuesToCheck;
        private final StoreTransaction tx;
        private final OrderedKeyValueStoreManager manager;
        private final OrderedKeyValueStore store;
        private final int pNumKeys;

        public SingleSlicesReader (StoreTransaction tx, CountDownLatch startLatch,
                                  CountDownLatch stopLatch, String[] values,
                                  OrderedKeyValueStoreManager manager,
                                  OrderedKeyValueStore store, int pNumKeys) {
            super(tx, startLatch, stopLatch);
            this.valuesToCheck = values;
            this.tx = tx;
            this.manager = manager;
            this.store = store;
            this.pNumKeys = pNumKeys;
        }

        @Override
        protected void doRun() throws BackendException {
            checkValuesWithSingleSlices(valuesToCheck, tx, manager, store, pNumKeys);
        }
    }

    private static class MultiSlicesReader extends BarrierRunnable {

        private final String[] valuesToCheck;
        private final StoreTransaction tx;
        private final OrderedKeyValueStoreManager manager;
        private final OrderedKeyValueStore store;
        private final int pNumKeys;
        private final int pItemsPerkey;

        public MultiSlicesReader (StoreTransaction tx, CountDownLatch startLatch,
                            CountDownLatch stopLatch, String[] values,
                            OrderedKeyValueStoreManager manager,
                            OrderedKeyValueStore store, int pNumKeys, int pItemsPerKey) {
            super(tx, startLatch, stopLatch);
            this.valuesToCheck = values;
            this.tx = tx;
            this.manager = manager;
            this.store = store;
            this.pNumKeys = pNumKeys;
            this.pItemsPerkey = pItemsPerKey;
        }

        @Override
        protected void doRun() throws BackendException {
            checkValuesWithMultipleSlices(valuesToCheck, tx, manager, store, pNumKeys, pItemsPerkey);
        }
    }

    private abstract static class BarrierRunnable implements Runnable {

        protected final StoreTransaction tx;
        protected final CountDownLatch startLatch;
        protected final CountDownLatch stopLatch;

        public BarrierRunnable(StoreTransaction tx, CountDownLatch startLatch, CountDownLatch stopLatch) {
            this.tx = tx;
            this.startLatch = startLatch;
            this.stopLatch = stopLatch;
        }

        protected abstract void doRun() throws BackendException;

        @Override
        public void run() {
            try {
                startLatch.await();
            } catch (Exception e) {
                throw new RuntimeException("Interrupted while waiting for peers to start");
            }

            try {
                doRun();
            } catch (Exception e) {
                log.error("Thread {} stopped due to exception", Thread.currentThread().getId(), e);
                throw new RuntimeException(e);
            }

            log.info("Thread {} completed successfully", Thread.currentThread().getId());
            stopLatch.countDown();
        }
    }

}
