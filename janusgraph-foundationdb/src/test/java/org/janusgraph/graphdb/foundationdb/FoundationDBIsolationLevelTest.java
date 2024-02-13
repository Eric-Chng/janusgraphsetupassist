package org.janusgraph.graphdb.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.ebay.nugraph.common.CallContext;
import com.ebay.nugraph.common.CallCtxThreadLocalHolder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.FoundationDBContainer;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.foundationdb.FoundationDBTxException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
public class FoundationDBIsolationLevelTest {
    private static final Logger log = LoggerFactory.getLogger(FoundationDBIsolationLevelTest.class);

    JanusGraph graph;
    GraphTraversalSource g;

    @Container
    public static FoundationDBContainer container = new FoundationDBContainer();

    // For storage isolation level
    private static final String USE_DEFAULT = "use_default";
    private static final String SERIALIZABLE = "serializable";
    private static final String READ_COMMITTED_OR_SERIALIZABLE = "read_committed_or_serializable";
    private static final String READ_COMMITTED_WITH_WRITE = "read_committed_with_write";

    // For request read mode
    private static final String READ_COMMITTED = "READ_COMMITTED";
    private static final String READ_SNAPSHOT = "READ_SNAPSHOT";
    private static final String IS_READ_ONLY = "IS_READ_ONLY";
    private static final String NOT_SPECIFIED = "NOT_SPECIFIED";


    private static boolean loadGraph = false;

    public static final int NUM_VERTICES = 150_000;
    private static final int NUM_VERTICES_TO_ADD = 1000;
    private static final int NUM_EDGES_TO_ADD = 2;
    private static final long MAX_FDB_TX_TIMEOUT = 5001;
    private static final int REPEATED = 1;

    static Stream<Arguments> testReadCommittedSource() {
        return Stream.of(
                arguments(USE_DEFAULT, READ_COMMITTED),
                arguments(SERIALIZABLE, READ_COMMITTED),
                arguments(READ_COMMITTED_OR_SERIALIZABLE, READ_COMMITTED),
                arguments(READ_COMMITTED_WITH_WRITE, READ_COMMITTED)
        );
    }

    static Stream<Arguments> testReadSnapshotSource() {
        return Stream.of(
                arguments(USE_DEFAULT, READ_SNAPSHOT),
                arguments(SERIALIZABLE, READ_SNAPSHOT),
                arguments(READ_COMMITTED_OR_SERIALIZABLE, READ_SNAPSHOT),
                arguments(READ_COMMITTED_WITH_WRITE, READ_SNAPSHOT),
                arguments(USE_DEFAULT, NOT_SPECIFIED),
                arguments(SERIALIZABLE, NOT_SPECIFIED)
        );
    }

    static Stream<Arguments> testReadAllCombined() {
        return Stream.of(
                arguments(USE_DEFAULT, READ_COMMITTED),
                arguments(SERIALIZABLE, READ_COMMITTED),
                arguments(READ_COMMITTED_OR_SERIALIZABLE, READ_COMMITTED),
                arguments(READ_COMMITTED_WITH_WRITE, READ_COMMITTED),
                arguments(USE_DEFAULT, READ_SNAPSHOT),
                arguments(SERIALIZABLE, READ_SNAPSHOT),
                arguments(READ_COMMITTED_OR_SERIALIZABLE, READ_SNAPSHOT),
                arguments(READ_COMMITTED_WITH_WRITE, READ_SNAPSHOT)
        );
    }

    private ModifiableConfiguration getFoundationDBConfiguration(String isolationLevel) {
        if (isolationLevel != null && !isolationLevel.isEmpty() && !isolationLevel.equals(USE_DEFAULT)) {
            return container.getFoundationDBConfiguration().set(ISOLATION_LEVEL, isolationLevel);
        } else {
            return container.getFoundationDBConfiguration().set(ISOLATION_LEVEL, SERIALIZABLE);
        }
    }

    private ModifiableConfiguration getFoundationDBConfiguration(String isolationLevel, long maxTraversalTimeout) {
        if (maxTraversalTimeout != 0) {
            return getFoundationDBConfiguration(isolationLevel).set(MAX_GRAPH_TRAVERSAL_TIMEOUT, maxTraversalTimeout);
        } else {
            return getFoundationDBConfiguration(isolationLevel);
        }
    }

    private ModifiableConfiguration getFoundationDBConfiguration() {
        return getFoundationDBConfiguration(SERIALIZABLE);
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetRequestContext();

        ModifiableConfiguration config = getFoundationDBConfiguration();

        try {
            if (loadGraph == false) {
                log.info("Load graph for the first time, cluster_file_path={}", CLUSTER_FILE_PATH);

                // First clean up the FDB keyspace
                String clusterFilePath = config.get(CLUSTER_FILE_PATH);
                FDB fdb = FDB.selectAPIVersion(config.get(VERSION));
                try (
                        Database db = fdb.open(clusterFilePath);
                        Transaction tx = db.createTransaction();) {
                    byte[] begin = new byte[] { (byte) 0x00 };
                    byte[] end = new byte[] { (byte) 0xff };
                    tx.clear(begin, end);
                    tx.commit().get();
                } catch (Exception e) {
                    log.error("Error when clean up FDB keyspace", e);
                }

                graph = JanusGraphFactory.open(config);
                g = graph.traversal();

                // Load the GraphOfTheGods sample to test
                int batchSize = 100;
                int curr = 0;
                GraphTraversal gt = null;
                Random rand = new Random();
                int loaded = 0;
                for (int i = 0; i < NUM_VERTICES; i++) {
                    if (gt == null) gt = g.addV("employee");
                    else gt.addV("employee");
                    gt.property("name", "John Doe " + i)
                            .property("age", rand.nextInt(200))
                            .property("position", "SE")
                            .property("gender", "male")
                            .property("address", "2714 Hamilton Avenue, San Jose CA 95135");
                    curr++;
                    if (curr == batchSize) {
                        gt.next();
                        g.tx().commit();
                        loaded += batchSize;
                        if (loaded % 10000 == 0)
                            log.info("Loaded {}, total {}", loaded, NUM_VERTICES);
                        gt = null;
                        curr = 0;
                    }
                }

                loadGraph = true;
            }

            if (g == null) {
                graph = JanusGraphFactory.open(config);
                g = graph.traversal();
            }

            // Validate the graph
            long cnt = g.V().count().next();
            assertEquals(NUM_VERTICES, cnt);
            g.tx().commit();

        } catch (Exception e) {
            if (g.tx() != null) {
                g.tx().rollback();
            }
            log.error("Got exception", e);
        } finally {
            g.close();
            graph.close();
        }
    }

    @Test
    public void testLoading() {
        log.debug("Blank test to test loading");
    }

    @AfterEach
    public void tearDown() throws Exception {
        resetRequestContext();

        if (g != null) {
            g.close();
        }
        if (graph != null) {
            graph.close();
        }
    }

    private void execLongQuery(GraphTraversalSource g, boolean expected) {
        try {
            GraphTraversal gt = null;
            for (int i = 0; i < 8; ++i) {
                if (gt == null) gt = g.V();
                else gt.V();
                gt.has("name", "John Doe " + i);
            }
            List list = gt.toList();
            g.tx().commit();
            log.info(Arrays.toString(list.toArray()));
            assertTrue(expected);
        } catch (Exception e) {
            unWrapException(e);
            assertTrue(!expected);
        }
    }

    private void sleepWaitFDBTxTimeout() {
        try {
            Thread.sleep(MAX_FDB_TX_TIMEOUT);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void execLargeWrite(GraphTraversalSource g, boolean expected) {
        // Construct a large write batch
        GraphTraversal gt = constructLargeBatchWriteV2(g);

        try {
            // Add edges
            gt.next();
            g.tx().commit();

            long cnt = g.E().count().next();
            g.tx().commit();
            assertEquals(NUM_EDGES_TO_ADD, cnt);

            // Drop those edges
            for (int i = 0; i < NUM_EDGES_TO_ADD; ++i) {
                g.V().has("name", "John Doe " + i).outE().drop().iterate();
                g.tx().commit();
            }

            cnt = g.V().count().next();
            g.tx().commit();
            assertEquals(NUM_VERTICES, cnt);

            cnt = g.E().count().next();
            g.tx().commit();
            assertEquals(0, cnt);

            assertTrue(expected);
        } catch (Exception e) {
            unWrapException(e);
            assertTrue(!expected);
            g.tx().rollback();
        }
    }

    private void unWrapException(Exception e) {
        Throwable fdbException = e;
        while (fdbException != null && !(fdbException instanceof FoundationDBTxException)) {
            fdbException = fdbException.getCause();
        }
        assertTrue(fdbException != null);
    }

    private void execSmallWrite(GraphTraversalSource g, boolean expected) {
        try {
            CallContext context = CallCtxThreadLocalHolder.callCtxThreadLocal.get();

            // Create a new vertex
            Vertex v = g.addV("mytest").property("a", "b").next();
            log.info("testSmallWrite Got vertx id = {}", v.id());
            g.tx().commit();

            // Drop the newly created vertex
            g.V(v.id()).drop().iterate().toList();
            g.tx().commit();

            assertTrue(expected);
        } catch (Exception e) {
            log.info("Got exception", e);
            assertTrue(!expected);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { USE_DEFAULT, SERIALIZABLE })
    public void serializableRead(String isolationLevel) throws Exception {
        graph = JanusGraphFactory.open(getFoundationDBConfiguration(isolationLevel));
        g = graph.traversal();
        for (int i = 0; i < REPEATED; ++i) {
            execLongQuery(g, false);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { READ_COMMITTED_WITH_WRITE, READ_COMMITTED_OR_SERIALIZABLE })
    public void readCommitted(String isolationLevel) throws Exception {
        graph = JanusGraphFactory.open(getFoundationDBConfiguration(isolationLevel));
        g = graph.traversal();
        for (int i = 0; i < REPEATED; ++i) {
            execLongQuery(g, true);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { READ_COMMITTED_WITH_WRITE, READ_COMMITTED_OR_SERIALIZABLE })
    public void readCommittedWithLimitedTime(String isolationLevel) throws Exception {
        graph = JanusGraphFactory.open(getFoundationDBConfiguration(isolationLevel, 5000));
        g = graph.traversal();
        for (int i = 0; i < REPEATED; ++i) {
            execLongQuery(g, false);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { USE_DEFAULT, SERIALIZABLE, READ_COMMITTED_OR_SERIALIZABLE,
            READ_COMMITTED_WITH_WRITE })
    public void testSmallWrite(String isolationLevel) throws Exception {
        graph = JanusGraphFactory.open(getFoundationDBConfiguration(isolationLevel));
        g = graph.traversal();

        // Small writes should go through no matter what storage isolation level is
        for (int i = 0; i < REPEATED; ++i) {
            execSmallWrite(g, true);
        }
    }

    private GraphTraversal<Vertex, Vertex> constructLargeBatchWrite(GraphTraversalSource g) {
        int numPropsPerVertex = 50;

        GraphTraversal<Vertex, Vertex> gt = null;
        for (int j = 0; j < NUM_VERTICES_TO_ADD; ++j) {
            if (gt == null) gt = g.addV("testLargeWrite");
            else gt.addV("testLargeWrite");

            // Large number of properties per vertex cause the transaction to take long time easier
            gt.property("name", "v" + j);
            for (int k = 0; k < numPropsPerVertex; k++) {
                gt.property("prop" + k, "val" + k);
            }
        }
        return gt;
    }

    private GraphTraversal constructLargeBatchWriteV2(GraphTraversalSource g) {
        int numPropsPerVertex = 50;

        GraphTraversal gt = null;
        String label = "testLargeWriteEdge";
        for (int j = 0; j < NUM_EDGES_TO_ADD; ++j) {
            if (gt == null) gt = g.V();
            else gt.V();

            gt.has("name", "John Doe " + j).as("s" + j)
                    .V().has("name", "John Doe " + (NUM_VERTICES-j-1)).as("d" + j);

            // Large number of properties per vertex cause the transaction to take long time easier
            gt.addE(label).property("name", "e" + j);
            for (int k = 0; k < numPropsPerVertex; k++) {
                gt.property("prop" + k, "val" + k);
            }

            gt.from("s" + j).to("d" + j);
        }

        return gt;
    }

    @ParameterizedTest
    @ValueSource(strings = { READ_COMMITTED_WITH_WRITE })
    public void testLargeWriteAllowed(String isolationLevel) {
        graph = JanusGraphFactory.open(getFoundationDBConfiguration(isolationLevel));
        g = graph.traversal();

        // With read_committed_with_write, large batch write is possible
        for (int i = 0; i < REPEATED; ++i) {
            execLargeWrite(g, true);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { USE_DEFAULT, SERIALIZABLE, READ_COMMITTED_OR_SERIALIZABLE })
    public void testLargeWriteNotAllowed(String isolationLevel) {
        graph = JanusGraphFactory.open(getFoundationDBConfiguration(isolationLevel));
        g = graph.traversal();

        // Large batch write is not allowed in this mode
        for (int i = 0; i < REPEATED; ++i) {
            execLargeWrite(g, false);
        }
    }

    private void setRequestContext(String requestReadMode, long maxTraversalTimeout) {
        CallContext context =
                new CallContext("op_name", "keyspace", 900000,
                        900000, "req_id", "app_id", "client_address",
                        "client_version", System.currentTimeMillis(), "gremlin_id",
                        "traversal", null, null,
                        CallContext.ReadMode.valueOf(requestReadMode), maxTraversalTimeout);
        CallCtxThreadLocalHolder.callCtxThreadLocal.set(context);
    }


    private void resetRequestContext() {
        CallCtxThreadLocalHolder.callCtxThreadLocal.set(null);
    }

    @ParameterizedTest
    @MethodSource("testReadCommittedSource")
    public void testLongReadOnlyRequestReadCommitted(String storageIsolationLevel, String requestReadMode) {
        graph = JanusGraphFactory.open(getFoundationDBConfiguration(storageIsolationLevel));
        g = graph.traversal();
        setRequestContext(requestReadMode, 0);

        // If request uses mode READ_COMMITTED, long queries should be ok
        for (int i = 0; i < REPEATED; ++i) {
            execLongQuery(g, true);
        }
    }

    @ParameterizedTest
    @MethodSource("testReadSnapshotSource")
    public void testLongReadOnlyRequestReadSnapshot(String storageIsolationLevel, String requestReadMode) {
        graph = JanusGraphFactory.open(getFoundationDBConfiguration(storageIsolationLevel));
        g = graph.traversal();
        setRequestContext(requestReadMode, 0);

        // If request uses mode READ_SNAPSHOT, long queries should fail
        for (int i = 0; i < REPEATED; ++i) {
            execLongQuery(g, false);
        }
    }

    @ParameterizedTest
    @MethodSource("testReadCommittedSource")
    public void testLongReadOnlyRequestWithLimitedTime(String storageIsolationLevel, String requestReadMode) {
        graph = JanusGraphFactory.open(getFoundationDBConfiguration(storageIsolationLevel));
        g = graph.traversal();
        setRequestContext(requestReadMode, 5000);

        // If request limit time, too long queries should fail
        for (int i = 0; i < REPEATED; ++i) {
            execLongQuery(g, false);
        }
    }

    @ParameterizedTest
    @MethodSource("testReadAllCombined")
    public void testReadOnlyRequestIncludesWrites(String storageIsolationLevel, String requestReadMode) {
        graph = JanusGraphFactory.open(getFoundationDBConfiguration(storageIsolationLevel));
        g = graph.traversal();
        setRequestContext(requestReadMode, 0);

        // If request specifies a read mode and it contains write, the traversal should fail
        for (int i = 0; i < REPEATED; ++i) {
            execSmallWrite(g, false);
            execLargeWrite(g, false);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { USE_DEFAULT, SERIALIZABLE, READ_COMMITTED_OR_SERIALIZABLE, READ_COMMITTED_WITH_WRITE })
    public void testReadOnlyRequestNotSpecifiedShouldPass(String storageIsolationLevel) {
        graph = JanusGraphFactory.open(getFoundationDBConfiguration(storageIsolationLevel));
        g = graph.traversal();
        setRequestContext(NOT_SPECIFIED, 0);

        // Write is allowed if request does not specify a mode
        for (int i = 0; i < REPEATED; ++i) {
            execSmallWrite(g, true);

            switch (storageIsolationLevel) {
                case SERIALIZABLE:
                case USE_DEFAULT:
                    execLongQuery(g, false);
                    execLargeWrite(g, false);
                    break;
                case READ_COMMITTED_OR_SERIALIZABLE:
                    execLongQuery(g, true);
                    execLargeWrite(g, false);
                    break;
                case READ_COMMITTED_WITH_WRITE:
                    execLongQuery(g, true);
                    execLargeWrite(g, true);
                    break;
            }
        }
    }
}
