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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.NetworkOptions;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.ebay.nugraph.common.CallContext;
import com.ebay.nugraph.common.CallCtxThreadLocalHolder;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricGroup;
import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsDefs;
import org.janusgraph.diskstorage.foundationdb.metrics.FoundationDBMetricsUtil;
import org.janusgraph.diskstorage.foundationdb.utils.LogWithCallContext;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

/**
 * Experimental FoundationDB storage manager implementation.
 *
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBStoreManager extends AbstractStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBStoreManager.class);

    private static final String PROP_FDB_DC_ID = "FDB_DC_ID";
    private static final String PROP_ENABLE_FDB_READ_VERSION_PREFETCH = "ENABLE_FDB_READ_VERSION_PREFETCH";
    private static final String PROP_READ_ONLY_MODE = "READ_ONLY_MODE";

    public static final int ASYNC = 0, NON_ASYNC = 1;
    private int mode;

    private final Map<String, FoundationDBKeyValueStore> stores;

    protected FDB fdb;
    protected Database db;
    protected final StoreFeatures features;
    protected DirectorySubspace rootDirectory;
    protected final String rootDirectoryName;

    // Note that this isolation is at the store manager
    protected final FoundationDBTx.IsolationLevel isolationLevel;

    protected final String fdbVersion;
    protected FoundationDBMetricGroup metricGroup;
    protected final boolean enableCausalReadRisky;
    protected final boolean enableTransactionTrace;
    protected final boolean keyspaceReadOnlyMode;

    protected FoundationDBGetReadVersionWorker getReadVersionWorker = null;

    private final long maxTraversalTimeoutMs;     // 10 minutes
    private final long transactionTimeoutMs;      // default with 5000 ms.
    private final int maxActiveAsyncIterator;

    private final boolean txProfileEnable;

    public FoundationDBStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        stores = new ConcurrentHashMap<>();

        fdb = initFDB(determineFoundationDbVersion(configuration));

        enableTransactionTrace = configuration.get(FoundationDBConfigOptions.ENABLE_TRANSACTION_TRACE);
        if (enableTransactionTrace) {
            String transactionTracePath = configuration.get(FoundationDBConfigOptions.TRANSACTION_TRACE_PATH);
            if (transactionTracePath != null) {
                fdb.options().setTraceEnable(transactionTracePath);
                log.info("Transaction trace enabled.");
            }
        }

        String readOnlyModeStr = System.getenv(PROP_READ_ONLY_MODE);
        if (readOnlyModeStr == null) {
            readOnlyModeStr = System.getProperty(PROP_READ_ONLY_MODE);
        }

        boolean readOnly = false;
        if (readOnlyModeStr != null) {
            try {
                readOnly = Boolean.parseBoolean(readOnlyModeStr);
            } catch (Exception e) {
                log.error("Cannot check read-only mode.");
            }
        }
        keyspaceReadOnlyMode = readOnly;
        log.info("FDB Read-only mode = {}", keyspaceReadOnlyMode);

        rootDirectoryName = determineRootDirectoryName(configuration);

        configureTlsSettings(fdb, configuration);

        db = !"default".equals(configuration.get(FoundationDBConfigOptions.CLUSTER_FILE_PATH)) ?
            fdb.open(configuration.get(FoundationDBConfigOptions.CLUSTER_FILE_PATH)) : fdb.open();

        txProfileEnable = configuration.get(FoundationDBConfigOptions.FDB_TRANSACTION_PROFILE);
        if (txProfileEnable) {
            log.info("Transaction profiling is enable");
        }

        String dcId = System.getenv(PROP_FDB_DC_ID);
        if (dcId == null) {
            dcId = System.getProperty(PROP_FDB_DC_ID);
            if (dcId == null) {
                log.warn("Data center id not provided. This must be provided in production environment.");
            }
        }
        if (dcId != null) {
            log.info("Set data center id to " + dcId);
            db.options().setDatacenterId(dcId);
        };
//        db.options().setLocationCacheSize(1_000_000);

        final String isolationLevelStr = configuration.get(FoundationDBConfigOptions.ISOLATION_LEVEL);
        switch (isolationLevelStr.toLowerCase().trim()) {
            case "serializable":
                isolationLevel = FoundationDBTx.IsolationLevel.SERIALIZABLE;
                break;
            case "read_committed_or_serializable":
                isolationLevel = FoundationDBTx.IsolationLevel.READ_COMMITTED_NO_WRITE;
                break;
            case "read_committed_with_write":
                isolationLevel = FoundationDBTx.IsolationLevel.READ_COMMITTED_WITH_WRITE;
                break;
            default:
                throw new PermanentBackendException("Unrecognized isolation level " + isolationLevelStr);
        }
        log.info("Isolation level is set to {}", isolationLevel);

        maxTraversalTimeoutMs = configuration.get(FoundationDBConfigOptions.MAX_GRAPH_TRAVERSAL_TIMEOUT);
        maxActiveAsyncIterator = configuration.get(FoundationDBConfigOptions.MAX_ACTIVE_ASYNC_ITERATOR);

        final String getRangeMode = configuration.get(FoundationDBConfigOptions.GET_RANGE_MODE);
        switch (getRangeMode.toLowerCase().trim()) {
            case "iterator":
                mode = ASYNC;
                break;
            case "list":
                mode = NON_ASYNC;
                break;
        }
        log.info("GetRange mode is set to {}, code is {}", getRangeMode, mode);

        initialize(rootDirectoryName);

        features = new StandardStoreFeatures.Builder()
                    .orderedScan(true)
                    .transactional(transactional)
                    .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
                    .locking(true)
                    .keyOrdered(true)
                    .supportsInterruption(false)
                    .optimisticLocking(true)
                    .multiQuery(true)
                    .build();

        fdbVersion = configuration.get(FoundationDBConfigOptions.VERSION).toString();
        initializeMetricsCollector();

        String strVal = System.getenv(PROP_ENABLE_FDB_READ_VERSION_PREFETCH);
        if (strVal == null) {
            strVal = System.getProperty(PROP_ENABLE_FDB_READ_VERSION_PREFETCH);
        }

        Boolean enableFdbVersionPrefetch = null;
        if (strVal != null) {
            try {
                enableFdbVersionPrefetch = Boolean.parseBoolean(strVal);
            } catch (Exception e) {
                log.error("Cannot parse value for enableFdbVersionPrefetch from system env and system property.");
            }
        }

        if (enableFdbVersionPrefetch == null) {
            log.warn("Get value for enableFdbVersionPrefetch from configuration file");
            enableFdbVersionPrefetch = configuration.get(FoundationDBConfigOptions.ENABLE_FDB_READ_VERSION_PREFETCH);
        }
        log.info("ENABLE_FDB_READ_VERSION_PREFETCH set to {}", enableFdbVersionPrefetch);

        if (enableFdbVersionPrefetch) {
            int fetchIntervalInMs = 500;
            try {
                fetchIntervalInMs = configuration.get(FoundationDBConfigOptions.FDB_READ_VERSION_FETCH_TIME);
            } catch (Exception e) {
                log.error("Cannot get FDB_READ_VERSION_FETCH_TIME from FDB config file.");
            }
            log.info("FDB_READ_VERSION_FETCH_TIME set to {}", fetchIntervalInMs);

            getReadVersionWorker = new FoundationDBGetReadVersionWorker(db, fetchIntervalInMs);
            getReadVersionWorker.start();
        }

        enableCausalReadRisky = configuration.get(FoundationDBConfigOptions.ENABLE_CAUSAL_READ_RISKY);
        log.info("ENABLE_CAUSAL_READ_RISKY set to {}", enableCausalReadRisky);

        transactionTimeoutMs = configuration.get(FoundationDBConfigOptions.FDB_TRANSACTION_TIMEOUT);
        log.info ("FDB_TRANSACTION_TIMEOUT set to {}", transactionTimeoutMs);

        log.info("FoundationDBStoreManager initialized");
    }

    private FDB initFDB(int version) {
        try {
            log.info("Initialize FDB");
            Method method = FDB.class.getDeclaredMethod("selectAPIVersion", int.class, boolean.class);
            method.setAccessible(true);
            return (FDB) method.invoke(null, version, false);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            log.error("Cannot initialize FDB with controlRuntime=false to disable shutdownHook");
            return FDB.selectAPIVersion(version);
        }
    }

    public int getMaxActiveAsyncIterator() { return maxActiveAsyncIterator; }

    private void initialize(final String directoryName) throws BackendException {
        try {
            // create the root directory to hold the JanusGraph data
            CompletableFuture<DirectorySubspace> future = DirectoryLayer.getDefault().createOrOpen(db,
                    PathUtil.from(directoryName));
            rootDirectory = future.get(10_000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (e instanceof TimeoutException) {
                log.error("Cannot connect to FDB. Please check whether the fdb.cluster file is still valid.");
            }
            throw new PermanentBackendException(e);
        }
    }


    private  void initializeMetricsCollector() throws BackendException{
        try {
            metricGroup = FoundationDBMetricGroup.getInstance();
            metricGroup.recordFDBVersion(fdbVersion);

        }
        catch (Exception e) {
            log.error("encounter exception when initialize metrics collector", e);
            throw new PermanentBackendException(e);
        }

    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig txCfg) throws BackendException {
        try {
            final Transaction tx = db.createTransaction();

            // Enable transaction trace, skip the transactions related to org.janusgraph.sys
            CallContext context = CallCtxThreadLocalHolder.callCtxThreadLocal.get();
            final String transactionId = (context == null) ? UUID.randomUUID().toString() : context.getRequestId();
            final boolean isJanusSysTx = txCfg.getGroupName() != null && txCfg.getGroupName().startsWith("org.janusgraph.sys");

            if (enableTransactionTrace && !isJanusSysTx) {
                // Generate a transaction id
                tx.options().setDebugTransactionIdentifier(transactionId);
                tx.options().setLogTransaction();
            }

            if (enableCausalReadRisky) {
                tx.options().setCausalReadRisky();
            }

            // Set transaction timeout
            tx.options().setTimeout(transactionTimeoutMs);

            // Favor the server mode first and foremost.
            // If the server mode is read only, by default read mode is READ_SNAPSHOT
            CallContext.ReadMode requestedReadMode = keyspaceReadOnlyMode ? CallContext.ReadMode.READ_SNAPSHOT :
                    CallContext.ReadMode.NOT_SPECIFIED;

            long maxTraversalTimeoutMs = this.maxTraversalTimeoutMs;
            if (context != null) {
                if (context.isReadOnly()) {
                    requestedReadMode = context.getReadMode();
                }

                maxTraversalTimeoutMs = context.getMaxTraversalTimeoutMs();
            }

            if (maxTraversalTimeoutMs == 0) {
                maxTraversalTimeoutMs = this.maxTraversalTimeoutMs;
            }

            // Note: if keyspace is configured in read-only mode, always use pre-fetch version for graph traversals
            if (getReadVersionWorker != null && (requestedReadMode != CallContext.ReadMode.NOT_SPECIFIED)) {
                Long readVersion = getReadVersionWorker.getReadVersion();
                if (readVersion != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Set read version to {}", readVersion);
                    }
                    tx.setReadVersion(readVersion.longValue());

                    //keep track of read version when it has been used.
                    FoundationDBMetricsUtil.TransactionScope.recordTransactionReadVersion(
                            metricGroup, isolationLevel.name(), readVersion.longValue());
                }
            }

            final StoreTransaction fdbTx = new FoundationDBTx(db, tx, transactionId, txCfg, isolationLevel, this,
                    requestedReadMode, maxTraversalTimeoutMs);

            if (log.isTraceEnabled()) {
                log.trace("FoundationDB tx created", new TransactionBegin(fdbTx.toString()));
            }

            return fdbTx;
        } catch (Exception e) {
            throw new PermanentBackendException("Could not start FoundationDB transaction", e);
        }
    }

    @Override
    public FoundationDBKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            return stores.get(name);
        }
        try {
            final DirectorySubspace storeDb = rootDirectory.createOrOpen(db, PathUtil.from(name)).get();
            if (log.isDebugEnabled()) {
                log.debug("Opened database {}", name /*, new Throwable()*/);
            }

            FoundationDBKeyValueStore store = new FoundationDBKeyValueStore(name, storeDb, this);
            stores.put(name, store);
            return store;
        } catch (Exception e) {
            throw new PermanentBackendException("Could not open FoundationDB data store", e);
        }
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        long callStartTime = System.nanoTime();

        try {
            for (Map.Entry<String, KVMutation> mutation : mutations.entrySet()) {
                FoundationDBKeyValueStore store = openDatabase(mutation.getKey());
                KVMutation mutationValue = mutation.getValue();

                if (!mutationValue.hasAdditions() && !mutationValue.hasDeletions()) {
                    if (log.isDebugEnabled()) {
                        LogWithCallContext.logDebug(log, String.format("Empty mutation set for %s, doing nothing", mutation.getKey()));
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        LogWithCallContext.logDebug(log, String.format("mutating: %s on thread: %d", mutation.getKey(),Thread.currentThread().getId()));
                    }
                }

                if (mutationValue.hasAdditions()) {
                    for (KeyValueEntry entry : mutationValue.getAdditions()) {
                        store.insert(entry.getKey(), entry.getValue(), txh, 0);
                    }

                    if (log.isDebugEnabled()) {
                        LogWithCallContext.logDebug(log, String.format(
                                "total number of Insertions: %d on thread: %d ",  mutationValue.getAdditions().size(),
                                Thread.currentThread().getId()));
                    }
                }
                if (mutationValue.hasDeletions()) {
                    for (StaticBuffer del : mutationValue.getDeletions()) {
                        store.delete(del, txh);

                    }

                    if (log.isDebugEnabled()) {
                        LogWithCallContext.logDebug(log, String.format("total number of Deletions: %d on thread: %d",
                                mutationValue.getDeletions().size(), Thread.currentThread().getId()));
                    }
                }
            }
        }
        catch (Exception ex) {
            FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                    metricGroup,
                    callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.mutateMany, false);
            FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallFailureCounts(
                    metricGroup,
                    FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.mutateMany, ex);

            if (ex instanceof BackendException) {
                throw (BackendException)ex;
            }
            else {
                throw new PermanentBackendException( ex);
            }
        }

        FoundationDBMetricsUtil.StoragePluginMethodScope.recordMethodLatency (
                metricGroup,
                callStartTime, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.mutateMany, true);
        FoundationDBMetricsUtil.StoragePluginMethodScope.incMethodCallCounts(
                metricGroup, FoundationDBMetricsDefs.FDB_STORAGEPLUGIN_METHOD_CALL.mutateMany);

    }

    void removeDatabase(FoundationDBKeyValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unknown database from the storage manager");
        }
        String name = db.getName();
        stores.remove(name);
        if (log.isDebugEnabled()) {
            log.debug("Removed database {}", name);
        }
    }

    @Override
    public void close() throws BackendException {
        if (fdb != null) {
            if (!stores.isEmpty())
                throw new IllegalStateException("Cannot shutdown manager since some databases are still open");
            try {
                // TODO this looks like a race condition
                //Wait just a little bit before closing so that independent transaction threads can clean up.
                Thread.sleep(30);
            } catch (InterruptedException e) {
                //Ignore
            }
            try {
                db.close();
            } catch (Exception e) {
                throw new PermanentBackendException("Could not close FoundationDB database", e);
            }
        }

        if (getReadVersionWorker != null) {
            getReadVersionWorker.stopRunning();
        }

        log.info("FoundationDBStoreManager closed");
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            rootDirectory.removeIfExists(db).get();
        } catch (Exception e) {
            throw new PermanentBackendException("Could not clear FoundationDB storage", e);
        }

        log.info("FoundationDBStoreManager cleared storage");
    }

    @Override
    public boolean exists() throws BackendException {
        // @todo
        try {
            return DirectoryLayer.getDefault().exists(db, PathUtil.from(rootDirectoryName)).get();
        } catch (InterruptedException e) {
            throw new PermanentBackendException(e);
        } catch (ExecutionException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }


    private static class TransactionBegin extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionBegin(String msg) {
            super(msg);
        }
    }

	/**
	 * Helper method to configure TLS settings in network options and handle any
	 * exceptions that occur if a connection has already been opened to FDB.
	 * 
	 * NOTE: This is a work-around to a limitation in the current FDB implementation
	 * which throws an exception if you attempt to set TLS settings against an FDB
	 * configuration which is already open and simultaneously provide no way to
	 * determine if the FDB database is open.
	 * 
	 * @param fdb    The FDB API entry-point to attempt to configure TLS settings
	 *               against
	 * @param config The JanusGraph Configuration to extra TLS settings from (if
	 *               present)
	 * 
	 * @throws FDBException If an {@linkplain FDBException} exception is thrown with
	 *                      an error code other than {@code 2107} (tls_error)
	 */
	private void configureTlsSettings(FDB fdb, Configuration config) {
		Preconditions.checkNotNull(fdb);
		Preconditions.checkNotNull(config);

		try {
			NetworkOptions netwkOpts = fdb.options();
			String tlsCertFilePath = determineTlsCertPath(config);
			if (!tlsCertFilePath.isEmpty()) {
				netwkOpts.setTLSCertPath(tlsCertFilePath);
				if (log.isInfoEnabled()) {
					log.info("FoundationDB - using TLS Certificate Path: " + tlsCertFilePath);
				}
			}
			else {
                if (log.isInfoEnabled()) {
                    log.info("FoundationDB - TLS Certificate Path is not specified (OK when TLS is not enabled)");
                }
            }

			String tlsKeyFilePath = determineTlsKeyPath(config);
			if (!tlsKeyFilePath.isEmpty()) {
				netwkOpts.setTLSKeyPath(tlsKeyFilePath);
				if (log.isInfoEnabled()) {
					log.info("FoundationDB - using TLS Key Path: " + tlsKeyFilePath);
				}
			}
			else{
                if (log.isInfoEnabled()) {
                    log.info("FoundationDB - TLS Key Path is not specified (OK when TLS is not enabled)");
                }
            }

			String tlsCaFilePath = determineTlsCaPath(config);
			if (!tlsCaFilePath.isEmpty()) {
				netwkOpts.setTLSCaPath(tlsCaFilePath);
				if (log.isInfoEnabled()) {
					log.info("FoundationDB - using TLS CA Path: " + tlsCaFilePath);
				}
			}
			else {
                if (log.isInfoEnabled()) {
                    log.info("FoundationDB - TLS CA Path is not specified (OK when TLS is not enabled)");
                }
            }

			String tlsVerifyPeers = determineTlsVerifyPeers(config);
			if (!tlsVerifyPeers.isEmpty()) {
				netwkOpts.setTLSVerifyPeers(tlsVerifyPeers.getBytes());
				if (log.isInfoEnabled()) {
					log.info("FoundationDB  - using TLS Verify Peers: " + tlsVerifyPeers);
				}
			}
			else {
                if (log.isInfoEnabled()) {
                    log.info("FoundationDB  - TLS Verify Peers is not specified (OK when TLS is not enabled)");
                }
            }
		} catch (FDBException fdbe) {
			// Ignore error code 2107 (tls_error) and re-throw any other others
			if (fdbe.getCode() != 2107) {
				throw fdbe;
			}
		}
	}

    private String determineRootDirectoryName(Configuration config) {
        if (!config.has(FoundationDBConfigOptions.KEYSPACE) && (config.has(GRAPH_NAME))) return config.get(GRAPH_NAME);
        return config.get(FoundationDBConfigOptions.KEYSPACE);
    }

    private int determineFoundationDbVersion(Configuration config) {
        return config.get(FoundationDBConfigOptions.VERSION);
    }
    
	private String determineTlsCaPath(Configuration config) {
		String tlsCaFilePath = null;
		if (config.has(FoundationDBConfigOptions.TLS_CA_FILE_PATH)) {
			tlsCaFilePath = config.get(FoundationDBConfigOptions.TLS_CA_FILE_PATH);
		}
		return tlsCaFilePath != null ? tlsCaFilePath : "";
	}

	private String determineTlsCertPath(Configuration config) {
		String tlsCertFilePath = null;
		if (config.has(FoundationDBConfigOptions.TLS_CERTIFICATE_FILE_PATH)) {
			tlsCertFilePath = config.get(FoundationDBConfigOptions.TLS_CERTIFICATE_FILE_PATH);
		}
		return tlsCertFilePath != null ? tlsCertFilePath : "";
	}
    
	private String determineTlsKeyPath(Configuration config) {
		String tlsKeyFilePath = null;
		if (config.has(FoundationDBConfigOptions.TLS_KEY_FILE_PATH)) {
			tlsKeyFilePath = config.get(FoundationDBConfigOptions.TLS_KEY_FILE_PATH);
		}
		return tlsKeyFilePath != null ? tlsKeyFilePath : "";
	}
    
	private String determineTlsVerifyPeers(Configuration config) {
		String tlsVerifyPeers = null;
		if (config.has(FoundationDBConfigOptions.TLS_VERIFY_PEERS)) {
			tlsVerifyPeers = config.get(FoundationDBConfigOptions.TLS_VERIFY_PEERS);
		}
		return tlsVerifyPeers != null ? tlsVerifyPeers : "";
	}

	public int getMode() {
	    return mode;
    }

    public Database getDb() { return db; }

    public String getRootDirectoryName() { return rootDirectoryName; }

    public boolean isTxProfileEnable() { return txProfileEnable; }
}
