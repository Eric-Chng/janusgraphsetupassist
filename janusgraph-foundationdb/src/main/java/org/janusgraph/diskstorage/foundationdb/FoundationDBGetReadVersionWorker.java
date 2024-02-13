package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * A separate thread that retrieves the getReadVersion upfront
 */
public class FoundationDBGetReadVersionWorker extends Thread {
    private static final Logger log = LoggerFactory.getLogger(FoundationDBGetReadVersionWorker.class);

    /**
     * A transaction should not use the read version if it was updated more than EXPIRED_TIME milliseconds ago.
     */
    private static final long EXPIRED_TIME = 10 * 1000;

    private final long fetchingInterval;
    private final Database db;
//    private final byte[] checkPrimaryDatacenterKey;

    private volatile long readVersion;
    private volatile long lastUpdated;
    private volatile boolean isRunning;

    public FoundationDBGetReadVersionWorker(Database db, long fetchingInterval) {
        this.db = db;
        this.fetchingInterval = fetchingInterval;

        this.lastUpdated = 0;

        // create the special key to check for primary data center
//        ByteBuffer buffer = ByteBuffer.allocate("/primaryDatacenter".length() + 1);
//        buffer.put((byte)0xff);
//        buffer.put("/primaryDatacenter".getBytes(Charset.defaultCharset()));
//        checkPrimaryDatacenterKey = buffer.array();
    }

    @Override
    public void run() {
        log.info("FoundationDBGetReadVersionWorker started.");
        isRunning = true;
        while (isRunning) {
            excTransGetReadVersion();

            if (fetchingInterval > 0) {
                try {
                    Thread.sleep(fetchingInterval);
                } catch (InterruptedException e) {
                    log.error("getReadVersion got interupped.", e);
                }
            }
        }

        log.info("FoundationDBGetReadVersionWorker stopped.");
    }

    private void excTransGetReadVersion() {
        Transaction tx = null;
        try {
            tx = db.createTransaction();
            readVersion = tx.getReadVersion().get();
            tx.commit().get();
            lastUpdated = System.currentTimeMillis();
            if (log.isDebugEnabled()) {
                log.debug("Update read version to {}", readVersion);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Cannot complete getReadVersion transaction.", e);
            if (tx != null) {
                try {
                    tx.cancel();
                } catch (Exception ex) {
                    log.error("Cannot rollback getReadVersion transaction.", ex);
                }
            }
        } finally {
            if (tx != null) {
                try {
                    tx.close();
                } catch (Exception e) {
                    log.error("Cannot close getReadVersion transaction.", e);
                }
            }
        }
    }

    public Long getReadVersion() {
        long current = System.currentTimeMillis();
        if (current - lastUpdated > EXPIRED_TIME) {     // a stale read version
            return null;
        } else {
            return readVersion;
        }
    }

    public void stopRunning() {
        this.isRunning = false;
    }

    public static void main(String[] args) throws InterruptedException {
        FDB fdb = FDB.selectAPIVersion(620);
        Database db = fdb.open("/tmp/nugraphperf80-fdb.cluster");

        FoundationDBGetReadVersionWorker worker =
                new FoundationDBGetReadVersionWorker(db, 100);
        worker.start();

        Thread.sleep(60000);

        worker.stopRunning();
        worker.join();
    }
}
