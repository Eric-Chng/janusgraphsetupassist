package org.janusgraph;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.FixedHostPortGenericContainer;

import java.io.IOException;
import java.net.ServerSocket;

import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class FoundationDBContainer extends FixedHostPortGenericContainer<FoundationDBContainer> {
    private final Logger log = LoggerFactory.getLogger(FoundationDBContainer.class);

    public static final String DEFAULT_IMAGE_AND_TAG = "foundationdb/foundationdb:6.2.27";
    private static final Integer DEFAULT_PORT = 4500;
    private static final String FDB_CLUSTER_FILE_ENV_KEY = "FDB_CLUSTER_FILE";
    private static final String FDB_NETWORKING_MODE_ENV_KEY = "FDB_NETWORKING_MODE";
    private static final String FDB_PORT_ENV_KEY = "FDB_PORT";
    private static final String DEFAULT_NETWORKING_MODE = "host";
    private static final String DEFAULT_CLUSTER_FILE_PARENT_DIR = "/etc/foundationdb";
    private static final String DEFAULT_CLUSTER_FILE_PATH = DEFAULT_CLUSTER_FILE_PARENT_DIR + "/" + "fdb.cluster";
    private static final String DEFAULT_VOLUME_SOURCE_PATH = "./fdb";

    private final String fdbClusterFile;
    private final boolean useRemoteFDBServer;

    private static FoundationDBContainer container = null;

    public static FoundationDBContainer getContainer() {
        if (container == null) {
            container = new FoundationDBContainer();
            container.start();
        }
        return container;
    }

    public FoundationDBContainer(String dockerImageName) {
        super(dockerImageName);
        Integer port = findRandomOpenPortOnAllLocalInterfaces();
        this.addFixedExposedPort(port, port);
        this.addExposedPorts(port);
        this.addEnv(FDB_CLUSTER_FILE_ENV_KEY, DEFAULT_CLUSTER_FILE_PATH);
        this.addEnv(FDB_PORT_ENV_KEY, port.toString());
        this.addEnv(FDB_NETWORKING_MODE_ENV_KEY, DEFAULT_NETWORKING_MODE);
        this.withClasspathResourceMapping(DEFAULT_VOLUME_SOURCE_PATH, DEFAULT_CLUSTER_FILE_PARENT_DIR, BindMode.READ_WRITE);

        fdbClusterFile = System.getProperty("FDB_CLUSTER_FILE");
        useRemoteFDBServer = (fdbClusterFile != null);
    }

    public FoundationDBContainer(){
        this(DEFAULT_IMAGE_AND_TAG);
    }

    private Integer findRandomOpenPortOnAllLocalInterfaces() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            log.error("Couldn't open random port, using default port '%d'.", DEFAULT_PORT);
            return DEFAULT_PORT;
        }
    }

    @Override
    public void start() {
        if (useRemoteFDBServer) return;

        if (this.getContainerId() != null) {
            // Already started this container.
            return;
        }
        super.start();
        // initialize the database
        Container.ExecResult execResult;
        try {
            execResult = this.execInContainer("fdbcli", "--exec", "configure new single ssd");
        } catch (UnsupportedOperationException | IOException | InterruptedException e) {
            throw new ContainerLaunchException("Container startup failed. Failed to initialize the database.", e);
        }
        if (execResult.getExitCode() != 0) {
            throw new ContainerLaunchException("Container startup failed. Failed to initialize the database. Received non zero exit code from fdbcli command. Response code was: " + execResult.getExitCode() + ".");
        }
    }

    public ModifiableConfiguration getFoundationDBConfiguration(long maxTraversalTimeout) {
        return getFoundationDBConfiguration("janusgraph-test-fdb", maxTraversalTimeout);
    }

    public ModifiableConfiguration getFoundationDBConfiguration(String graphName) {
        return getFoundationDBConfiguration(graphName, 600000);
    }

    public ModifiableConfiguration getFoundationDBConfiguration() {
        return getFoundationDBConfiguration("janusgraph-test-fdb", 600000);
    }

    public ModifiableConfiguration getFoundationDBConfiguration(final String graphName,
                                                                long maxGraphTraversalTimeout) {
        String clusterFilePath = useRemoteFDBServer ? fdbClusterFile : "target/test-classes/fdb/fdb.cluster";
        ModifiableConfiguration config = buildGraphConfiguration()
                 .set(STORAGE_BACKEND,"org.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager")
                 .set(KEYSPACE, graphName)
                 .set(DROP_ON_CLEAR, false)
                 .set(CLUSTER_FILE_PATH, clusterFilePath)
                 .set(ISOLATION_LEVEL, "read_committed_or_serializable")
                 .set(GET_RANGE_MODE, "iterator")
                 .set(MAX_GRAPH_TRAVERSAL_TIMEOUT, maxGraphTraversalTimeout)
                 .set(FDB_TRANSACTION_TIMEOUT, 0)
                 .set(VERSION, 620);

        return config;
    }

    public WriteConfiguration getFoundationDBGraphConfiguration(long maxGraphTraversalTimeout) {
        return getFoundationDBConfiguration(maxGraphTraversalTimeout).getConfiguration();
    }

    public WriteConfiguration getFoundationDBGraphConfiguration() {
        return getFoundationDBConfiguration(600000).getConfiguration();
    }
}