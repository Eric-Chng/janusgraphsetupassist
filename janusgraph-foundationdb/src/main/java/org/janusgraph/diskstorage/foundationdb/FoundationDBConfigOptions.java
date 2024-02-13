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

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

/**
 * Configuration options for the FoundationDB storage backend.
 * These are managed under the 'fdb' namespace in the configuration.
 *
 * @author Ted Wilmes (twilmes@gmail.com)
 */
/**
 * Modified by stevwest: Modified by eBay to add support for TLS related
 * settings supported by the FoundationDB Java API
 */
@PreInitializeConfigOptions
public interface FoundationDBConfigOptions {

    ConfigNamespace FDB_NS = new ConfigNamespace(
        GraphDatabaseConfiguration.STORAGE_NS,
        "fdb",
        "FoundationDB storage backend options");

    ConfigOption<String> KEYSPACE = new ConfigOption<>(
        FDB_NS,
        "keyspace",
        "The base path of the JanusGraph directory in FoundationDB.  It will be created if it does not exist.",
        ConfigOption.Type.LOCAL,
        "janusgraph");

    /**
     * Unless we explicitly change the version in {@link FoundationDBStoreManager}, the storage plugin
     * will pick up the default version.
     */
    ConfigOption<Integer> VERSION = new ConfigOption<>(
        FDB_NS,
        "version",
        "The version of the FoundationDB cluster.",
        ConfigOption.Type.LOCAL,
        620);

    ConfigOption<String> CLUSTER_FILE_PATH = new ConfigOption<>(
        FDB_NS,
        "cluster-file-path",
        "Path to the FoundationDB cluster file",
        ConfigOption.Type.LOCAL,
        "default");

    ConfigOption<String> ISOLATION_LEVEL = new ConfigOption<>(
        FDB_NS,
        "isolation-level",
        "Options are serializable, read_committed_or_serializable, read_committed_with_write",
        ConfigOption.Type.LOCAL,
        "serializable");

    // stevwest: Add support for TLS related configuration settings
    ConfigOption<String> TLS_CERTIFICATE_FILE_PATH = new ConfigOption<>(
        FDB_NS,
        "tls-certificate-file-path",
        "Path to the file containing the TLS certificate for FoundationDB to use.",
        ConfigOption.Type.LOCAL,
        "");

	ConfigOption<String> TLS_KEY_FILE_PATH = new ConfigOption<>(
        FDB_NS,
        "tls-key-file-path",
        "Path to the file containing the private key corresponding to the TLS certificate for FoundationDB to use.",
        ConfigOption.Type.LOCAL,
        "");

	ConfigOption<String> TLS_CA_FILE_PATH = new ConfigOption<>(
        FDB_NS,
        "tls-ca-file-path",
        "Path to the file containing the CA certificates chain for FoundationDB to use.",
        ConfigOption.Type.LOCAL,
        "");

	ConfigOption<String> TLS_VERIFY_PEERS = new ConfigOption<>(
        FDB_NS,
        "tls-verify-peers",
        "The constraints for FoundatioDB to validate TLS peers against.",
        ConfigOption.Type.LOCAL,
        "");

    ConfigOption<Integer> FDB_READ_VERSION_FETCH_TIME = new ConfigOption<>(
        FDB_NS,
        "fdb-read-version-fetch-time",
        "The interval of fetching FDB read version.",
        ConfigOption.Type.LOCAL,
        250);

    ConfigOption<Boolean> ENABLE_FDB_READ_VERSION_PREFETCH = new ConfigOption<>(
        FDB_NS,
        "enable-fdb-read-version-prefetch",
        "Whether to prefetch the FDB version or not. Enabling it is to favor performance over consistency.",
        ConfigOption.Type.LOCAL,
        false);

    ConfigOption<Boolean> ENABLE_CAUSAL_READ_RISKY = new ConfigOption<>(
        FDB_NS,
        "enable-causal-read-risky",
        "Enable FDB transaction causal read risky.",
        ConfigOption.Type.LOCAL,
        false
    );

    ConfigOption<Boolean> ENABLE_TRANSACTION_TRACE = new ConfigOption<>(
        FDB_NS,
        "enable-transaction-trace",
        "Enable FDB transaction trace.",
        ConfigOption.Type.LOCAL,
        false
    );

    ConfigOption<String> TRANSACTION_TRACE_PATH = new ConfigOption<>(
        FDB_NS,
        "transaction-trace-path",
        "FDB transaction trace path (a folder, not a file)",
        ConfigOption.Type.LOCAL,
        "/tmp/"
    );

    ConfigOption<String> GET_RANGE_MODE = new ConfigOption<>(
        FDB_NS,
        "get-range-mode",
        "The mod of executing FDB getRange, either `iterator` or `list`",
        ConfigOption.Type.LOCAL,
        "list"
    );

    ConfigOption<Long> MAX_GRAPH_TRAVERSAL_TIMEOUT = new ConfigOption<>(
        FDB_NS,
        "max-graph-traversal-timeout",
        "Maximum graph traversal timeout (if read is relaxed with read_mode = READ_COMMITED)",
        ConfigOption.Type.LOCAL,
        600_000L
    );

    ConfigOption<Integer> MAX_ACTIVE_ASYNC_ITERATOR = new ConfigOption<>(
        FDB_NS,
        "max-active-async-iterator",
        "Maximum number of active AsyncIterator used in multi-query",
        ConfigOption.Type.LOCAL,
        8
    );

    /** Reference: https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/TransactionOptions.html#setTimeout(long)
     * Note that this transaction timeout allows the FDB query invocation to be returned from CompletableFuture's get()
     * even when the underlying FDB cluster is not healthy. This timeout is different from the default 5-second transaction timeout.
     * The timeout from this particular parameter leads to error code = 1031, defined at: https://apple.github.io/foundationdb/api-error-codes.html
     * In contrast, 5-second transaction timeout leads to error code = 1007.
     *
     * To differentiate the FDB enforcement from this transaction timeout vs. 5-second transaction limit, we set this transaction timeout
     * value to be larger than the 5-second limit. Often, 5-second transaction limit enforcement by FDB does not happen at exactly 5 seconds.

     */
    ConfigOption<Integer> FDB_TRANSACTION_TIMEOUT = new ConfigOption<>(
            FDB_NS,
            "fdb-transaction-timeout",
            "FDB transaction timeout in milliseconds to cause the transaction automatically to be cancelled",
            ConfigOption.Type.LOCAL,
            8000
    );

    ConfigOption<Boolean> FDB_TRANSACTION_PROFILE = new ConfigOption<>(
            FDB_NS,
            "tx-profile",
            "Turn on FDB transaction profiling that shows the detailed latencies of getSlice and getSlices",
            ConfigOption.Type.LOCAL,
            false
    );
}
