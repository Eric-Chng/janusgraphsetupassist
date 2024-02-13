package org.janusgraph.diskstorage.foundationdb.metrics;

/**
 * Created by eBay NuGraph team
 */
public class FoundationDBMetricsDefs {

    public static final String NAMESPACE="nugraph";
    public static final String SUBSYSTEM="fdbstorage";

    public static final double  TO_MILLISECONDS_CONVERSION = 1000000.0;

    public static interface FDB_TNX_METHOD_CALL {
        public final static String get = "get";
        public final static String getRange = "getRange";
        public final static String getMultiRange = "getMultiRange";
    }

    public static interface FDB_STORAGEPLUGIN_METHOD_CALL {
        public final static String get = "get";
        public final static String getSliceNonAsync = "getSliceNonAsync";
        public final static String getSliceAsync = "getSliceAsync";
        public final static String getSlicesNonAsync = "getSlicesNonAsync";
        public final static String getSlicesAsync = "getSlicesAsync";
        public final static String insert = "insert";
        public final static String delete = "delete";

        //for store manager
        public final static String mutateMany = "mutateMany";
    }

    /**
     *Note: the following histogram buckets are defined in milliseconds. This definition will have to be
     * exactly the same as the one defined in GraphDB service.
     */
    public static final double[] buckets =
            {
                    0.3, 0.45, 0.75, 1.0, 3.0, 5.0, 7.0, 9.0, 11.0, 13.0, 15.0, 17.0, 19.0,
                    21.0, 32.0, 45.0, 75.0, 110.0, 160.0, 240.0, 360.0, 540.0, 800.0, 1200.0,
                    1800.0, 2700.0, 4000.0, 6000.0, 9000.0, 13500, 20250, 30375, 45562.5,
                    68343.75, 102515.625, 153773.4375, 250000, 375000, 600000
            };


    /**
     * This bucket array is designed for the queue length, for example, the number of the queries in the getSlices
     * method.
     */
    public static final double[] qbuckets =
            {
                 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192,  16384, 32768, 65536
            };

}
