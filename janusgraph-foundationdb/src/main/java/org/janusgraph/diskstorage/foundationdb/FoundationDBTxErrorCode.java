package org.janusgraph.diskstorage.foundationdb;

public class FoundationDBTxErrorCode {
    public static final int MAX_TRANSACTION_RESET = 9000;
    public static final int OPERATION_CONFLICT_WITH_ISOLATION_LEVEL = 9001;
    public static final int TRANSACTION_NOT_RESTARTABLE = 9001;
    public static final int MAX_TRANSACTION_TIME_EXCEEDED = 9002;
    public static final int GET_MULTI_RANGE_ERROR = 9003;
    public static final int UNEXPECTED_ERROR = 9100;
}
