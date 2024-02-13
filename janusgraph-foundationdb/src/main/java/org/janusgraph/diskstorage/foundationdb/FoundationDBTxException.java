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


import com.apple.foundationdb.FDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBTxException extends RuntimeException {
    private static final Logger log = LoggerFactory.getLogger(FoundationDBTxException.class);

    private static final String msgFormat = "%s (lastFDBException=%s, errCode=%d)";

    private final int errorCode;

    public static FoundationDBTxException withLastFDBException(String message, FDBException lastFDBException) {
        if (lastFDBException == null) {
            log.warn("Do not expect null lastFDBException here");
            return FoundationDBTxException.withErrorCode(message, FoundationDBTxErrorCode.UNEXPECTED_ERROR);
        }

        // Enhance the message with lastFDBException and errorCode
        message = String.format(msgFormat, message, lastFDBException.getMessage(),
                lastFDBException.getCode());

        return new FoundationDBTxException(message, lastFDBException);
    }

    public static FoundationDBTxException withErrorCode(String message, int errCode) {
        message = String.format(msgFormat, message, "null", errCode);
        return new FoundationDBTxException(message, errCode);
    }

    private FoundationDBTxException(String msg, FDBException lastFDBException) {
        super(msg);
        this.errorCode = lastFDBException.getCode();
    }

    private FoundationDBTxException(String msg, int errorCode) {
        super(msg);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}