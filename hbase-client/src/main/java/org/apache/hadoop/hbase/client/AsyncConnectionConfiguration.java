/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_PAUSE;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_META_SCANNER_CACHING;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_PAUSE;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_CACHING;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.HConstants.HBASE_META_SCANNER_CACHING;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_READ_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.BUFFERED_MUTATOR_MAX_MUTATIONS_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.BUFFERED_MUTATOR_MAX_MUTATIONS_KEY;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.HBASE_CLIENT_META_READ_RPC_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.HBASE_CLIENT_META_SCANNER_TIMEOUT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.MAX_KEYVALUE_SIZE_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.PRIMARY_CALL_TIMEOUT_MICROSECOND;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.PRIMARY_CALL_TIMEOUT_MICROSECOND_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.PRIMARY_SCAN_TIMEOUT_MICROSECOND;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.PRIMARY_SCAN_TIMEOUT_MICROSECOND_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.WRITE_BUFFER_SIZE_KEY;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Timeout configs.
 */
@InterfaceAudience.Private
class AsyncConnectionConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncConnectionConfiguration.class);

  /**
   * Parameter name for client pause when server is overloaded, denoted by
   * {@link org.apache.hadoop.hbase.HBaseServerException#isServerOverloaded()}
   */
  public static final String HBASE_CLIENT_PAUSE_FOR_SERVER_OVERLOADED =
    "hbase.client.pause.server.overloaded";

  static {
    // This is added where the configs are referenced. It may be too late to happen before
    // any user _sets_ the old cqtbe config onto a Configuration option. So we still need
    // to handle checking both properties in parsing below. The benefit of calling this is
    // that it should still cause Configuration to log a warning if we do end up falling
    // through to the old deprecated config.
    Configuration.addDeprecation(HConstants.HBASE_CLIENT_PAUSE_FOR_CQTBE,
      HBASE_CLIENT_PAUSE_FOR_SERVER_OVERLOADED);
  }

  /**
   * Configure the number of failures after which the client will start logging. A few failures is
   * fine: region moved, then is not opened, then is overloaded. We try to have an acceptable
   * heuristic for the number of errors we don't log. 5 was chosen because we wait for 1s at this
   * stage.
   */
  public static final String START_LOG_ERRORS_AFTER_COUNT_KEY =
    "hbase.client.start.log.errors.counter";
  public static final int DEFAULT_START_LOG_ERRORS_AFTER_COUNT = 5;

  private final long metaOperationTimeoutNs;

  // timeout for a whole operation such as get, put or delete. Notice that scan will not be effected
  // by this value, see scanTimeoutNs.
  private final long operationTimeoutNs;

  // timeout for each rpc request. Can be overridden by a more specific config, such as
  // readRpcTimeout or writeRpcTimeout.
  private final long rpcTimeoutNs;

  // timeout for each read rpc request
  private final long readRpcTimeoutNs;

  // timeout for each read rpc request against system tables
  private final long metaReadRpcTimeoutNs;

  // timeout for each write rpc request
  private final long writeRpcTimeoutNs;

  private final long pauseNs;

  private final long pauseNsForServerOverloaded;

  private final int maxRetries;

  /** How many retries are allowed before we start to log */
  private final int startLogErrorsCnt;

  // As now we have heartbeat support for scan, ideally a scan will never timeout unless the RS is
  // crash. The RS will always return something before the rpc timeout or scan timeout to tell the
  // client that it is still alive. The scan timeout is used as operation timeout for every
  // operations in a scan, such as openScanner or next.
  private final long scanTimeoutNs;
  private final long metaScanTimeoutNs;

  private final int scannerCaching;

  private final int metaScannerCaching;

  private final long scannerMaxResultSize;

  private final long writeBufferSize;

  private final long writeBufferPeriodicFlushTimeoutNs;

  // this is for supporting region replica get, if the primary does not finished within this
  // timeout, we will send request to secondaries.
  private final long primaryCallTimeoutNs;

  private final long primaryScanTimeoutNs;

  private final long primaryMetaScanTimeoutNs;

  private final int maxKeyValueSize;

  private final int bufferedMutatorMaxMutations;

  AsyncConnectionConfiguration(Configuration conf) {
    long operationTimeoutMs =
      conf.getLong(HBASE_CLIENT_OPERATION_TIMEOUT, DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    this.operationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(operationTimeoutMs);
    this.metaOperationTimeoutNs = TimeUnit.MILLISECONDS
      .toNanos(conf.getLong(HBASE_CLIENT_META_OPERATION_TIMEOUT, operationTimeoutMs));
    long rpcTimeoutMs = conf.getLong(HBASE_RPC_TIMEOUT_KEY, DEFAULT_HBASE_RPC_TIMEOUT);
    this.rpcTimeoutNs = TimeUnit.MILLISECONDS.toNanos(rpcTimeoutMs);
    long readRpcTimeoutMillis = conf.getLong(HBASE_RPC_READ_TIMEOUT_KEY, rpcTimeoutMs);
    this.readRpcTimeoutNs = TimeUnit.MILLISECONDS.toNanos(readRpcTimeoutMillis);
    this.metaReadRpcTimeoutNs = TimeUnit.MILLISECONDS
      .toNanos(conf.getLong(HBASE_CLIENT_META_READ_RPC_TIMEOUT_KEY, readRpcTimeoutMillis));
    this.writeRpcTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getLong(HBASE_RPC_WRITE_TIMEOUT_KEY, rpcTimeoutMs));
    long pauseMs = conf.getLong(HBASE_CLIENT_PAUSE, DEFAULT_HBASE_CLIENT_PAUSE);
    long pauseMsForServerOverloaded = conf.getLong(HBASE_CLIENT_PAUSE_FOR_SERVER_OVERLOADED,
      conf.getLong(HConstants.HBASE_CLIENT_PAUSE_FOR_CQTBE, pauseMs));
    if (pauseMsForServerOverloaded < pauseMs) {
      LOG.warn(
        "The {} setting: {} ms is less than the {} setting: {} ms, use the greater one instead",
        HBASE_CLIENT_PAUSE_FOR_SERVER_OVERLOADED, pauseMsForServerOverloaded, HBASE_CLIENT_PAUSE,
        pauseMs);
      pauseMsForServerOverloaded = pauseMs;
    }
    this.pauseNs = TimeUnit.MILLISECONDS.toNanos(pauseMs);
    this.pauseNsForServerOverloaded = TimeUnit.MILLISECONDS.toNanos(pauseMsForServerOverloaded);
    this.maxRetries = conf.getInt(HBASE_CLIENT_RETRIES_NUMBER, DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.startLogErrorsCnt =
      conf.getInt(START_LOG_ERRORS_AFTER_COUNT_KEY, DEFAULT_START_LOG_ERRORS_AFTER_COUNT);
    long scannerTimeoutMillis = conf.getLong(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);
    this.scanTimeoutNs = TimeUnit.MILLISECONDS.toNanos(scannerTimeoutMillis);
    this.metaScanTimeoutNs = TimeUnit.MILLISECONDS
      .toNanos(conf.getLong(HBASE_CLIENT_META_SCANNER_TIMEOUT, scannerTimeoutMillis));
    this.scannerCaching =
      conf.getInt(HBASE_CLIENT_SCANNER_CACHING, DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
    this.metaScannerCaching =
      conf.getInt(HBASE_META_SCANNER_CACHING, DEFAULT_HBASE_META_SCANNER_CACHING);
    this.scannerMaxResultSize = conf.getLong(HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
      DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
    this.writeBufferSize = conf.getLong(WRITE_BUFFER_SIZE_KEY, WRITE_BUFFER_SIZE_DEFAULT);
    this.writeBufferPeriodicFlushTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getLong(WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS,
        WRITE_BUFFER_PERIODIC_FLUSH_TIMEOUT_MS_DEFAULT));
    this.primaryCallTimeoutNs = TimeUnit.MICROSECONDS.toNanos(
      conf.getLong(PRIMARY_CALL_TIMEOUT_MICROSECOND, PRIMARY_CALL_TIMEOUT_MICROSECOND_DEFAULT));
    this.primaryScanTimeoutNs = TimeUnit.MICROSECONDS.toNanos(
      conf.getLong(PRIMARY_SCAN_TIMEOUT_MICROSECOND, PRIMARY_SCAN_TIMEOUT_MICROSECOND_DEFAULT));
    this.primaryMetaScanTimeoutNs =
      TimeUnit.MICROSECONDS.toNanos(conf.getLong(HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT,
        HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT_DEFAULT));
    this.maxKeyValueSize = conf.getInt(MAX_KEYVALUE_SIZE_KEY, MAX_KEYVALUE_SIZE_DEFAULT);
    this.bufferedMutatorMaxMutations = conf.getInt(BUFFERED_MUTATOR_MAX_MUTATIONS_KEY,
      conf.getInt(HConstants.BATCH_ROWS_THRESHOLD_NAME, BUFFERED_MUTATOR_MAX_MUTATIONS_DEFAULT));
  }

  long getMetaOperationTimeoutNs() {
    return metaOperationTimeoutNs;
  }

  long getOperationTimeoutNs() {
    return operationTimeoutNs;
  }

  long getRpcTimeoutNs() {
    return rpcTimeoutNs;
  }

  long getReadRpcTimeoutNs() {
    return readRpcTimeoutNs;
  }

  long getMetaReadRpcTimeoutNs() {
    return metaReadRpcTimeoutNs;
  }

  long getWriteRpcTimeoutNs() {
    return writeRpcTimeoutNs;
  }

  long getPauseNs() {
    return pauseNs;
  }

  long getPauseNsForServerOverloaded() {
    return pauseNsForServerOverloaded;
  }

  int getMaxRetries() {
    return maxRetries;
  }

  int getStartLogErrorsCnt() {
    return startLogErrorsCnt;
  }

  long getScanTimeoutNs() {
    return scanTimeoutNs;
  }

  long getMetaScanTimeoutNs() {
    return metaScanTimeoutNs;
  }

  int getScannerCaching() {
    return scannerCaching;
  }

  int getMetaScannerCaching() {
    return metaScannerCaching;
  }

  long getScannerMaxResultSize() {
    return scannerMaxResultSize;
  }

  long getWriteBufferSize() {
    return writeBufferSize;
  }

  long getWriteBufferPeriodicFlushTimeoutNs() {
    return writeBufferPeriodicFlushTimeoutNs;
  }

  long getPrimaryCallTimeoutNs() {
    return primaryCallTimeoutNs;
  }

  long getPrimaryScanTimeoutNs() {
    return primaryScanTimeoutNs;
  }

  long getPrimaryMetaScanTimeoutNs() {
    return primaryMetaScanTimeoutNs;
  }

  int getMaxKeyValueSize() {
    return maxKeyValueSize;
  }

  int getBufferedMutatorMaxMutations() {
    return bufferedMutatorMaxMutations;
  }
}
