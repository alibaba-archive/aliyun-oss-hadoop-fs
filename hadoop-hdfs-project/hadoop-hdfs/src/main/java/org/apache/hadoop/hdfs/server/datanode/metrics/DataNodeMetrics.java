/**
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
package org.apache.hadoop.hdfs.server.datanode.metrics;

import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.source.JvmMetrics;

import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * This class is for maintaining  the various DataNode statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #blocksRead}.inc()
 *
 */
@InterfaceAudience.Private
@Metrics(about="DataNode metrics", context="dfs")
public class DataNodeMetrics {

  @Metric MutableCounterLong bytesWritten;
  @Metric("Milliseconds spent writing")
  MutableCounterLong totalWriteTime;
  @Metric MutableCounterLong bytesRead;
  @Metric("Milliseconds spent reading")
  MutableCounterLong totalReadTime;
  @Metric MutableCounterLong blocksWritten;
  @Metric MutableCounterLong blocksRead;
  @Metric MutableCounterLong blocksReplicated;
  @Metric MutableCounterLong blocksRemoved;
  @Metric MutableCounterLong blocksVerified;
  @Metric MutableCounterLong blockVerificationFailures;
  @Metric MutableCounterLong blocksCached;
  @Metric MutableCounterLong blocksUncached;
  @Metric MutableCounterLong readsFromLocalClient;
  @Metric MutableCounterLong readsFromRemoteClient;
  @Metric MutableCounterLong writesFromLocalClient;
  @Metric MutableCounterLong writesFromRemoteClient;
  @Metric MutableCounterLong blocksGetLocalPathInfo;
  @Metric("Bytes read by remote client")
  MutableCounterLong remoteBytesRead;
  @Metric("Bytes written by remote client")
  MutableCounterLong remoteBytesWritten;

  // RamDisk metrics on read/write
  @Metric MutableCounterLong ramDiskBlocksWrite;
  @Metric MutableCounterLong ramDiskBlocksWriteFallback;
  @Metric MutableCounterLong ramDiskBytesWrite;
  @Metric MutableCounterLong ramDiskBlocksReadHits;

  // RamDisk metrics on eviction
  @Metric MutableCounterLong ramDiskBlocksEvicted;
  @Metric MutableCounterLong ramDiskBlocksEvictedWithoutRead;
  @Metric MutableRate        ramDiskBlocksEvictionWindowMs;
  final MutableQuantiles[]   ramDiskBlocksEvictionWindowMsQuantiles;


  // RamDisk metrics on lazy persist
  @Metric MutableCounterLong ramDiskBlocksLazyPersisted;
  @Metric MutableCounterLong ramDiskBlocksDeletedBeforeLazyPersisted;
  @Metric MutableCounterLong ramDiskBytesLazyPersisted;
  @Metric MutableRate        ramDiskBlocksLazyPersistWindowMs;
  final MutableQuantiles[]   ramDiskBlocksLazyPersistWindowMsQuantiles;

  @Metric MutableCounterLong fsyncCount;
  
  @Metric MutableCounterLong volumeFailures;

  @Metric("Count of network errors on the datanode")
  MutableCounterLong datanodeNetworkErrors;

  @Metric MutableRate readBlockOp;
  @Metric MutableRate writeBlockOp;
  @Metric MutableRate blockChecksumOp;
  @Metric MutableRate copyBlockOp;
  @Metric MutableRate replaceBlockOp;
  @Metric MutableRate heartbeats;
  @Metric MutableRate blockReports;
  @Metric MutableRate incrementalBlockReports;
  @Metric MutableRate cacheReports;
  @Metric MutableRate packetAckRoundTripTimeNanos;
  final MutableQuantiles[] packetAckRoundTripTimeNanosQuantiles;
  
  @Metric MutableRate flushNanos;
  final MutableQuantiles[] flushNanosQuantiles;
  
  @Metric MutableRate fsyncNanos;
  final MutableQuantiles[] fsyncNanosQuantiles;
  
  @Metric MutableRate sendDataPacketBlockedOnNetworkNanos;
  final MutableQuantiles[] sendDataPacketBlockedOnNetworkNanosQuantiles;
  @Metric MutableRate sendDataPacketTransferNanos;
  final MutableQuantiles[] sendDataPacketTransferNanosQuantiles;

  final MetricsRegistry registry = new MetricsRegistry("datanode");
  final String name;
  JvmMetrics jvmMetrics = null;
  
  public DataNodeMetrics(String name, String sessionId, int[] intervals,
      final JvmMetrics jvmMetrics) {
    this.name = name;
    this.jvmMetrics = jvmMetrics;    
    registry.tag(SessionId, sessionId);
    
    final int len = intervals.length;
    packetAckRoundTripTimeNanosQuantiles = new MutableQuantiles[len];
    flushNanosQuantiles = new MutableQuantiles[len];
    fsyncNanosQuantiles = new MutableQuantiles[len];
    sendDataPacketBlockedOnNetworkNanosQuantiles = new MutableQuantiles[len];
    sendDataPacketTransferNanosQuantiles = new MutableQuantiles[len];
    ramDiskBlocksEvictionWindowMsQuantiles = new MutableQuantiles[len];
    ramDiskBlocksLazyPersistWindowMsQuantiles = new MutableQuantiles[len];
    
    for (int i = 0; i < len; i++) {
      int interval = intervals[i];
      packetAckRoundTripTimeNanosQuantiles[i] = registry.newQuantiles(
          "packetAckRoundTripTimeNanos" + interval + "s",
          "Packet Ack RTT in ns", "ops", "latency", interval);
      flushNanosQuantiles[i] = registry.newQuantiles(
          "flushNanos" + interval + "s", 
          "Disk flush latency in ns", "ops", "latency", interval);
      fsyncNanosQuantiles[i] = registry.newQuantiles(
          "fsyncNanos" + interval + "s", "Disk fsync latency in ns", 
          "ops", "latency", interval);
      sendDataPacketBlockedOnNetworkNanosQuantiles[i] = registry.newQuantiles(
          "sendDataPacketBlockedOnNetworkNanos" + interval + "s", 
          "Time blocked on network while sending a packet in ns",
          "ops", "latency", interval);
      sendDataPacketTransferNanosQuantiles[i] = registry.newQuantiles(
          "sendDataPacketTransferNanos" + interval + "s", 
          "Time reading from disk and writing to network while sending " +
          "a packet in ns", "ops", "latency", interval);
      ramDiskBlocksEvictionWindowMsQuantiles[i] = registry.newQuantiles(
          "ramDiskBlocksEvictionWindows" + interval + "s",
          "Time between the RamDisk block write and eviction in ms",
          "ops", "latency", interval);
      ramDiskBlocksLazyPersistWindowMsQuantiles[i] = registry.newQuantiles(
          "ramDiskBlocksLazyPersistWindows" + interval + "s",
          "Time between the RamDisk block write and disk persist in ms",
          "ops", "latency", interval);
    }
  }

  public static DataNodeMetrics create(Configuration conf, String dnName) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create("DataNode", sessionId, ms);
    String name = "DataNodeActivity-"+ (dnName.isEmpty()
        ? "UndefinedDataNodeName"+ ThreadLocalRandom.current().nextInt()
            : dnName.replace(':', '-'));

    // Percentile measurement is off by default, by watching no intervals
    int[] intervals = 
        conf.getInts(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY);
    
    return ms.register(name, null, new DataNodeMetrics(name, sessionId,
        intervals, jm));
  }

  public String name() { return name; }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }
  
  public void addHeartbeat(long latency) {
    heartbeats.add(latency);
  }

  public void addBlockReport(long latency) {
    blockReports.add(latency);
  }

  public void addIncrementalBlockReport(long latency) {
    incrementalBlockReports.add(latency);
  }

  public void addCacheReport(long latency) {
    cacheReports.add(latency);
  }

  public void incrBlocksReplicated() {
    blocksReplicated.incr();
  }

  public void incrBlocksWritten() {
    blocksWritten.incr();
  }

  public void incrBlocksRemoved(int delta) {
    blocksRemoved.incr(delta);
  }

  public void incrBytesWritten(int delta) {
    bytesWritten.incr(delta);
  }

  public void incrBlockVerificationFailures() {
    blockVerificationFailures.incr();
  }

  public void incrBlocksVerified() {
    blocksVerified.incr();
  }


  public void incrBlocksCached(int delta) {
    blocksCached.incr(delta);
  }

  public void incrBlocksUncached(int delta) {
    blocksUncached.incr(delta);
  }

  public void addReadBlockOp(long latency) {
    readBlockOp.add(latency);
  }

  public void addWriteBlockOp(long latency) {
    writeBlockOp.add(latency);
  }

  public void addReplaceBlockOp(long latency) {
    replaceBlockOp.add(latency);
  }

  public void addCopyBlockOp(long latency) {
    copyBlockOp.add(latency);
  }

  public void addBlockChecksumOp(long latency) {
    blockChecksumOp.add(latency);
  }

  public void incrBytesRead(int delta) {
    bytesRead.incr(delta);
  }

  public void incrBlocksRead() {
    blocksRead.incr();
  }

  public void incrFsyncCount() {
    fsyncCount.incr();
  }

  public void incrTotalWriteTime(long timeTaken) {
    totalWriteTime.incr(timeTaken);
  }

  public void incrTotalReadTime(long timeTaken) {
    totalReadTime.incr(timeTaken);
  }


  public void addPacketAckRoundTripTimeNanos(long latencyNanos) {
    packetAckRoundTripTimeNanos.add(latencyNanos);
    for (MutableQuantiles q : packetAckRoundTripTimeNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void addFlushNanos(long latencyNanos) {
    flushNanos.add(latencyNanos);
    for (MutableQuantiles q : flushNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void addFsyncNanos(long latencyNanos) {
    fsyncNanos.add(latencyNanos);
    for (MutableQuantiles q : fsyncNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  public void incrWritesFromClient(boolean local, long size) {
    if(local) {
      writesFromLocalClient.incr();
    } else {
      writesFromRemoteClient.incr();
      remoteBytesWritten.incr(size);
    }
  }

  public void incrReadsFromClient(boolean local, long size) {

    if (local) {
      readsFromLocalClient.incr();
    } else {
      readsFromRemoteClient.incr();
      remoteBytesRead.incr(size);
    }
  }
  
  public void incrVolumeFailures() {
    volumeFailures.incr();
  }

  public void incrDatanodeNetworkErrors() {
    datanodeNetworkErrors.incr();
  }

  /** Increment for getBlockLocalPathInfo calls */
  public void incrBlocksGetLocalPathInfo() {
    blocksGetLocalPathInfo.incr();
  }

  public void addSendDataPacketBlockedOnNetworkNanos(long latencyNanos) {
    sendDataPacketBlockedOnNetworkNanos.add(latencyNanos);
    for (MutableQuantiles q : sendDataPacketBlockedOnNetworkNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void addSendDataPacketTransferNanos(long latencyNanos) {
    sendDataPacketTransferNanos.add(latencyNanos);
    for (MutableQuantiles q : sendDataPacketTransferNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void incrRamDiskBlocksWrite() {
    ramDiskBlocksWrite.incr();
  }

  public void incrRamDiskBlocksWriteFallback() {
    ramDiskBlocksWriteFallback.incr();
  }

  public void addRamDiskBytesWrite(long bytes) {
    ramDiskBytesWrite.incr(bytes);
  }

  public void incrRamDiskBlocksReadHits() {
    ramDiskBlocksReadHits.incr();
  }

  public void incrRamDiskBlocksEvicted() {
    ramDiskBlocksEvicted.incr();
  }

  public void incrRamDiskBlocksEvictedWithoutRead() {
    ramDiskBlocksEvictedWithoutRead.incr();
  }

  public void addRamDiskBlocksEvictionWindowMs(long latencyMs) {
    ramDiskBlocksEvictionWindowMs.add(latencyMs);
    for (MutableQuantiles q : ramDiskBlocksEvictionWindowMsQuantiles) {
      q.add(latencyMs);
    }
  }

  public void incrRamDiskBlocksLazyPersisted() {
    ramDiskBlocksLazyPersisted.incr();
  }

  public void incrRamDiskBlocksDeletedBeforeLazyPersisted() {
    ramDiskBlocksDeletedBeforeLazyPersisted.incr();
  }

  public void incrRamDiskBytesLazyPersisted(long bytes) {
    ramDiskBytesLazyPersisted.incr(bytes);
  }

  public void addRamDiskBlocksLazyPersistWindowMs(long latencyMs) {
    ramDiskBlocksLazyPersistWindowMs.add(latencyMs);
    for (MutableQuantiles q : ramDiskBlocksLazyPersistWindowMsQuantiles) {
      q.add(latencyMs);
    }
  }
}
