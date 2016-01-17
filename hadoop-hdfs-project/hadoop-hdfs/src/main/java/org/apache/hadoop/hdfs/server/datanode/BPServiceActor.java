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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Joiner;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;

/**
 * A thread per active or standby namenode to perform:
 * <ul>
 * <li> Pre-registration handshake with namenode</li>
 * <li> Registration with namenode</li>
 * <li> Send periodic heartbeats to the namenode</li>
 * <li> Handle commands received from the namenode</li>
 * </ul>
 */
@InterfaceAudience.Private
class BPServiceActor implements Runnable {
  
  static final Logger LOG = DataNode.LOG;
  final InetSocketAddress nnAddr;
  HAServiceState state;

  final BPOfferService bpos;
  
  volatile long lastCacheReport = 0;
  private final Scheduler scheduler;

  Thread bpThread;
  DatanodeProtocolClientSideTranslatorPB bpNamenode;

  static enum RunningState {
    CONNECTING, INIT_FAILED, RUNNING, EXITED, FAILED;
  }

  private volatile RunningState runningState = RunningState.CONNECTING;

  /**
   * Between block reports (which happen on the order of once an hour) the
   * DN reports smaller incremental changes to its block list. This map,
   * keyed by block ID, contains the pending changes which have yet to be
   * reported to the NN. Access should be synchronized on this object.
   */
  private final Map<DatanodeStorage, PerStoragePendingIncrementalBR>
      pendingIncrementalBRperStorage = Maps.newHashMap();

  // IBR = Incremental Block Report. If this flag is set then an IBR will be
  // sent immediately by the actor thread without waiting for the IBR timer
  // to elapse.
  private volatile boolean sendImmediateIBR = false;
  private volatile boolean shouldServiceRun = true;
  private final DataNode dn;
  private final DNConf dnConf;
  private long prevBlockReportId;

  private DatanodeRegistration bpRegistration;
  final LinkedList<BPServiceActorAction> bpThreadQueue 
      = new LinkedList<BPServiceActorAction>();

  BPServiceActor(InetSocketAddress nnAddr, BPOfferService bpos) {
    this.bpos = bpos;
    this.dn = bpos.getDataNode();
    this.nnAddr = nnAddr;
    this.dnConf = dn.getDnConf();
    prevBlockReportId = ThreadLocalRandom.current().nextLong();
    scheduler = new Scheduler(dnConf.heartBeatInterval, dnConf.blockReportInterval);
  }

  public DatanodeRegistration getBpRegistration() {
    return bpRegistration;
  }

  boolean isAlive() {
    if (!shouldServiceRun || !bpThread.isAlive()) {
      return false;
    }
    return runningState == BPServiceActor.RunningState.RUNNING
        || runningState == BPServiceActor.RunningState.CONNECTING;
  }

  @Override
  public String toString() {
    return bpos.toString() + " service to " + nnAddr;
  }
  
  InetSocketAddress getNNSocketAddress() {
    return nnAddr;
  }

  /**
   * Used to inject a spy NN in the unit tests.
   */
  @VisibleForTesting
  void setNameNode(DatanodeProtocolClientSideTranslatorPB dnProtocol) {
    bpNamenode = dnProtocol;
  }

  @VisibleForTesting
  DatanodeProtocolClientSideTranslatorPB getNameNodeProxy() {
    return bpNamenode;
  }

  /**
   * Perform the first part of the handshake with the NameNode.
   * This calls <code>versionRequest</code> to determine the NN's
   * namespace and version info. It automatically retries until
   * the NN responds or the DN is shutting down.
   * 
   * @return the NamespaceInfo
   */
  @VisibleForTesting
  NamespaceInfo retrieveNamespaceInfo() throws IOException {
    NamespaceInfo nsInfo = null;
    while (shouldRun()) {
      try {
        nsInfo = bpNamenode.versionRequest();
        LOG.debug(this + " received versionRequest response: " + nsInfo);
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.warn("Problem connecting to server: " + nnAddr);
      } catch(IOException e ) {  // namenode is not available
        LOG.warn("Problem connecting to server: " + nnAddr);
      }
      
      // try again in a second
      sleepAndLogInterrupts(5000, "requesting version info from NN");
    }
    
    if (nsInfo != null) {
      checkNNVersion(nsInfo);
    } else {
      throw new IOException("DN shut down before block pool connected");
    }
    return nsInfo;
  }

  private void checkNNVersion(NamespaceInfo nsInfo)
      throws IncorrectVersionException {
    // build and layout versions should match
    String nnVersion = nsInfo.getSoftwareVersion();
    String minimumNameNodeVersion = dnConf.getMinimumNameNodeVersion();
    if (VersionUtil.compareVersions(nnVersion, minimumNameNodeVersion) < 0) {
      IncorrectVersionException ive = new IncorrectVersionException(
          minimumNameNodeVersion, nnVersion, "NameNode", "DataNode");
      LOG.warn(ive.getMessage());
      throw ive;
    }
    String dnVersion = VersionInfo.getVersion();
    if (!nnVersion.equals(dnVersion)) {
      LOG.info("Reported NameNode version '" + nnVersion + "' does not match " +
          "DataNode version '" + dnVersion + "' but is within acceptable " +
          "limits. Note: This is normal during a rolling upgrade.");
    }
  }

  private void connectToNNAndHandshake() throws IOException {
    // get NN proxy
    bpNamenode = dn.connectToNN(nnAddr);

    // First phase of the handshake with NN - get the namespace
    // info.
    NamespaceInfo nsInfo = retrieveNamespaceInfo();
    
    // Verify that this matches the other NN in this HA pair.
    // This also initializes our block pool in the DN if we are
    // the first NN connection for this BP.
    bpos.verifyAndSetNamespaceInfo(nsInfo);
    
    // Second phase of the handshake with the NN.
    register(nsInfo);
  }

  /**
   * Report received blocks and delete hints to the Namenode for each
   * storage.
   *
   * @throws IOException
   */
  private void reportReceivedDeletedBlocks() throws IOException {

    // Generate a list of the pending reports for each storage under the lock
    ArrayList<StorageReceivedDeletedBlocks> reports =
        new ArrayList<StorageReceivedDeletedBlocks>(pendingIncrementalBRperStorage.size());
    synchronized (pendingIncrementalBRperStorage) {
      for (Map.Entry<DatanodeStorage, PerStoragePendingIncrementalBR> entry :
           pendingIncrementalBRperStorage.entrySet()) {
        final DatanodeStorage storage = entry.getKey();
        final PerStoragePendingIncrementalBR perStorageMap = entry.getValue();

        if (perStorageMap.getBlockInfoCount() > 0) {
          // Send newly-received and deleted blockids to namenode
          ReceivedDeletedBlockInfo[] rdbi = perStorageMap.dequeueBlockInfos();
          reports.add(new StorageReceivedDeletedBlocks(storage, rdbi));
        }
      }
      sendImmediateIBR = false;
    }

    if (reports.size() == 0) {
      // Nothing new to report.
      return;
    }

    // Send incremental block reports to the Namenode outside the lock
    boolean success = false;
    final long startTime = monotonicNow();
    try {
      bpNamenode.blockReceivedAndDeleted(bpRegistration,
          bpos.getBlockPoolId(),
          reports.toArray(new StorageReceivedDeletedBlocks[reports.size()]));
      success = true;
    } finally {
      dn.getMetrics().addIncrementalBlockReport(monotonicNow() - startTime);
      if (!success) {
        synchronized (pendingIncrementalBRperStorage) {
          for (StorageReceivedDeletedBlocks report : reports) {
            // If we didn't succeed in sending the report, put all of the
            // blocks back onto our queue, but only in the case where we
            // didn't put something newer in the meantime.
            PerStoragePendingIncrementalBR perStorageMap =
                pendingIncrementalBRperStorage.get(report.getStorage());
            perStorageMap.putMissingBlockInfos(report.getBlocks());
            sendImmediateIBR = true;
          }
        }
      }
    }
  }

  /**
   * @return pending incremental block report for given {@code storage}
   */
  private PerStoragePendingIncrementalBR getIncrementalBRMapForStorage(
      DatanodeStorage storage) {
    PerStoragePendingIncrementalBR mapForStorage =
        pendingIncrementalBRperStorage.get(storage);

    if (mapForStorage == null) {
      // This is the first time we are adding incremental BR state for
      // this storage so create a new map. This is required once per
      // storage, per service actor.
      mapForStorage = new PerStoragePendingIncrementalBR();
      pendingIncrementalBRperStorage.put(storage, mapForStorage);
    }

    return mapForStorage;
  }

  /**
   * Add a blockInfo for notification to NameNode. If another entry
   * exists for the same block it is removed.
   *
   * Caller must synchronize access using pendingIncrementalBRperStorage.
   */
  void addPendingReplicationBlockInfo(ReceivedDeletedBlockInfo bInfo,
      DatanodeStorage storage) {
    // Make sure another entry for the same block is first removed.
    // There may only be one such entry.
    for (Map.Entry<DatanodeStorage, PerStoragePendingIncrementalBR> entry :
          pendingIncrementalBRperStorage.entrySet()) {
      if (entry.getValue().removeBlockInfo(bInfo)) {
        break;
      }
    }
    getIncrementalBRMapForStorage(storage).putBlockInfo(bInfo);
  }

  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  void notifyNamenodeBlock(ReceivedDeletedBlockInfo bInfo,
      String storageUuid, boolean now) {
    synchronized (pendingIncrementalBRperStorage) {
      addPendingReplicationBlockInfo(
          bInfo, dn.getFSDataset().getStorage(storageUuid));
      sendImmediateIBR = true;
      // If now is true, the report is sent right away.
      // Otherwise, it will be sent out in the next heartbeat.
      if (now) {
        pendingIncrementalBRperStorage.notifyAll();
      }
    }
  }

  void notifyNamenodeDeletedBlock(
      ReceivedDeletedBlockInfo bInfo, String storageUuid) {
    synchronized (pendingIncrementalBRperStorage) {
      addPendingReplicationBlockInfo(
          bInfo, dn.getFSDataset().getStorage(storageUuid));
    }
  }

  /**
   * Run an immediate block report on this thread. Used by tests.
   */
  @VisibleForTesting
  void triggerBlockReportForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      scheduler.scheduleHeartbeat();
      long oldBlockReportTime = scheduler.nextBlockReportTime;
      scheduler.forceFullBlockReportNow();
      pendingIncrementalBRperStorage.notifyAll();
      while (oldBlockReportTime == scheduler.nextBlockReportTime) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }
  
  @VisibleForTesting
  void triggerHeartbeatForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      final long nextHeartbeatTime = scheduler.scheduleHeartbeat();
      pendingIncrementalBRperStorage.notifyAll();
      while (nextHeartbeatTime - scheduler.nextHeartbeatTime >= 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  @VisibleForTesting
  void triggerDeletionReportForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      sendImmediateIBR = true;
      pendingIncrementalBRperStorage.notifyAll();

      while (sendImmediateIBR) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  @VisibleForTesting
  boolean hasPendingIBR() {
    return sendImmediateIBR;
  }

  private long generateUniqueBlockReportId() {
    // Initialize the block report ID the first time through.
    // Note that 0 is used on the NN to indicate "uninitialized", so we should
    // not send a 0 value ourselves.
    prevBlockReportId++;
    while (prevBlockReportId == 0) {
      prevBlockReportId = ThreadLocalRandom.current().nextLong();
    }
    return prevBlockReportId;
  }

  /**
   * Report the list blocks to the Namenode
   * @return DatanodeCommands returned by the NN. May be null.
   * @throws IOException
   */
  List<DatanodeCommand> blockReport(long fullBrLeaseId) throws IOException {
    final ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>();

    // Flush any block information that precedes the block report. Otherwise
    // we have a chance that we will miss the delHint information
    // or we will report an RBW replica after the BlockReport already reports
    // a FINALIZED one.
    reportReceivedDeletedBlocks();

    long brCreateStartTime = monotonicNow();
    Map<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists =
        dn.getFSDataset().getBlockReports(bpos.getBlockPoolId());

    // Convert the reports to the format expected by the NN.
    int i = 0;
    int totalBlockCount = 0;
    StorageBlockReport reports[] =
        new StorageBlockReport[perVolumeBlockLists.size()];

    for(Map.Entry<DatanodeStorage, BlockListAsLongs> kvPair : perVolumeBlockLists.entrySet()) {
      BlockListAsLongs blockList = kvPair.getValue();
      reports[i++] = new StorageBlockReport(kvPair.getKey(), blockList);
      totalBlockCount += blockList.getNumberOfBlocks();
    }

    // Send the reports to the NN.
    int numReportsSent = 0;
    int numRPCs = 0;
    boolean success = false;
    long brSendStartTime = monotonicNow();
    long reportId = generateUniqueBlockReportId();
    try {
      if (totalBlockCount < dnConf.blockReportSplitThreshold) {
        // Below split threshold, send all reports in a single message.
        DatanodeCommand cmd = bpNamenode.blockReport(
            bpRegistration, bpos.getBlockPoolId(), reports,
              new BlockReportContext(1, 0, reportId, fullBrLeaseId));
        numRPCs = 1;
        numReportsSent = reports.length;
        if (cmd != null) {
          cmds.add(cmd);
        }
      } else {
        // Send one block report per message.
        for (int r = 0; r < reports.length; r++) {
          StorageBlockReport singleReport[] = { reports[r] };
          DatanodeCommand cmd = bpNamenode.blockReport(
              bpRegistration, bpos.getBlockPoolId(), singleReport,
              new BlockReportContext(reports.length, r, reportId,
                  fullBrLeaseId));
          numReportsSent++;
          numRPCs++;
          if (cmd != null) {
            cmds.add(cmd);
          }
        }
      }
      success = true;
    } finally {
      // Log the block report processing stats from Datanode perspective
      long brSendCost = monotonicNow() - brSendStartTime;
      long brCreateCost = brSendStartTime - brCreateStartTime;
      dn.getMetrics().addBlockReport(brSendCost);
      final int nCmds = cmds.size();
      LOG.info((success ? "S" : "Uns") +
          "uccessfully sent block report 0x" +
          Long.toHexString(reportId) + ",  containing " + reports.length +
          " storage report(s), of which we sent " + numReportsSent + "." +
          " The reports had " + totalBlockCount +
          " total blocks and used " + numRPCs +
          " RPC(s). This took " + brCreateCost +
          " msec to generate and " + brSendCost +
          " msecs for RPC and NN processing." +
          " Got back " +
          ((nCmds == 0) ? "no commands" :
              ((nCmds == 1) ? "one command: " + cmds.get(0) :
                  (nCmds + " commands: " + Joiner.on("; ").join(cmds)))) +
          ".");
    }
    scheduler.scheduleNextBlockReport();
    return cmds.size() == 0 ? null : cmds;
  }

  DatanodeCommand cacheReport() throws IOException {
    // If caching is disabled, do not send a cache report
    if (dn.getFSDataset().getCacheCapacity() == 0) {
      return null;
    }
    // send cache report if timer has expired.
    DatanodeCommand cmd = null;
    final long startTime = monotonicNow();
    if (startTime - lastCacheReport > dnConf.cacheReportInterval) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending cacheReport from service actor: " + this);
      }
      lastCacheReport = startTime;

      String bpid = bpos.getBlockPoolId();
      List<Long> blockIds = dn.getFSDataset().getCacheReport(bpid);
      long createTime = monotonicNow();

      cmd = bpNamenode.cacheReport(bpRegistration, bpid, blockIds);
      long sendTime = monotonicNow();
      long createCost = createTime - startTime;
      long sendCost = sendTime - createTime;
      dn.getMetrics().addCacheReport(sendCost);
      if (LOG.isDebugEnabled()) {
        LOG.debug("CacheReport of " + blockIds.size()
            + " block(s) took " + createCost + " msec to generate and "
            + sendCost + " msecs for RPC and NN processing");
      }
    }
    return cmd;
  }
  
  HeartbeatResponse sendHeartBeat(boolean requestBlockReportLease)
      throws IOException {
    scheduler.scheduleNextHeartbeat();
    StorageReport[] reports =
        dn.getFSDataset().getStorageReports(bpos.getBlockPoolId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending heartbeat with " + reports.length +
                " storage reports from service actor: " + this);
    }
    
    VolumeFailureSummary volumeFailureSummary = dn.getFSDataset()
        .getVolumeFailureSummary();
    int numFailedVolumes = volumeFailureSummary != null ?
        volumeFailureSummary.getFailedStorageLocations().length : 0;
    return bpNamenode.sendHeartbeat(bpRegistration,
        reports,
        dn.getFSDataset().getCacheCapacity(),
        dn.getFSDataset().getCacheUsed(),
        dn.getXmitsInProgress(),
        dn.getXceiverCount(),
        numFailedVolumes,
        volumeFailureSummary,
        requestBlockReportLease);
  }
  
  //This must be called only by BPOfferService
  void start() {
    if ((bpThread != null) && (bpThread.isAlive())) {
      //Thread is started already
      return;
    }
    bpThread = new Thread(this, formatThreadName());
    bpThread.setDaemon(true); // needed for JUnit testing
    bpThread.start();
  }
  
  private String formatThreadName() {
    Collection<StorageLocation> dataDirs =
        DataNode.getStorageLocations(dn.getConf());
    return "DataNode: [" + dataDirs.toString() + "] " +
      " heartbeating to " + nnAddr;
  }
  
  //This must be called only by blockPoolManager.
  void stop() {
    shouldServiceRun = false;
    if (bpThread != null) {
        bpThread.interrupt();
    }
  }
  
  //This must be called only by blockPoolManager
  void join() {
    try {
      if (bpThread != null) {
        bpThread.join();
      }
    } catch (InterruptedException ie) { }
  }
  
  //Cleanup method to be called by current thread before exiting.
  private synchronized void cleanUp() {
    
    shouldServiceRun = false;
    IOUtils.cleanup(null, bpNamenode);
    bpos.shutdownActor(this);
  }

  private void handleRollingUpgradeStatus(HeartbeatResponse resp) throws IOException {
    RollingUpgradeStatus rollingUpgradeStatus = resp.getRollingUpdateStatus();
    if (rollingUpgradeStatus != null &&
        rollingUpgradeStatus.getBlockPoolId().compareTo(bpos.getBlockPoolId()) != 0) {
      // Can this ever occur?
      LOG.error("Invalid BlockPoolId " +
          rollingUpgradeStatus.getBlockPoolId() +
          " in HeartbeatResponse. Expected " +
          bpos.getBlockPoolId());
    } else {
      bpos.signalRollingUpgrade(rollingUpgradeStatus);
    }
  }

  /**
   * Main loop for each BP thread. Run until shutdown,
   * forever calling remote NameNode functions.
   */
  private void offerService() throws Exception {
    LOG.info("For namenode " + nnAddr + " using"
        + " BLOCKREPORT_INTERVAL of " + dnConf.blockReportInterval + "msec"
        + " CACHEREPORT_INTERVAL of " + dnConf.cacheReportInterval + "msec"
        + " Initial delay: " + dnConf.initialBlockReportDelayMs + "msec"
        + "; heartBeatInterval=" + dnConf.heartBeatInterval);
    long fullBlockReportLeaseId = 0;

    //
    // Now loop for a long time....
    //
    while (shouldRun()) {
      try {
        final long startTime = scheduler.monotonicNow();

        //
        // Every so often, send heartbeat or block-report
        //
        final boolean sendHeartbeat = scheduler.isHeartbeatDue(startTime);
        HeartbeatResponse resp = null;
        if (sendHeartbeat) {
          //
          // All heartbeat messages include following info:
          // -- Datanode name
          // -- data transfer port
          // -- Total capacity
          // -- Bytes remaining
          //
          boolean requestBlockReportLease = (fullBlockReportLeaseId == 0) &&
                  scheduler.isBlockReportDue(startTime);
          if (!dn.areHeartbeatsDisabledForTests()) {
            resp = sendHeartBeat(requestBlockReportLease);
            assert resp != null;
            if (resp.getFullBlockReportLeaseId() != 0) {
              if (fullBlockReportLeaseId != 0) {
                LOG.warn(nnAddr + " sent back a full block report lease " +
                        "ID of 0x" +
                        Long.toHexString(resp.getFullBlockReportLeaseId()) +
                        ", but we already have a lease ID of 0x" +
                        Long.toHexString(fullBlockReportLeaseId) + ". " +
                        "Overwriting old lease ID.");
              }
              fullBlockReportLeaseId = resp.getFullBlockReportLeaseId();
            }
            dn.getMetrics().addHeartbeat(scheduler.monotonicNow() - startTime);

            // If the state of this NN has changed (eg STANDBY->ACTIVE)
            // then let the BPOfferService update itself.
            //
            // Important that this happens before processCommand below,
            // since the first heartbeat to a new active might have commands
            // that we should actually process.
            bpos.updateActorStatesFromHeartbeat(
                this, resp.getNameNodeHaState());
            state = resp.getNameNodeHaState().getState();

            if (state == HAServiceState.ACTIVE) {
              handleRollingUpgradeStatus(resp);
            }

            long startProcessCommands = monotonicNow();
            if (!processCommand(resp.getCommands()))
              continue;
            long endProcessCommands = monotonicNow();
            if (endProcessCommands - startProcessCommands > 2000) {
              LOG.info("Took " + (endProcessCommands - startProcessCommands)
                  + "ms to process " + resp.getCommands().length
                  + " commands from NN");
            }
          }
        }
        if (sendImmediateIBR || sendHeartbeat) {
          reportReceivedDeletedBlocks();
        }

        List<DatanodeCommand> cmds = null;
        boolean forceFullBr =
            scheduler.forceFullBlockReport.getAndSet(false);
        if (forceFullBr) {
          LOG.info("Forcing a full block report to " + nnAddr);
        }
        if ((fullBlockReportLeaseId != 0) || forceFullBr) {
          cmds = blockReport(fullBlockReportLeaseId);
          fullBlockReportLeaseId = 0;
        }
        processCommand(cmds == null ? null : cmds.toArray(new DatanodeCommand[cmds.size()]));

        if (!dn.areCacheReportsDisabledForTests()) {
          DatanodeCommand cmd = cacheReport();
          processCommand(new DatanodeCommand[]{ cmd });
        }

        //
        // There is no work to do;  sleep until hearbeat timer elapses, 
        // or work arrives, and then iterate again.
        //
        long waitTime = scheduler.getHeartbeatWaitTime();
        synchronized(pendingIncrementalBRperStorage) {
          if (waitTime > 0 && !sendImmediateIBR) {
            try {
              pendingIncrementalBRperStorage.wait(waitTime);
            } catch (InterruptedException ie) {
              LOG.warn("BPOfferService for " + this + " interrupted");
            }
          }
        } // synchronized
      } catch(RemoteException re) {
        String reClass = re.getClassName();
        if (UnregisteredNodeException.class.getName().equals(reClass) ||
            DisallowedDatanodeException.class.getName().equals(reClass) ||
            IncorrectVersionException.class.getName().equals(reClass)) {
          LOG.warn(this + " is shutting down", re);
          shouldServiceRun = false;
          return;
        }
        LOG.warn("RemoteException in offerService", re);
        try {
          long sleepTime = Math.min(1000, dnConf.heartBeatInterval);
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      } catch (IOException e) {
        LOG.warn("IOException in offerService", e);
      }
      processQueueMessages();
    } // while (shouldRun())
  } // offerService

  /**
   * Register one bp with the corresponding NameNode
   * <p>
   * The bpDatanode needs to register with the namenode on startup in order
   * 1) to report which storage it is serving now and 
   * 2) to receive a registrationID
   *  
   * issued by the namenode to recognize registered datanodes.
   * 
   * @param nsInfo current NamespaceInfo
   * @see FSNamesystem#registerDatanode(DatanodeRegistration)
   * @throws IOException
   */
  void register(NamespaceInfo nsInfo) throws IOException {
    // The handshake() phase loaded the block pool storage
    // off disk - so update the bpRegistration object from that info
    DatanodeRegistration newBpRegistration = bpos.createRegistration();

    LOG.info(this + " beginning handshake with NN");

    while (shouldRun()) {
      try {
        // Use returned registration from namenode with updated fields
        newBpRegistration = bpNamenode.registerDatanode(newBpRegistration);
        newBpRegistration.setNamespaceInfo(nsInfo);
        bpRegistration = newBpRegistration;
        break;
      } catch(EOFException e) {  // namenode might have just restarted
        LOG.info("Problem connecting to server: " + nnAddr + " :"
            + e.getLocalizedMessage());
        sleepAndLogInterrupts(1000, "connecting to server");
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + nnAddr);
        sleepAndLogInterrupts(1000, "connecting to server");
      }
    }
    
    LOG.info("Block pool " + this + " successfully registered with NN");
    bpos.registrationSucceeded(this, bpRegistration);

    // random short delay - helps scatter the BR from all DNs
    scheduler.scheduleBlockReport(dnConf.initialBlockReportDelayMs);
  }


  private void sleepAndLogInterrupts(int millis,
      String stateString) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      LOG.info("BPOfferService " + this + " interrupted while " + stateString);
    }
  }

  /**
   * No matter what kind of exception we get, keep retrying to offerService().
   * That's the loop that connects to the NameNode and provides basic DataNode
   * functionality.
   *
   * Only stop when "shouldRun" or "shouldServiceRun" is turned off, which can
   * happen either at shutdown or due to refreshNamenodes.
   */
  @Override
  public void run() {
    LOG.info(this + " starting to offer service");

    try {
      while (true) {
        // init stuff
        try {
          // setup storage
          connectToNNAndHandshake();
          break;
        } catch (IOException ioe) {
          // Initial handshake, storage recovery or registration failed
          runningState = RunningState.INIT_FAILED;
          if (shouldRetryInit()) {
            // Retry until all namenode's of BPOS failed initialization
            LOG.error("Initialization failed for " + this + " "
                + ioe.getLocalizedMessage());
            sleepAndLogInterrupts(5000, "initializing");
          } else {
            runningState = RunningState.FAILED;
            LOG.error("Initialization failed for " + this + ". Exiting. ", ioe);
            return;
          }
        }
      }

      runningState = RunningState.RUNNING;

      while (shouldRun()) {
        try {
          offerService();
        } catch (Exception ex) {
          LOG.error("Exception in BPOfferService for " + this, ex);
          sleepAndLogInterrupts(5000, "offering service");
        }
      }
      runningState = RunningState.EXITED;
    } catch (Throwable ex) {
      LOG.warn("Unexpected exception in block pool " + this, ex);
      runningState = RunningState.FAILED;
    } finally {
      LOG.warn("Ending block pool service for: " + this);
      cleanUp();
    }
  }

  private boolean shouldRetryInit() {
    return shouldRun() && bpos.shouldRetryInit();
  }

  private boolean shouldRun() {
    return shouldServiceRun && dn.shouldRun();
  }

  /**
   * Process an array of datanode commands
   * 
   * @param cmds an array of datanode commands
   * @return true if further processing may be required or false otherwise. 
   */
  boolean processCommand(DatanodeCommand[] cmds) {
    if (cmds != null) {
      for (DatanodeCommand cmd : cmds) {
        try {
          if (bpos.processCommandFromActor(cmd, this) == false) {
            return false;
          }
        } catch (IOException ioe) {
          LOG.warn("Error processing datanode Command", ioe);
        }
      }
    }
    return true;
  }


  /**
   * Report a bad block from another DN in this cluster.
   */
  void reportRemoteBadBlock(DatanodeInfo dnInfo, ExtendedBlock block)
      throws IOException {
    LocatedBlock lb = new LocatedBlock(block, 
                                    new DatanodeInfo[] {dnInfo});
    bpNamenode.reportBadBlocks(new LocatedBlock[] {lb});
  }

  void reRegister() throws IOException {
    if (shouldRun()) {
      // re-retrieve namespace info to make sure that, if the NN
      // was restarted, we still match its version (HDFS-2120)
      NamespaceInfo nsInfo = retrieveNamespaceInfo();
      // and re-register
      register(nsInfo);
      scheduler.scheduleHeartbeat();
    }
  }

  private static class PerStoragePendingIncrementalBR {
    private final Map<Long, ReceivedDeletedBlockInfo> pendingIncrementalBR =
        Maps.newHashMap();

    /**
     * Return the number of blocks on this storage that have pending
     * incremental block reports.
     * @return
     */
    int getBlockInfoCount() {
      return pendingIncrementalBR.size();
    }

    /**
     * Dequeue and return all pending incremental block report state.
     * @return
     */
    ReceivedDeletedBlockInfo[] dequeueBlockInfos() {
      ReceivedDeletedBlockInfo[] blockInfos =
          pendingIncrementalBR.values().toArray(
              new ReceivedDeletedBlockInfo[getBlockInfoCount()]);

      pendingIncrementalBR.clear();
      return blockInfos;
    }

    /**
     * Add blocks from blockArray to pendingIncrementalBR, unless the
     * block already exists in pendingIncrementalBR.
     * @param blockArray list of blocks to add.
     * @return the number of missing blocks that we added.
     */
    int putMissingBlockInfos(ReceivedDeletedBlockInfo[] blockArray) {
      int blocksPut = 0;
      for (ReceivedDeletedBlockInfo rdbi : blockArray) {
        if (!pendingIncrementalBR.containsKey(rdbi.getBlock().getBlockId())) {
          pendingIncrementalBR.put(rdbi.getBlock().getBlockId(), rdbi);
          ++blocksPut;
        }
      }
      return blocksPut;
    }

    /**
     * Add pending incremental block report for a single block.
     * @param blockInfo
     */
    void putBlockInfo(ReceivedDeletedBlockInfo blockInfo) {
      pendingIncrementalBR.put(blockInfo.getBlock().getBlockId(), blockInfo);
    }

    /**
     * Remove pending incremental block report for a single block if it
     * exists.
     *
     * @param blockInfo
     * @return true if a report was removed, false if no report existed for
     *         the given block.
     */
    boolean removeBlockInfo(ReceivedDeletedBlockInfo blockInfo) {
      return (pendingIncrementalBR.remove(blockInfo.getBlock().getBlockId()) != null);
    }
  }

  void triggerBlockReport(BlockReportOptions options) throws IOException {
    if (options.isIncremental()) {
      LOG.info(bpos.toString() + ": scheduling an incremental block report.");
      synchronized(pendingIncrementalBRperStorage) {
        sendImmediateIBR = true;
        pendingIncrementalBRperStorage.notifyAll();
      }
    } else {
      LOG.info(bpos.toString() + ": scheduling a full block report.");
      synchronized(pendingIncrementalBRperStorage) {
        scheduler.forceFullBlockReportNow();
        pendingIncrementalBRperStorage.notifyAll();
      }
    }
  }
  
  public void bpThreadEnqueue(BPServiceActorAction action) {
    synchronized (bpThreadQueue) {
      if (!bpThreadQueue.contains(action)) {
        bpThreadQueue.add(action);
      }
    }
  }

  private void processQueueMessages() {
    LinkedList<BPServiceActorAction> duplicateQueue;
    synchronized (bpThreadQueue) {
      duplicateQueue = new LinkedList<BPServiceActorAction>(bpThreadQueue);
      bpThreadQueue.clear();
    }
    while (!duplicateQueue.isEmpty()) {
      BPServiceActorAction actionItem = duplicateQueue.remove();
      try {
        actionItem.reportTo(bpNamenode, bpRegistration);
      } catch (BPServiceActorActionException baae) {
        LOG.warn(baae.getMessage() + nnAddr , baae);
        // Adding it back to the queue if not present
        bpThreadEnqueue(actionItem);
      }
    }
  }

  Scheduler getScheduler() {
    return scheduler;
  }

  /**
   * Utility class that wraps the timestamp computations for scheduling
   * heartbeats and block reports.
   */
  static class Scheduler {
    // nextBlockReportTime and nextHeartbeatTime may be assigned/read
    // by testing threads (through BPServiceActor#triggerXXX), while also
    // assigned/read by the actor thread.
    @VisibleForTesting
    volatile long nextBlockReportTime = monotonicNow();

    @VisibleForTesting
    volatile long nextHeartbeatTime = monotonicNow();

    @VisibleForTesting
    boolean resetBlockReportTime = true;

    private final AtomicBoolean forceFullBlockReport =
        new AtomicBoolean(false);

    private final long heartbeatIntervalMs;
    private final long blockReportIntervalMs;

    Scheduler(long heartbeatIntervalMs, long blockReportIntervalMs) {
      this.heartbeatIntervalMs = heartbeatIntervalMs;
      this.blockReportIntervalMs = blockReportIntervalMs;
    }

    // This is useful to make sure NN gets Heartbeat before Blockreport
    // upon NN restart while DN keeps retrying Otherwise,
    // 1. NN restarts.
    // 2. Heartbeat RPC will retry and succeed. NN asks DN to reregister.
    // 3. After reregistration completes, DN will send Blockreport first.
    // 4. Given NN receives Blockreport after Heartbeat, it won't mark
    //    DatanodeStorageInfo#blockContentsStale to false until the next
    //    Blockreport.
    long scheduleHeartbeat() {
      nextHeartbeatTime = monotonicNow();
      return nextHeartbeatTime;
    }

    long scheduleNextHeartbeat() {
      // Numerical overflow is possible here and is okay.
      nextHeartbeatTime = monotonicNow() + heartbeatIntervalMs;
      return nextHeartbeatTime;
    }

    boolean isHeartbeatDue(long startTime) {
      return (nextHeartbeatTime - startTime <= 0);
    }

    boolean isBlockReportDue(long curTime) {
      return nextBlockReportTime - curTime <= 0;
    }

    void forceFullBlockReportNow() {
      forceFullBlockReport.set(true);
      resetBlockReportTime = true;
    }

    /**
     * This methods  arranges for the data node to send the block report at
     * the next heartbeat.
     */
    long scheduleBlockReport(long delay) {
      if (delay > 0) { // send BR after random delay
        // Numerical overflow is possible here and is okay.
        nextBlockReportTime =
            monotonicNow() + ThreadLocalRandom.current().nextInt((int) (delay));
      } else { // send at next heartbeat
        nextBlockReportTime = monotonicNow();
      }
      resetBlockReportTime = true; // reset future BRs for randomness
      return nextBlockReportTime;
    }

    /**
     * Schedule the next block report after the block report interval. If the
     * current block report was delayed then the next block report is sent per
     * the original schedule.
     * Numerical overflow is possible here.
     */
    void scheduleNextBlockReport() {
      // If we have sent the first set of block reports, then wait a random
      // time before we start the periodic block reports.
      if (resetBlockReportTime) {
        nextBlockReportTime = monotonicNow() +
            ThreadLocalRandom.current().nextInt((int)(blockReportIntervalMs));
        resetBlockReportTime = false;
      } else {
        /* say the last block report was at 8:20:14. The current report
         * should have started around 9:20:14 (default 1 hour interval).
         * If current time is :
         *   1) normal like 9:20:18, next report should be at 10:20:14
         *   2) unexpected like 11:35:43, next report should be at 12:20:14
         */
        nextBlockReportTime +=
              (((monotonicNow() - nextBlockReportTime + blockReportIntervalMs) /
                  blockReportIntervalMs)) * blockReportIntervalMs;
      }
    }

    long getHeartbeatWaitTime() {
      return nextHeartbeatTime - monotonicNow();
    }

    /**
     * Wrapped for testing.
     * @return
     */
    @VisibleForTesting
    public long monotonicNow() {
      return Time.monotonicNow();
    }
  }
}
