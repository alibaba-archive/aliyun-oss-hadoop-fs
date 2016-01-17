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
package org.apache.hadoop.hdfs.protocolPB;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.protobuf.ByteString;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BalancerBandwidthCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockECRecoveryCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockIdCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockRecoveryCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.FinalizeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.KeyUpdateCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.NNHAStatusHeartbeatProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.VolumeFailureSummaryProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportContextProto;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos.BlockECRecoveryInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageUuidsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfosProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageTypesProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.BlockKeyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.BlockWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.BlocksWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.CheckpointCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.CheckpointSignatureProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.ExportedBlockKeysProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamenodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamenodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamenodeRegistrationProto.NamenodeRoleProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamespaceInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.RecoveringBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.RemoteEditLogManifestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.ReplicaStateProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalInfoProto;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockECRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockIdCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockECRecoveryCommand.BlockECRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringStripedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.StripedBlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;

/**
 * Utilities for converting protobuf classes to and from implementation classes
 * and other helper utilities to help in dealing with protobuf.
 * 
 * Note that when converting from an internal type to protobuf type, the
 * converter never return null for protobuf type. The check for internal type
 * being null must be done before calling the convert() method.
 *
 * For those helper methods that convert HDFS client-side data structures from
 * and to protobuf, see {@link PBHelperClient}.
 */
public class PBHelper {
  private static final RegisterCommandProto REG_CMD_PROTO = 
      RegisterCommandProto.newBuilder().build();
  private static final RegisterCommand REG_CMD = new RegisterCommand();

  private PBHelper() {
    /** Hidden constructor */
  }

  public static NamenodeRole convert(NamenodeRoleProto role) {
    switch (role) {
    case NAMENODE:
      return NamenodeRole.NAMENODE;
    case BACKUP:
      return NamenodeRole.BACKUP;
    case CHECKPOINT:
      return NamenodeRole.CHECKPOINT;
    }
    return null;
  }

  public static NamenodeRoleProto convert(NamenodeRole role) {
    switch (role) {
    case NAMENODE:
      return NamenodeRoleProto.NAMENODE;
    case BACKUP:
      return NamenodeRoleProto.BACKUP;
    case CHECKPOINT:
      return NamenodeRoleProto.CHECKPOINT;
    }
    return null;
  }

  public static StorageInfoProto convert(StorageInfo info) {
    return StorageInfoProto.newBuilder().setClusterID(info.getClusterID())
        .setCTime(info.getCTime()).setLayoutVersion(info.getLayoutVersion())
        .setNamespceID(info.getNamespaceID()).build();
  }

  public static StorageInfo convert(StorageInfoProto info, NodeType type) {
    return new StorageInfo(info.getLayoutVersion(), info.getNamespceID(),
        info.getClusterID(), info.getCTime(), type);
  }

  public static NamenodeRegistrationProto convert(NamenodeRegistration reg) {
    return NamenodeRegistrationProto.newBuilder()
        .setHttpAddress(reg.getHttpAddress()).setRole(convert(reg.getRole()))
        .setRpcAddress(reg.getAddress())
        .setStorageInfo(convert((StorageInfo) reg)).build();
  }

  public static NamenodeRegistration convert(NamenodeRegistrationProto reg) {
    StorageInfo si = convert(reg.getStorageInfo(), NodeType.NAME_NODE);
    return new NamenodeRegistration(reg.getRpcAddress(), reg.getHttpAddress(),
        si, convert(reg.getRole()));
  }

  public static BlockWithLocationsProto convert(BlockWithLocations blk) {
    BlockWithLocationsProto.Builder builder = BlockWithLocationsProto
        .newBuilder().setBlock(PBHelperClient.convert(blk.getBlock()))
        .addAllDatanodeUuids(Arrays.asList(blk.getDatanodeUuids()))
        .addAllStorageUuids(Arrays.asList(blk.getStorageIDs()))
        .addAllStorageTypes(PBHelperClient.convertStorageTypes(blk.getStorageTypes()));
    if (blk instanceof StripedBlockWithLocations) {
      StripedBlockWithLocations sblk = (StripedBlockWithLocations) blk;
      builder.setIndices(PBHelperClient.getByteString(sblk.getIndices()));
      builder.setDataBlockNum(sblk.getDataBlockNum());
      builder.setCellSize(sblk.getCellSize());
    }
    return builder.build();
  }

  public static BlockWithLocations convert(BlockWithLocationsProto b) {
    final List<String> datanodeUuids = b.getDatanodeUuidsList();
    final List<String> storageUuids = b.getStorageUuidsList();
    final List<StorageTypeProto> storageTypes = b.getStorageTypesList();
    BlockWithLocations blk = new BlockWithLocations(PBHelperClient.
        convert(b.getBlock()),
        datanodeUuids.toArray(new String[datanodeUuids.size()]),
        storageUuids.toArray(new String[storageUuids.size()]),
        PBHelperClient.convertStorageTypes(storageTypes, storageUuids.size()));
    if (b.hasIndices()) {
      blk = new StripedBlockWithLocations(blk, b.getIndices().toByteArray(),
          (short) b.getDataBlockNum(), b.getCellSize());
    }
    return blk;
  }

  public static BlocksWithLocationsProto convert(BlocksWithLocations blks) {
    BlocksWithLocationsProto.Builder builder = BlocksWithLocationsProto
        .newBuilder();
    for (BlockWithLocations b : blks.getBlocks()) {
      builder.addBlocks(convert(b));
    }
    return builder.build();
  }

  public static BlocksWithLocations convert(BlocksWithLocationsProto blocks) {
    List<BlockWithLocationsProto> b = blocks.getBlocksList();
    BlockWithLocations[] ret = new BlockWithLocations[b.size()];
    int i = 0;
    for (BlockWithLocationsProto entry : b) {
      ret[i++] = convert(entry);
    }
    return new BlocksWithLocations(ret);
  }

  public static BlockKeyProto convert(BlockKey key) {
    byte[] encodedKey = key.getEncodedKey();
    ByteString keyBytes = PBHelperClient.getByteString(encodedKey == null ?
        DFSUtilClient.EMPTY_BYTES : encodedKey);
    return BlockKeyProto.newBuilder().setKeyId(key.getKeyId())
        .setKeyBytes(keyBytes).setExpiryDate(key.getExpiryDate()).build();
  }

  public static BlockKey convert(BlockKeyProto k) {
    return new BlockKey(k.getKeyId(), k.getExpiryDate(), k.getKeyBytes()
        .toByteArray());
  }

  public static ExportedBlockKeysProto convert(ExportedBlockKeys keys) {
    ExportedBlockKeysProto.Builder builder = ExportedBlockKeysProto
        .newBuilder();
    builder.setIsBlockTokenEnabled(keys.isBlockTokenEnabled())
        .setKeyUpdateInterval(keys.getKeyUpdateInterval())
        .setTokenLifeTime(keys.getTokenLifetime())
        .setCurrentKey(convert(keys.getCurrentKey()));
    for (BlockKey k : keys.getAllKeys()) {
      builder.addAllKeys(convert(k));
    }
    return builder.build();
  }

  public static ExportedBlockKeys convert(ExportedBlockKeysProto keys) {
    return new ExportedBlockKeys(keys.getIsBlockTokenEnabled(),
        keys.getKeyUpdateInterval(), keys.getTokenLifeTime(),
        convert(keys.getCurrentKey()), convertBlockKeys(keys.getAllKeysList()));
  }

  public static CheckpointSignatureProto convert(CheckpointSignature s) {
    return CheckpointSignatureProto.newBuilder()
        .setBlockPoolId(s.getBlockpoolID())
        .setCurSegmentTxId(s.getCurSegmentTxId())
        .setMostRecentCheckpointTxId(s.getMostRecentCheckpointTxId())
        .setStorageInfo(PBHelper.convert((StorageInfo) s)).build();
  }

  public static CheckpointSignature convert(CheckpointSignatureProto s) {
    StorageInfo si = PBHelper.convert(s.getStorageInfo(), NodeType.NAME_NODE);
    return new CheckpointSignature(si, s.getBlockPoolId(),
        s.getMostRecentCheckpointTxId(), s.getCurSegmentTxId());
  }

  public static RemoteEditLogProto convert(RemoteEditLog log) {
    return RemoteEditLogProto.newBuilder()
        .setStartTxId(log.getStartTxId())
        .setEndTxId(log.getEndTxId())
        .setIsInProgress(log.isInProgress()).build();
  }

  public static RemoteEditLog convert(RemoteEditLogProto l) {
    return new RemoteEditLog(l.getStartTxId(), l.getEndTxId(),
        l.getIsInProgress());
  }

  public static RemoteEditLogManifestProto convert(
      RemoteEditLogManifest manifest) {
    RemoteEditLogManifestProto.Builder builder = RemoteEditLogManifestProto
        .newBuilder();
    for (RemoteEditLog log : manifest.getLogs()) {
      builder.addLogs(convert(log));
    }
    return builder.build();
  }

  public static RemoteEditLogManifest convert(
      RemoteEditLogManifestProto manifest) {
    List<RemoteEditLog> logs = new ArrayList<RemoteEditLog>(manifest
        .getLogsList().size());
    for (RemoteEditLogProto l : manifest.getLogsList()) {
      logs.add(convert(l));
    }
    return new RemoteEditLogManifest(logs);
  }

  public static CheckpointCommandProto convert(CheckpointCommand cmd) {
    return CheckpointCommandProto.newBuilder()
        .setSignature(convert(cmd.getSignature()))
        .setNeedToReturnImage(cmd.needToReturnImage()).build();
  }

  public static NamenodeCommandProto convert(NamenodeCommand cmd) {
    if (cmd instanceof CheckpointCommand) {
      return NamenodeCommandProto.newBuilder().setAction(cmd.getAction())
          .setType(NamenodeCommandProto.Type.CheckPointCommand)
          .setCheckpointCmd(convert((CheckpointCommand) cmd)).build();
    }
    return NamenodeCommandProto.newBuilder()
        .setType(NamenodeCommandProto.Type.NamenodeCommand)
        .setAction(cmd.getAction()).build();
  }

  public static BlockKey[] convertBlockKeys(List<BlockKeyProto> list) {
    BlockKey[] ret = new BlockKey[list.size()];
    int i = 0;
    for (BlockKeyProto k : list) {
      ret[i++] = convert(k);
    }
    return ret;
  }

  public static NamespaceInfo convert(NamespaceInfoProto info) {
    StorageInfoProto storage = info.getStorageInfo();
    return new NamespaceInfo(storage.getNamespceID(), storage.getClusterID(),
        info.getBlockPoolID(), storage.getCTime(), info.getBuildVersion(),
        info.getSoftwareVersion(), info.getCapabilities());
  }

  public static NamenodeCommand convert(NamenodeCommandProto cmd) {
    if (cmd == null) return null;
    switch (cmd.getType()) {
    case CheckPointCommand:
      CheckpointCommandProto chkPt = cmd.getCheckpointCmd();
      return new CheckpointCommand(PBHelper.convert(chkPt.getSignature()),
          chkPt.getNeedToReturnImage());
    default:
      return new NamenodeCommand(cmd.getAction());
    }
  }

  public static RecoveringBlockProto convert(RecoveringBlock b) {
    if (b == null) {
      return null;
    }
    LocatedBlockProto lb = PBHelperClient.convertLocatedBlock(b);
    RecoveringBlockProto.Builder builder = RecoveringBlockProto.newBuilder();
    builder.setBlock(lb).setNewGenStamp(b.getNewGenerationStamp());
    if(b.getNewBlock() != null)
      builder.setTruncateBlock(PBHelperClient.convert(b.getNewBlock()));
    if (b instanceof RecoveringStripedBlock) {
      RecoveringStripedBlock sb = (RecoveringStripedBlock) b;
      builder.setEcPolicy(PBHelperClient.convertErasureCodingPolicy(
          sb.getErasureCodingPolicy()));
      builder.setBlockIndices(PBHelperClient.getByteString(sb.getBlockIndices()));
    }
    return builder.build();
  }

  public static RecoveringBlock convert(RecoveringBlockProto b) {
    LocatedBlock lb = PBHelperClient.convertLocatedBlockProto(b.getBlock());
    RecoveringBlock rBlock;
    if (b.hasTruncateBlock()) {
      rBlock = new RecoveringBlock(lb.getBlock(), lb.getLocations(),
          PBHelperClient.convert(b.getTruncateBlock()));
    } else {
      rBlock = new RecoveringBlock(lb.getBlock(), lb.getLocations(),
          b.getNewGenStamp());
    }

    if (b.hasEcPolicy()) {
      assert b.hasBlockIndices();
      byte[] indices = b.getBlockIndices().toByteArray();
      rBlock = new RecoveringStripedBlock(rBlock, indices,
          PBHelperClient.convertErasureCodingPolicy(b.getEcPolicy()));
    }
    return rBlock;
  }

  public static ReplicaState convert(ReplicaStateProto state) {
    switch (state) {
    case RBW:
      return ReplicaState.RBW;
    case RUR:
      return ReplicaState.RUR;
    case RWR:
      return ReplicaState.RWR;
    case TEMPORARY:
      return ReplicaState.TEMPORARY;
    case FINALIZED:
    default:
      return ReplicaState.FINALIZED;
    }
  }

  public static ReplicaStateProto convert(ReplicaState state) {
    switch (state) {
    case RBW:
      return ReplicaStateProto.RBW;
    case RUR:
      return ReplicaStateProto.RUR;
    case RWR:
      return ReplicaStateProto.RWR;
    case TEMPORARY:
      return ReplicaStateProto.TEMPORARY;
    case FINALIZED:
    default:
      return ReplicaStateProto.FINALIZED;
    }
  }
  
  public static DatanodeRegistrationProto convert(
      DatanodeRegistration registration) {
    DatanodeRegistrationProto.Builder builder = DatanodeRegistrationProto
        .newBuilder();
    return builder.setDatanodeID(PBHelperClient.convert((DatanodeID) registration))
        .setStorageInfo(convert(registration.getStorageInfo()))
        .setKeys(convert(registration.getExportedKeys()))
        .setSoftwareVersion(registration.getSoftwareVersion()).build();
  }

  public static DatanodeRegistration convert(DatanodeRegistrationProto proto) {
    StorageInfo si = convert(proto.getStorageInfo(), NodeType.DATA_NODE);
    return new DatanodeRegistration(PBHelperClient.convert(proto.getDatanodeID()),
        si, convert(proto.getKeys()), proto.getSoftwareVersion());
  }

  public static DatanodeCommand convert(DatanodeCommandProto proto) {
    switch (proto.getCmdType()) {
    case BalancerBandwidthCommand:
      return PBHelper.convert(proto.getBalancerCmd());
    case BlockCommand:
      return PBHelper.convert(proto.getBlkCmd());
    case BlockRecoveryCommand:
      return PBHelper.convert(proto.getRecoveryCmd());
    case FinalizeCommand:
      return PBHelper.convert(proto.getFinalizeCmd());
    case KeyUpdateCommand:
      return PBHelper.convert(proto.getKeyUpdateCmd());
    case RegisterCommand:
      return REG_CMD;
    case BlockIdCommand:
      return PBHelper.convert(proto.getBlkIdCmd());
    case BlockECRecoveryCommand:
      return PBHelper.convert(proto.getBlkECRecoveryCmd());
    default:
      return null;
    }
  }
  
  public static BalancerBandwidthCommandProto convert(
      BalancerBandwidthCommand bbCmd) {
    return BalancerBandwidthCommandProto.newBuilder()
        .setBandwidth(bbCmd.getBalancerBandwidthValue()).build();
  }

  public static KeyUpdateCommandProto convert(KeyUpdateCommand cmd) {
    return KeyUpdateCommandProto.newBuilder()
        .setKeys(convert(cmd.getExportedKeys())).build();
  }

  public static BlockRecoveryCommandProto convert(BlockRecoveryCommand cmd) {
    BlockRecoveryCommandProto.Builder builder = BlockRecoveryCommandProto
        .newBuilder();
    for (RecoveringBlock b : cmd.getRecoveringBlocks()) {
      builder.addBlocks(PBHelper.convert(b));
    }
    return builder.build();
  }

  public static FinalizeCommandProto convert(FinalizeCommand cmd) {
    return FinalizeCommandProto.newBuilder()
        .setBlockPoolId(cmd.getBlockPoolId()).build();
  }

  public static BlockCommandProto convert(BlockCommand cmd) {
    BlockCommandProto.Builder builder = BlockCommandProto.newBuilder()
        .setBlockPoolId(cmd.getBlockPoolId());
    switch (cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      builder.setAction(BlockCommandProto.Action.TRANSFER);
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      builder.setAction(BlockCommandProto.Action.INVALIDATE);
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      builder.setAction(BlockCommandProto.Action.SHUTDOWN);
      break;
    default:
      throw new AssertionError("Invalid action");
    }
    Block[] blocks = cmd.getBlocks();
    for (int i = 0; i < blocks.length; i++) {
      builder.addBlocks(PBHelperClient.convert(blocks[i]));
    }
    builder.addAllTargets(PBHelperClient.convert(cmd.getTargets()))
           .addAllTargetStorageUuids(convert(cmd.getTargetStorageIDs()));
    StorageType[][] types = cmd.getTargetStorageTypes();
    if (types != null) {
      builder.addAllTargetStorageTypes(PBHelperClient.convert(types));
    }
    return builder.build();
  }

  public static BlockIdCommandProto convert(BlockIdCommand cmd) {
    BlockIdCommandProto.Builder builder = BlockIdCommandProto.newBuilder()
        .setBlockPoolId(cmd.getBlockPoolId());
    switch (cmd.getAction()) {
    case DatanodeProtocol.DNA_CACHE:
      builder.setAction(BlockIdCommandProto.Action.CACHE);
      break;
    case DatanodeProtocol.DNA_UNCACHE:
      builder.setAction(BlockIdCommandProto.Action.UNCACHE);
      break;
    default:
      throw new AssertionError("Invalid action");
    }
    long[] blockIds = cmd.getBlockIds();
    for (int i = 0; i < blockIds.length; i++) {
      builder.addBlockIds(blockIds[i]);
    }
    return builder.build();
  }

  private static List<StorageUuidsProto> convert(String[][] targetStorageUuids) {
    StorageUuidsProto[] ret = new StorageUuidsProto[targetStorageUuids.length];
    for (int i = 0; i < targetStorageUuids.length; i++) {
      ret[i] = StorageUuidsProto.newBuilder()
          .addAllStorageUuids(Arrays.asList(targetStorageUuids[i])).build();
    }
    return Arrays.asList(ret);
  }

  public static DatanodeCommandProto convert(DatanodeCommand datanodeCommand) {
    DatanodeCommandProto.Builder builder = DatanodeCommandProto.newBuilder();
    if (datanodeCommand == null) {
      return builder.setCmdType(DatanodeCommandProto.Type.NullDatanodeCommand)
          .build();
    }
    switch (datanodeCommand.getAction()) {
    case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
      builder.setCmdType(DatanodeCommandProto.Type.BalancerBandwidthCommand)
          .setBalancerCmd(
              PBHelper.convert((BalancerBandwidthCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
      builder
          .setCmdType(DatanodeCommandProto.Type.KeyUpdateCommand)
          .setKeyUpdateCmd(PBHelper.convert((KeyUpdateCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      builder.setCmdType(DatanodeCommandProto.Type.BlockRecoveryCommand)
          .setRecoveryCmd(
              PBHelper.convert((BlockRecoveryCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      builder.setCmdType(DatanodeCommandProto.Type.FinalizeCommand)
          .setFinalizeCmd(PBHelper.convert((FinalizeCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_REGISTER:
      builder.setCmdType(DatanodeCommandProto.Type.RegisterCommand)
          .setRegisterCmd(REG_CMD_PROTO);
      break;
    case DatanodeProtocol.DNA_TRANSFER:
    case DatanodeProtocol.DNA_INVALIDATE:
    case DatanodeProtocol.DNA_SHUTDOWN:
      builder.setCmdType(DatanodeCommandProto.Type.BlockCommand).
        setBlkCmd(PBHelper.convert((BlockCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_CACHE:
    case DatanodeProtocol.DNA_UNCACHE:
      builder.setCmdType(DatanodeCommandProto.Type.BlockIdCommand).
        setBlkIdCmd(PBHelper.convert((BlockIdCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_ERASURE_CODING_RECOVERY:
      builder.setCmdType(DatanodeCommandProto.Type.BlockECRecoveryCommand)
          .setBlkECRecoveryCmd(
              convert((BlockECRecoveryCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_UNKNOWN: //Not expected
    default:
      builder.setCmdType(DatanodeCommandProto.Type.NullDatanodeCommand);
    }
    return builder.build();
  }

  public static KeyUpdateCommand convert(KeyUpdateCommandProto keyUpdateCmd) {
    return new KeyUpdateCommand(convert(keyUpdateCmd.getKeys()));
  }

  public static FinalizeCommand convert(FinalizeCommandProto finalizeCmd) {
    return new FinalizeCommand(finalizeCmd.getBlockPoolId());
  }

  public static BlockRecoveryCommand convert(
      BlockRecoveryCommandProto recoveryCmd) {
    List<RecoveringBlockProto> list = recoveryCmd.getBlocksList();
    List<RecoveringBlock> recoveringBlocks = new ArrayList<RecoveringBlock>(
        list.size());
    
    for (RecoveringBlockProto rbp : list) {
      recoveringBlocks.add(PBHelper.convert(rbp));
    }
    return new BlockRecoveryCommand(recoveringBlocks);
  }

  public static BlockCommand convert(BlockCommandProto blkCmd) {
    List<BlockProto> blockProtoList = blkCmd.getBlocksList();
    Block[] blocks = new Block[blockProtoList.size()];
    for (int i = 0; i < blockProtoList.size(); i++) {
      blocks[i] = PBHelperClient.convert(blockProtoList.get(i));
    }
    List<DatanodeInfosProto> targetList = blkCmd.getTargetsList();
    DatanodeInfo[][] targets = new DatanodeInfo[targetList.size()][];
    for (int i = 0; i < targetList.size(); i++) {
      targets[i] = PBHelperClient.convert(targetList.get(i));
    }

    StorageType[][] targetStorageTypes = new StorageType[targetList.size()][];
    List<StorageTypesProto> targetStorageTypesList = blkCmd.getTargetStorageTypesList();
    if (targetStorageTypesList.isEmpty()) { // missing storage types
      for(int i = 0; i < targetStorageTypes.length; i++) {
        targetStorageTypes[i] = new StorageType[targets[i].length];
        Arrays.fill(targetStorageTypes[i], StorageType.DEFAULT);
      }
    } else {
      for(int i = 0; i < targetStorageTypes.length; i++) {
        List<StorageTypeProto> p = targetStorageTypesList.get(i).getStorageTypesList();
        targetStorageTypes[i] = PBHelperClient.convertStorageTypes(p, targets[i].length);
      }
    }

    List<StorageUuidsProto> targetStorageUuidsList = blkCmd.getTargetStorageUuidsList();
    String[][] targetStorageIDs = new String[targetStorageUuidsList.size()][];
    for(int i = 0; i < targetStorageIDs.length; i++) {
      List<String> storageIDs = targetStorageUuidsList.get(i).getStorageUuidsList();
      targetStorageIDs[i] = storageIDs.toArray(new String[storageIDs.size()]);
    }

    int action = DatanodeProtocol.DNA_UNKNOWN;
    switch (blkCmd.getAction()) {
    case TRANSFER:
      action = DatanodeProtocol.DNA_TRANSFER;
      break;
    case INVALIDATE:
      action = DatanodeProtocol.DNA_INVALIDATE;
      break;
    case SHUTDOWN:
      action = DatanodeProtocol.DNA_SHUTDOWN;
      break;
    default:
      throw new AssertionError("Unknown action type: " + blkCmd.getAction());
    }
    return new BlockCommand(action, blkCmd.getBlockPoolId(), blocks, targets,
        targetStorageTypes, targetStorageIDs);
  }

  public static BlockIdCommand convert(BlockIdCommandProto blkIdCmd) {
    int numBlockIds = blkIdCmd.getBlockIdsCount();
    long blockIds[] = new long[numBlockIds];
    for (int i = 0; i < numBlockIds; i++) {
      blockIds[i] = blkIdCmd.getBlockIds(i);
    }
    int action = DatanodeProtocol.DNA_UNKNOWN;
    switch (blkIdCmd.getAction()) {
    case CACHE:
      action = DatanodeProtocol.DNA_CACHE;
      break;
    case UNCACHE:
      action = DatanodeProtocol.DNA_UNCACHE;
      break;
    default:
      throw new AssertionError("Unknown action type: " + blkIdCmd.getAction());
    }
    return new BlockIdCommand(action, blkIdCmd.getBlockPoolId(), blockIds);
  }

  public static BalancerBandwidthCommand convert(
      BalancerBandwidthCommandProto balancerCmd) {
    return new BalancerBandwidthCommand(balancerCmd.getBandwidth());
  }

  public static ReceivedDeletedBlockInfoProto convert(
      ReceivedDeletedBlockInfo receivedDeletedBlockInfo) {
    ReceivedDeletedBlockInfoProto.Builder builder = 
        ReceivedDeletedBlockInfoProto.newBuilder();
    
    ReceivedDeletedBlockInfoProto.BlockStatus status;
    switch (receivedDeletedBlockInfo.getStatus()) {
    case RECEIVING_BLOCK:
      status = ReceivedDeletedBlockInfoProto.BlockStatus.RECEIVING;
      break;
    case RECEIVED_BLOCK:
      status = ReceivedDeletedBlockInfoProto.BlockStatus.RECEIVED;
      break;
    case DELETED_BLOCK:
      status = ReceivedDeletedBlockInfoProto.BlockStatus.DELETED;
      break;
    default:
      throw new IllegalArgumentException("Bad status: " +
          receivedDeletedBlockInfo.getStatus());
    }
    builder.setStatus(status);
    
    if (receivedDeletedBlockInfo.getDelHints() != null) {
      builder.setDeleteHint(receivedDeletedBlockInfo.getDelHints());
    }
    return builder.setBlock(
        PBHelperClient.convert(receivedDeletedBlockInfo.getBlock())).build();
  }

  public static ReceivedDeletedBlockInfo convert(
      ReceivedDeletedBlockInfoProto proto) {
    ReceivedDeletedBlockInfo.BlockStatus status = null;
    switch (proto.getStatus()) {
    case RECEIVING:
      status = BlockStatus.RECEIVING_BLOCK;
      break;
    case RECEIVED:
      status = BlockStatus.RECEIVED_BLOCK;
      break;
    case DELETED:
      status = BlockStatus.DELETED_BLOCK;
      break;
    }
    return new ReceivedDeletedBlockInfo(
        PBHelperClient.convert(proto.getBlock()),
        status,
        proto.hasDeleteHint() ? proto.getDeleteHint() : null);
  }
  
  public static NamespaceInfoProto convert(NamespaceInfo info) {
    return NamespaceInfoProto.newBuilder()
        .setBlockPoolID(info.getBlockPoolID())
        .setBuildVersion(info.getBuildVersion())
        .setUnused(0)
        .setStorageInfo(PBHelper.convert((StorageInfo)info))
        .setSoftwareVersion(info.getSoftwareVersion())
        .setCapabilities(info.getCapabilities())
        .build();
  }

  public static NNHAStatusHeartbeat convert(NNHAStatusHeartbeatProto s) {
    if (s == null) return null;
    switch (s.getState()) {
    case ACTIVE:
      return new NNHAStatusHeartbeat(HAServiceState.ACTIVE, s.getTxid());
    case STANDBY:
      return new NNHAStatusHeartbeat(HAServiceState.STANDBY, s.getTxid());
    default:
      throw new IllegalArgumentException("Unexpected NNHAStatusHeartbeat.State:" + s.getState());
    }
  }

  public static NNHAStatusHeartbeatProto convert(NNHAStatusHeartbeat hb) {
    if (hb == null) return null;
    NNHAStatusHeartbeatProto.Builder builder =
      NNHAStatusHeartbeatProto.newBuilder();
    switch (hb.getState()) {
      case ACTIVE:
        builder.setState(HAServiceProtocolProtos.HAServiceStateProto.ACTIVE);
        break;
      case STANDBY:
        builder.setState(HAServiceProtocolProtos.HAServiceStateProto.STANDBY);
        break;
      default:
        throw new IllegalArgumentException("Unexpected NNHAStatusHeartbeat.State:" +
            hb.getState());
    }
    builder.setTxid(hb.getTxId());
    return builder.build();
  }

  public static VolumeFailureSummary convertVolumeFailureSummary(
      VolumeFailureSummaryProto proto) {
    List<String> failedStorageLocations = proto.getFailedStorageLocationsList();
    return new VolumeFailureSummary(
        failedStorageLocations.toArray(new String[failedStorageLocations.size()]),
        proto.getLastVolumeFailureDate(), proto.getEstimatedCapacityLostTotal());
  }

  public static VolumeFailureSummaryProto convertVolumeFailureSummary(
      VolumeFailureSummary volumeFailureSummary) {
    VolumeFailureSummaryProto.Builder builder =
        VolumeFailureSummaryProto.newBuilder();
    for (String failedStorageLocation:
        volumeFailureSummary.getFailedStorageLocations()) {
      builder.addFailedStorageLocations(failedStorageLocation);
    }
    builder.setLastVolumeFailureDate(
        volumeFailureSummary.getLastVolumeFailureDate());
    builder.setEstimatedCapacityLostTotal(
        volumeFailureSummary.getEstimatedCapacityLostTotal());
    return builder.build();
  }

  public static JournalInfo convert(JournalInfoProto info) {
    int lv = info.hasLayoutVersion() ? info.getLayoutVersion() : 0;
    int nsID = info.hasNamespaceID() ? info.getNamespaceID() : 0;
    return new JournalInfo(lv, info.getClusterID(), nsID);
  }

  /**
   * Method used for converting {@link JournalInfoProto} sent from Namenode
   * to Journal receivers to {@link NamenodeRegistration}.
   */
  public static JournalInfoProto convert(JournalInfo j) {
    return JournalInfoProto.newBuilder().setClusterID(j.getClusterId())
        .setLayoutVersion(j.getLayoutVersion())
        .setNamespaceID(j.getNamespaceId()).build();
  }


  public static BlockReportContext convert(BlockReportContextProto proto) {
    return new BlockReportContext(proto.getTotalRpcs(),
        proto.getCurRpc(), proto.getId(), proto.getLeaseId());
  }

  public static BlockReportContextProto convert(BlockReportContext context) {
    return BlockReportContextProto.newBuilder().
        setTotalRpcs(context.getTotalRpcs()).
        setCurRpc(context.getCurRpc()).
        setId(context.getReportId()).
        setLeaseId(context.getLeaseId()).
        build();
  }

  private static StorageTypesProto convertStorageTypesProto(
      StorageType[] targetStorageTypes) {
    StorageTypesProto.Builder builder = StorageTypesProto.newBuilder();
    for (StorageType storageType : targetStorageTypes) {
      builder.addStorageTypes(PBHelperClient.convertStorageType(storageType));
    }
    return builder.build();
  }

  private static HdfsProtos.StorageUuidsProto convertStorageIDs(String[] targetStorageIDs) {
    HdfsProtos.StorageUuidsProto.Builder builder = HdfsProtos.StorageUuidsProto.newBuilder();
    for (String storageUuid : targetStorageIDs) {
      builder.addStorageUuids(storageUuid);
    }
    return builder.build();
  }

  private static DatanodeInfosProto convertToDnInfosProto(DatanodeInfo[] dnInfos) {
    DatanodeInfosProto.Builder builder = DatanodeInfosProto.newBuilder();
    for (DatanodeInfo datanodeInfo : dnInfos) {
      builder.addDatanodes(PBHelperClient.convert(datanodeInfo));
    }
    return builder.build();
  }

  private static String[] convert(HdfsProtos.StorageUuidsProto targetStorageUuidsProto) {
    List<String> storageUuidsList = targetStorageUuidsProto
        .getStorageUuidsList();
    String[] storageUuids = new String[storageUuidsList.size()];
    for (int i = 0; i < storageUuidsList.size(); i++) {
      storageUuids[i] = storageUuidsList.get(i);
    }
    return storageUuids;
  }

  public static BlockECRecoveryInfo convertBlockECRecoveryInfo(
      BlockECRecoveryInfoProto blockEcRecoveryInfoProto) {
    ExtendedBlockProto blockProto = blockEcRecoveryInfoProto.getBlock();
    ExtendedBlock block = PBHelperClient.convert(blockProto);

    DatanodeInfosProto sourceDnInfosProto = blockEcRecoveryInfoProto
        .getSourceDnInfos();
    DatanodeInfo[] sourceDnInfos = PBHelperClient.convert(sourceDnInfosProto);

    DatanodeInfosProto targetDnInfosProto = blockEcRecoveryInfoProto
        .getTargetDnInfos();
    DatanodeInfo[] targetDnInfos = PBHelperClient.convert(targetDnInfosProto);

    HdfsProtos.StorageUuidsProto targetStorageUuidsProto = blockEcRecoveryInfoProto
        .getTargetStorageUuids();
    String[] targetStorageUuids = convert(targetStorageUuidsProto);

    StorageTypesProto targetStorageTypesProto = blockEcRecoveryInfoProto
        .getTargetStorageTypes();
    StorageType[] convertStorageTypes = PBHelperClient.convertStorageTypes(
        targetStorageTypesProto.getStorageTypesList(), targetStorageTypesProto
            .getStorageTypesList().size());

    byte[] liveBlkIndices = blockEcRecoveryInfoProto.getLiveBlockIndices()
        .toByteArray();
    ErasureCodingPolicy ecPolicy =
        PBHelperClient.convertErasureCodingPolicy(
            blockEcRecoveryInfoProto.getEcPolicy());
    return new BlockECRecoveryInfo(block, sourceDnInfos, targetDnInfos,
        targetStorageUuids, convertStorageTypes, liveBlkIndices, ecPolicy);
  }

  public static BlockECRecoveryInfoProto convertBlockECRecoveryInfo(
      BlockECRecoveryInfo blockEcRecoveryInfo) {
    BlockECRecoveryInfoProto.Builder builder = BlockECRecoveryInfoProto
        .newBuilder();
    builder.setBlock(PBHelperClient.convert(
        blockEcRecoveryInfo.getExtendedBlock()));

    DatanodeInfo[] sourceDnInfos = blockEcRecoveryInfo.getSourceDnInfos();
    builder.setSourceDnInfos(convertToDnInfosProto(sourceDnInfos));

    DatanodeInfo[] targetDnInfos = blockEcRecoveryInfo.getTargetDnInfos();
    builder.setTargetDnInfos(convertToDnInfosProto(targetDnInfos));

    String[] targetStorageIDs = blockEcRecoveryInfo.getTargetStorageIDs();
    builder.setTargetStorageUuids(convertStorageIDs(targetStorageIDs));

    StorageType[] targetStorageTypes = blockEcRecoveryInfo
        .getTargetStorageTypes();
    builder.setTargetStorageTypes(convertStorageTypesProto(targetStorageTypes));

    byte[] liveBlockIndices = blockEcRecoveryInfo.getLiveBlockIndices();
    builder.setLiveBlockIndices(PBHelperClient.getByteString(liveBlockIndices));

    builder.setEcPolicy(PBHelperClient.convertErasureCodingPolicy(
        blockEcRecoveryInfo.getErasureCodingPolicy()));

    return builder.build();
  }

  public static BlockECRecoveryCommandProto convert(
      BlockECRecoveryCommand blkECRecoveryCmd) {
    BlockECRecoveryCommandProto.Builder builder = BlockECRecoveryCommandProto
        .newBuilder();
    Collection<BlockECRecoveryInfo> blockECRecoveryInfos = blkECRecoveryCmd
        .getECTasks();
    for (BlockECRecoveryInfo blkECRecoveryInfo : blockECRecoveryInfos) {
      builder
          .addBlockECRecoveryinfo(convertBlockECRecoveryInfo(blkECRecoveryInfo));
    }
    return builder.build();
  }

  public static BlockECRecoveryCommand convert(
      BlockECRecoveryCommandProto blkECRecoveryCmdProto) {
    Collection<BlockECRecoveryInfo> blkECRecoveryInfos = new ArrayList<>();
    List<BlockECRecoveryInfoProto> blockECRecoveryinfoList = blkECRecoveryCmdProto
        .getBlockECRecoveryinfoList();
    for (BlockECRecoveryInfoProto blockECRecoveryInfoProto : blockECRecoveryinfoList) {
      blkECRecoveryInfos
          .add(convertBlockECRecoveryInfo(blockECRecoveryInfoProto));
    }
    return new BlockECRecoveryCommand(DatanodeProtocol.DNA_ERASURE_CODING_RECOVERY,
        blkECRecoveryInfos);
  }
}
