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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.NO_SNAPSHOT_ID;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.LongBitFormat;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/** I-node for closed file. */
@InterfaceAudience.Private
public class INodeFile extends INodeWithAdditionalFields
    implements INodeFileAttributes, BlockCollection {

  /** The same as valueOf(inode, path, false). */
  public static INodeFile valueOf(INode inode, String path
      ) throws FileNotFoundException {
    return valueOf(inode, path, false);
  }

  /** Cast INode to INodeFile. */
  public static INodeFile valueOf(INode inode, String path, boolean acceptNull)
      throws FileNotFoundException {
    if (inode == null) {
      if (acceptNull) {
        return null;
      } else {
        throw new FileNotFoundException("File does not exist: " + path);
      }
    }
    if (!inode.isFile()) {
      throw new FileNotFoundException("Path is not a file: " + path);
    }
    return inode.asFile();
  }

  /** 
   * Bit format:
   * [4-bit storagePolicyID][1-bit isStriped]
   * [11-bit replication][48-bit preferredBlockSize]
   */
  enum HeaderFormat {
    PREFERRED_BLOCK_SIZE(null, 48, 1),
    REPLICATION(PREFERRED_BLOCK_SIZE.BITS, 11, 0),
    IS_STRIPED(REPLICATION.BITS, 1, 0),
    STORAGE_POLICY_ID(IS_STRIPED.BITS, BlockStoragePolicySuite.ID_BIT_LENGTH,
        0);

    private final LongBitFormat BITS;

    HeaderFormat(LongBitFormat previous, int length, long min) {
      BITS = new LongBitFormat(name(), previous, length, min);
    }

    static short getReplication(long header) {
      return (short)REPLICATION.BITS.retrieve(header);
    }

    static long getPreferredBlockSize(long header) {
      return PREFERRED_BLOCK_SIZE.BITS.retrieve(header);
    }

    static byte getStoragePolicyID(long header) {
      return (byte)STORAGE_POLICY_ID.BITS.retrieve(header);
    }

    static boolean isStriped(long header) {
      long isStriped = IS_STRIPED.BITS.retrieve(header);
      Preconditions.checkState(isStriped == 0 || isStriped == 1);
      return isStriped == 1;
    }

    static long toLong(long preferredBlockSize, short replication,
        boolean isStriped, byte storagePolicyID) {
      long h = 0;
      if (preferredBlockSize == 0) {
        preferredBlockSize = PREFERRED_BLOCK_SIZE.BITS.getMin();
      }
      h = PREFERRED_BLOCK_SIZE.BITS.combine(preferredBlockSize, h);
      // Replication factor for striped files is zero
      if (isStriped) {
        h = REPLICATION.BITS.combine(0L, h);
        h = IS_STRIPED.BITS.combine(1L, h);
      } else {
        h = REPLICATION.BITS.combine(replication, h);
        h = IS_STRIPED.BITS.combine(0L, h);
      }
      h = STORAGE_POLICY_ID.BITS.combine(storagePolicyID, h);
      return h;
    }

  }

  private long header = 0L;

  private BlockInfo[] blocks;

  INodeFile(long id, byte[] name, PermissionStatus permissions, long mtime,
            long atime, BlockInfo[] blklist, short replication,
            long preferredBlockSize) {
    this(id, name, permissions, mtime, atime, blklist, replication,
        preferredBlockSize, (byte) 0, false);
  }

  INodeFile(long id, byte[] name, PermissionStatus permissions, long mtime,
      long atime, BlockInfo[] blklist, short replication,
      long preferredBlockSize, byte storagePolicyID, boolean isStriped) {
    super(id, name, permissions, mtime, atime);
    header = HeaderFormat.toLong(preferredBlockSize, replication, isStriped,
        storagePolicyID);
    if (blklist != null && blklist.length > 0) {
      for (BlockInfo b : blklist) {
        Preconditions.checkArgument(b.isStriped() == isStriped);
      }
    }
    setBlocks(blklist);
  }
  
  public INodeFile(INodeFile that) {
    super(that);
    this.header = that.header;
    this.blocks = that.blocks;
    this.features = that.features;
  }
  
  public INodeFile(INodeFile that, FileDiffList diffs) {
    this(that);
    Preconditions.checkArgument(!that.isWithSnapshot());
    this.addSnapshotFeature(diffs);
  }

  /** @return true unconditionally. */
  @Override
  public final boolean isFile() {
    return true;
  }

  /** @return this object. */
  @Override
  public final INodeFile asFile() {
    return this;
  }

  @Override
  public boolean metadataEquals(INodeFileAttributes other) {
    return other != null
        && getHeaderLong()== other.getHeaderLong()
        && getPermissionLong() == other.getPermissionLong()
        && getAclFeature() == other.getAclFeature()
        && getXAttrFeature() == other.getXAttrFeature();
  }

  /* Start of Under-Construction Feature */

  /**
   * If the inode contains a {@link FileUnderConstructionFeature}, return it;
   * otherwise, return null.
   */
  public final FileUnderConstructionFeature getFileUnderConstructionFeature() {
    return getFeature(FileUnderConstructionFeature.class);
  }

  /** Is this file under construction? */
  @Override // BlockCollection
  public boolean isUnderConstruction() {
    return getFileUnderConstructionFeature() != null;
  }

  INodeFile toUnderConstruction(String clientName, String clientMachine) {
    Preconditions.checkState(!isUnderConstruction(),
        "file is already under construction");
    FileUnderConstructionFeature uc = new FileUnderConstructionFeature(
        clientName, clientMachine);
    addFeature(uc);
    return this;
  }

  /**
   * Convert the file to a complete file, i.e., to remove the Under-Construction
   * feature.
   */
  public INodeFile toCompleteFile(long mtime) {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");
    FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
    if (uc != null) {
      assertAllBlocksComplete();
      removeFeature(uc);
      this.setModificationTime(mtime);
    }
    return this;
  }

  /** Assert all blocks are complete. */
  private void assertAllBlocksComplete() {
    if (blocks == null) {
      return;
    }
    for (int i = 0; i < blocks.length; i++) {
      Preconditions.checkState(blocks[i].isComplete(), "Failed to finalize"
          + " %s %s since blocks[%s] is non-complete, where blocks=%s.",
          getClass().getSimpleName(), this, i, Arrays.asList(blocks));
    }
  }

  @Override // BlockCollection
  public void setBlock(int index, BlockInfo blk) {
    Preconditions.checkArgument(blk.isStriped() == this.isStriped());
    this.blocks[index] = blk;
  }

  @Override // BlockCollection, the file should be under construction
  public void convertLastBlockToUC(BlockInfo lastBlock,
      DatanodeStorageInfo[] locations) throws IOException {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");
    if (numBlocks() == 0) {
      throw new IOException("Failed to set last block: File is empty.");
    }
    lastBlock.convertToBlockUnderConstruction(BlockUCState.UNDER_CONSTRUCTION,
        locations);
  }

  void setLastBlock(BlockInfo blk) {
    blk.setBlockCollectionId(this.getId());
    setBlock(numBlocks() - 1, blk);
  }

  /**
   * Remove a block from the block list. This block should be
   * the last one on the list.
   */
  BlockInfo removeLastBlock(Block oldblock) {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");
    if (blocks == null || blocks.length == 0) {
      return null;
    }
    int size_1 = blocks.length - 1;
    if (!blocks[size_1].equals(oldblock)) {
      return null;
    }

    BlockInfo ucBlock = blocks[size_1];
    //copy to a new list
    BlockInfo[] newlist = new BlockInfo[size_1];
    System.arraycopy(blocks, 0, newlist, 0, size_1);
    setBlocks(newlist);
    return ucBlock;
  }

  /* End of Under-Construction Feature */
  
  /* Start of Snapshot Feature */

  public FileWithSnapshotFeature addSnapshotFeature(FileDiffList diffs) {
    Preconditions.checkState(!isWithSnapshot(), 
        "File is already with snapshot");
    FileWithSnapshotFeature sf = new FileWithSnapshotFeature(diffs);
    this.addFeature(sf);
    return sf;
  }
  
  /**
   * If feature list contains a {@link FileWithSnapshotFeature}, return it;
   * otherwise, return null.
   */
  public final FileWithSnapshotFeature getFileWithSnapshotFeature() {
    return getFeature(FileWithSnapshotFeature.class);
  }

  /** Is this file has the snapshot feature? */
  public final boolean isWithSnapshot() {
    return getFileWithSnapshotFeature() != null;
  }
    
  @Override
  public String toDetailString() {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    return super.toDetailString() + (sf == null ? "" : sf.getDetailedString()); 
  }

  @Override
  public INodeFileAttributes getSnapshotINode(final int snapshotId) {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf != null) {
      return sf.getDiffs().getSnapshotINode(snapshotId, this);
    } else {
      return this;
    }
  }

  @Override
  public void recordModification(final int latestSnapshotId) {
    recordModification(latestSnapshotId, false);
  }

  public void recordModification(final int latestSnapshotId, boolean withBlocks) {
    if (isInLatestSnapshot(latestSnapshotId)
        && !shouldRecordInSrcSnapshot(latestSnapshotId)) {
      // the file is in snapshot, create a snapshot feature if it does not have
      FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
      if (sf == null) {
        sf = addSnapshotFeature(null);
      }
      // record self in the diff list if necessary
      sf.getDiffs().saveSelf2Snapshot(latestSnapshotId, this, null, withBlocks);
    }
  }

  public FileDiffList getDiffs() {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf != null) {
      return sf.getDiffs();
    }
    return null;
  }
  
  /* End of Snapshot Feature */

  /** @return the replication factor of the file. */
  public final short getFileReplication(int snapshot) {
    if (snapshot != CURRENT_STATE_ID) {
      return getSnapshotINode(snapshot).getFileReplication();
    }
    return HeaderFormat.getReplication(header);
  }

  /** The same as getFileReplication(null). */
  @Override // INodeFileAttributes
  // TODO properly handle striped files
  public final short getFileReplication() {
    return getFileReplication(CURRENT_STATE_ID);
  }

  public short getPreferredBlockReplication() {
    short max = getFileReplication(CURRENT_STATE_ID);
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf != null) {
      short maxInSnapshot = sf.getMaxBlockRepInDiffs(null);
      if (sf.isCurrentFileDeleted()) {
        return maxInSnapshot;
      }
      max = maxInSnapshot > max ? maxInSnapshot : max;
    }
    if(!isStriped()){
      return max;
    }
    // TODO support more policies based on policyId
    ErasureCodingPolicy ecPolicy =
        ErasureCodingPolicyManager.getSystemDefaultPolicy();
    return (short) (ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
  }

  /** Set the replication factor of this file. */
  private void setFileReplication(short replication) {
    header = HeaderFormat.REPLICATION.BITS.combine(replication, header);
  }

  /** Set the replication factor of this file. */
  public final INodeFile setFileReplication(short replication,
      int latestSnapshotId) throws QuotaExceededException {
    recordModification(latestSnapshotId);
    setFileReplication(replication);
    return this;
  }

  /** @return preferred block size (in bytes) of the file. */
  @Override
  public long getPreferredBlockSize() {
    return HeaderFormat.getPreferredBlockSize(header);
  }

  @Override
  public byte getLocalStoragePolicyID() {
    return HeaderFormat.getStoragePolicyID(header);
  }

  @Override
  public byte getStoragePolicyID() {
    byte id = getLocalStoragePolicyID();
    if (id == BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      return this.getParent() != null ?
          this.getParent().getStoragePolicyID() : id;
    }
    return id;
  }

  private void setStoragePolicyID(byte storagePolicyId) {
    header = HeaderFormat.STORAGE_POLICY_ID.BITS.combine(storagePolicyId,
        header);
  }

  public final void setStoragePolicyID(byte storagePolicyId,
      int latestSnapshotId) throws QuotaExceededException {
    recordModification(latestSnapshotId);
    setStoragePolicyID(storagePolicyId);
  }


  /**
   * @return The ID of the erasure coding policy on the file. 0 represents no
   *          EC policy (file is in contiguous format). 1 represents the system
   *          default EC policy:
   *          {@link ErasureCodingPolicyManager#SYS_DEFAULT_POLICY}.
   * TODO: support more policies by reusing {@link HeaderFormat#REPLICATION}.
   */
  @VisibleForTesting
  @Override
  public byte getErasureCodingPolicyID() {
    return isStriped() ? (byte)1 : (byte)0;
  }

  /**
   * @return true if the file is in the striping layout.
   */
  @VisibleForTesting
  @Override
  public boolean isStriped() {
    return HeaderFormat.isStriped(header);
  }

  @Override // INodeFileAttributes
  public long getHeaderLong() {
    return header;
  }

  /** @return the blocks of the file. */
  @Override // BlockCollection
  public BlockInfo[] getBlocks() {
    return this.blocks;
  }

  /** @return blocks of the file corresponding to the snapshot. */
  public BlockInfo[] getBlocks(int snapshot) {
    if (snapshot == CURRENT_STATE_ID || getDiffs() == null) {
      return getBlocks();
    }
    // find blocks stored in snapshot diffs (for truncate)
    FileDiff diff = getDiffs().getDiffById(snapshot);
    // note that currently FileDiff can only store contiguous blocks
    BlockInfo[] snapshotBlocks = diff == null ? getBlocks() : diff.getBlocks();
    if (snapshotBlocks != null) {
      return snapshotBlocks;
    }
    // Blocks are not in the current snapshot
    // Find next snapshot with blocks present or return current file blocks
    snapshotBlocks = getDiffs().findLaterSnapshotBlocks(snapshot);
    return (snapshotBlocks == null) ? getBlocks() : snapshotBlocks;
  }

  /**
   * append array of blocks to this.blocks
   */
  void concatBlocks(INodeFile[] inodes, BlockManager bm) {
    int size = this.blocks.length;
    int totalAddedBlocks = 0;
    for(INodeFile f : inodes) {
      Preconditions.checkState(f.isStriped() == this.isStriped());
      totalAddedBlocks += f.blocks.length;
    }
    
    BlockInfo[] newlist =
        new BlockInfo[size + totalAddedBlocks];
    System.arraycopy(this.blocks, 0, newlist, 0, size);
    
    for(INodeFile in: inodes) {
      System.arraycopy(in.blocks, 0, newlist, size, in.blocks.length);
      size += in.blocks.length;
    }

    setBlocks(newlist);
    for(BlockInfo b : blocks) {
      b.setBlockCollectionId(getId());
      short oldRepl = b.getReplication();
      short repl = getPreferredBlockReplication();
      if (oldRepl != repl) {
        bm.setReplication(oldRepl, repl, b);
      }
    }
  }
  
  /**
   * add a block to the block list
   */
  void addBlock(BlockInfo newblock) {
    Preconditions.checkArgument(newblock.isStriped() == this.isStriped());
    if (this.blocks == null) {
      this.setBlocks(new BlockInfo[]{newblock});
    } else {
      int size = this.blocks.length;
      BlockInfo[] newlist = new BlockInfo[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newblock;
      this.setBlocks(newlist);
    }
  }

  /** Set the blocks. */
  private void setBlocks(BlockInfo[] blocks) {
    this.blocks = blocks;
  }

  /** Clear all blocks of the file. */
  public void clearBlocks() {
    setBlocks(null);
  }

  @Override
  public void cleanSubtree(ReclaimContext reclaimContext,
      final int snapshot, int priorSnapshotId) {
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      // TODO: avoid calling getStoragePolicyID
      sf.cleanFile(reclaimContext, this, snapshot, priorSnapshotId,
          getStoragePolicyID());
    } else {
      if (snapshot == CURRENT_STATE_ID) {
        if (priorSnapshotId == NO_SNAPSHOT_ID) {
          // this only happens when deleting the current file and it is not
          // in any snapshot
          destroyAndCollectBlocks(reclaimContext);
        } else {
          FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
          // when deleting the current file and it is in snapshot, we should
          // clean the 0-sized block if the file is UC
          if (uc != null) {
            uc.cleanZeroSizeBlock(this, reclaimContext.collectedBlocks);
            if (reclaimContext.removedUCFiles != null) {
              reclaimContext.removedUCFiles.add(getId());
            }
          }
        }
      }
    }
  }

  @Override
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    // TODO pass in the storage policy
    reclaimContext.quotaDelta().add(computeQuotaUsage(reclaimContext.bsps,
        false));
    clearFile(reclaimContext);
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      sf.getDiffs().destroyAndCollectSnapshotBlocks(
          reclaimContext.collectedBlocks);
      sf.clearDiffs();
    }
    if (isUnderConstruction() && reclaimContext.removedUCFiles != null) {
      reclaimContext.removedUCFiles.add(getId());
    }
  }

  public void clearFile(ReclaimContext reclaimContext) {
    if (blocks != null && reclaimContext.collectedBlocks != null) {
      for (BlockInfo blk : blocks) {
        reclaimContext.collectedBlocks.addDeleteBlock(blk);
        blk.setBlockCollectionId(INodeId.INVALID_INODE_ID);
      }
    }
    clearBlocks();
    if (getAclFeature() != null) {
      AclStorage.removeAclFeature(getAclFeature());
    }
    clear();
    reclaimContext.removedINodes.add(this);
  }

  @Override
  public String getName() {
    // Get the full path name of this inode.
    return getFullPathName();
  }

  // This is the only place that needs to use the BlockStoragePolicySuite to
  // derive the intended storage type usage for quota by storage type
  @Override
  public final QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
    final QuotaCounts counts = new QuotaCounts.Builder().nameSpace(1).build();

    final BlockStoragePolicy bsp = (blockStoragePolicyId ==
        BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) ? null :
        bsps.getPolicy(blockStoragePolicyId);
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf == null) {
      counts.add(storagespaceConsumed(bsp));
      return counts;
    }

    FileDiffList fileDiffList = sf.getDiffs();
    int last = fileDiffList.getLastSnapshotId();

    if (lastSnapshotId == Snapshot.CURRENT_STATE_ID
        || last == Snapshot.CURRENT_STATE_ID) {
      counts.add(storagespaceConsumed(bsp));
      return counts;
    }

    final long ssDeltaNoReplication;
    short replication;
    if (isStriped()) {
      return computeQuotaUsageWithStriped(bsp, counts);
    }

    if (last < lastSnapshotId) {
      ssDeltaNoReplication = computeFileSize(true, false);
      replication = getFileReplication();
    } else {
      int sid = fileDiffList.getSnapshotById(lastSnapshotId);
      ssDeltaNoReplication = computeFileSize(sid);
      replication = getFileReplication(sid);
    }

    counts.addStorageSpace(ssDeltaNoReplication * replication);
    if (bsp != null) {
      List<StorageType> storageTypes = bsp.chooseStorageTypes(replication);
      for (StorageType t : storageTypes) {
        if (!t.supportTypeQuota()) {
          continue;
        }
        counts.addTypeSpace(t, ssDeltaNoReplication);
      }
    }
    return counts;
  }

  /**
   * Compute quota of striped file. Note that currently EC files do not support
   * append/hflush/hsync, thus the file length recorded in snapshots should be
   * the same with the current file length.
   */
  public final QuotaCounts computeQuotaUsageWithStriped(
      BlockStoragePolicy bsp, QuotaCounts counts) {
    counts.addNameSpace(1);
    counts.add(storagespaceConsumed(bsp));
    return counts;
  }

  @Override
  public final ContentSummaryComputationContext computeContentSummary(
      int snapshotId, final ContentSummaryComputationContext summary) {
    final ContentCounts counts = summary.getCounts();
    counts.addContent(Content.FILE, 1);
    final long fileLen = computeFileSize(snapshotId);
    counts.addContent(Content.LENGTH, fileLen);
    counts.addContent(Content.DISKSPACE, storagespaceConsumed(null)
        .getStorageSpace());

    if (getStoragePolicyID() != BLOCK_STORAGE_POLICY_ID_UNSPECIFIED){
      BlockStoragePolicy bsp = summary.getBlockStoragePolicySuite().
          getPolicy(getStoragePolicyID());
      List<StorageType> storageTypes = bsp.chooseStorageTypes(getFileReplication());
      for (StorageType t : storageTypes) {
        if (!t.supportTypeQuota()) {
          continue;
        }
        counts.addTypeSpace(t, fileLen);
      }
    }
    return summary;
  }

  /** The same as computeFileSize(null). */
  public final long computeFileSize() {
    return computeFileSize(CURRENT_STATE_ID);
  }

  /**
   * Compute file size of the current file if the given snapshot is null;
   * otherwise, get the file size from the given snapshot.
   */
  public final long computeFileSize(int snapshotId) {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (snapshotId != CURRENT_STATE_ID && sf != null) {
      final FileDiff d = sf.getDiffs().getDiffById(snapshotId);
      if (d != null) {
        return d.getFileSize();
      }
    }
    return computeFileSize(true, false);
  }

  /**
   * Compute file size of the current file size
   * but not including the last block if it is under construction.
   */
  public final long computeFileSizeNotIncludingLastUcBlock() {
    return computeFileSize(false, false);
  }

  /**
   * Compute file size of the current file.
   * 
   * @param includesLastUcBlock
   *          If the last block is under construction, should it be included?
   * @param usePreferredBlockSize4LastUcBlock
   *          If the last block is under construction, should we use actual
   *          block size or preferred block size?
   *          Note that usePreferredBlockSize4LastUcBlock is ignored
   *          if includesLastUcBlock == false.
   * @return file size
   */
  public final long computeFileSize(boolean includesLastUcBlock,
      boolean usePreferredBlockSize4LastUcBlock) {
    if (blocks == null || blocks.length == 0) {
      return 0;
    }
    final int last = blocks.length - 1;
    //check if the last block is BlockInfoUnderConstruction
    BlockInfo lastBlk = blocks[last];
    long size = lastBlk.getNumBytes();
    if (!lastBlk.isComplete()) {
       if (!includesLastUcBlock) {
         size = 0;
       } else if (usePreferredBlockSize4LastUcBlock) {
         size = isStriped()?
             getPreferredBlockSize() *
                 ((BlockInfoStriped)lastBlk).getDataBlockNum() :
             getPreferredBlockSize();
       }
    }
    //sum other blocks
    for (int i = 0; i < last; i++) {
      size += blocks[i].getNumBytes();
    }
    return size;
  }

  /**
   * Compute size consumed by all blocks of the current file,
   * including blocks in its snapshots.
   * Use preferred block size for the last block if it is under construction.
   */
  public final QuotaCounts storagespaceConsumed(BlockStoragePolicy bsp) {
    if (isStriped()) {
      return storagespaceConsumedStriped();
    } else {
      return storagespaceConsumedContiguous(bsp);
    }
  }

  // TODO: support EC with heterogeneous storage
  public final QuotaCounts storagespaceConsumedStriped() {
    QuotaCounts counts = new QuotaCounts.Builder().build();
    if (blocks == null || blocks.length == 0) {
      return counts;
    }

    for (BlockInfo b : blocks) {
      Preconditions.checkState(b.isStriped());
      long blockSize = b.isComplete() ?
          ((BlockInfoStriped)b).spaceConsumed() : getPreferredBlockSize() *
          ((BlockInfoStriped)b).getTotalBlockNum();
      counts.addStorageSpace(blockSize);
    }
    return  counts;
  }

  public final QuotaCounts storagespaceConsumedContiguous(
      BlockStoragePolicy bsp) {
    QuotaCounts counts = new QuotaCounts.Builder().build();
    final Iterable<BlockInfo> blocks;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf == null) {
      blocks = Arrays.asList(getBlocks());
    } else {
      // Collect all distinct blocks
      Set<BlockInfo> allBlocks = new HashSet<>(Arrays.asList(getBlocks()));
      List<FileDiff> diffs = sf.getDiffs().asList();
      for(FileDiff diff : diffs) {
        BlockInfo[] diffBlocks = diff.getBlocks();
        if (diffBlocks != null) {
          allBlocks.addAll(Arrays.asList(diffBlocks));
        }
      }
      blocks = allBlocks;
    }

    final short replication = getPreferredBlockReplication();
    for (BlockInfo b : blocks) {
      long blockSize = b.isComplete() ? b.getNumBytes() :
          getPreferredBlockSize();
      counts.addStorageSpace(blockSize * replication);
      if (bsp != null) {
        List<StorageType> types = bsp.chooseStorageTypes(replication);
        for (StorageType t : types) {
          if (t.supportTypeQuota()) {
            counts.addTypeSpace(t, blockSize);
          }
        }
      }
    }
    return counts;
  }

  /**
   * Return the penultimate allocated block for this file.
   */
  BlockInfo getPenultimateBlock() {
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }

  @Override
  public BlockInfo getLastBlock() {
    return blocks == null || blocks.length == 0? null: blocks[blocks.length-1];
  }

  @Override
  public int numBlocks() {
    return blocks == null ? 0 : blocks.length;
  }

  @VisibleForTesting
  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      final int snapshotId) {
    super.dumpTreeRecursively(out, prefix, snapshotId);
    out.print(", fileSize=" + computeFileSize(snapshotId));
    // only compare the first block
    out.print(", blocks=");
    out.print(blocks == null || blocks.length == 0? null: blocks[0]);
    out.println();
  }

  /**
   * Remove full blocks at the end file up to newLength
   * @return sum of sizes of the remained blocks
   */
  public long collectBlocksBeyondMax(final long max,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] oldBlocks = getBlocks();
    if (oldBlocks == null) {
      return 0;
    }
    // find the minimum n such that the size of the first n blocks > max
    int n = 0;
    long size = 0;
    for(; n < oldBlocks.length && max > size; n++) {
      size += oldBlocks[n].getNumBytes();
    }
    if (n >= oldBlocks.length) {
      return size;
    }

    // starting from block n, the data is beyond max.
    // resize the array.
    truncateBlocksTo(n);

    // collect the blocks beyond max
    if (collectedBlocks != null) {
      for(; n < oldBlocks.length; n++) {
        collectedBlocks.addDeleteBlock(oldBlocks[n]);
      }
    }
    return size;
  }

  /**
   * compute the quota usage change for a truncate op
   * @param newLength the length for truncation
   * TODO: properly handle striped blocks (HDFS-7622)
   **/
  void computeQuotaDeltaForTruncate(
      long newLength, BlockStoragePolicy bsps,
      QuotaCounts delta) {
    final BlockInfo[] blocks = getBlocks();
    if (blocks == null || blocks.length == 0) {
      return;
    }

    long size = 0;
    for (BlockInfo b : blocks) {
      size += b.getNumBytes();
    }

    BlockInfo[] sblocks = null;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      FileDiff diff = sf.getDiffs().getLast();
      sblocks = diff != null ? diff.getBlocks() : null;
    }

    for (int i = blocks.length - 1; i >= 0 && size > newLength;
         size -= blocks[i].getNumBytes(), --i) {
      BlockInfo bi = blocks[i];
      long truncatedBytes;
      if (size - newLength < bi.getNumBytes()) {
        // Record a full block as the last block will be copied during
        // recovery
        truncatedBytes = bi.getNumBytes() - getPreferredBlockSize();
      } else {
        truncatedBytes = bi.getNumBytes();
      }

      // The block exist in snapshot, adding back the truncated bytes in the
      // existing files
      if (sblocks != null && i < sblocks.length && bi.equals(sblocks[i])) {
        truncatedBytes -= bi.getNumBytes();
      }

      delta.addStorageSpace(-truncatedBytes * bi.getReplication());
      if (bsps != null) {
        List<StorageType> types = bsps.chooseStorageTypes(bi.getReplication());
        for (StorageType t : types) {
          if (t.supportTypeQuota()) {
            delta.addTypeSpace(t, -truncatedBytes);
          }
        }
      }
    }
  }

  void truncateBlocksTo(int n) {
    final BlockInfo[] newBlocks;
    if (n == 0) {
      newBlocks = BlockInfo.EMPTY_ARRAY;
    } else {
      newBlocks = new BlockInfo[n];
      System.arraycopy(getBlocks(), 0, newBlocks, 0, n);
    }
    // set new blocks
    setBlocks(newBlocks);
  }

  /**
   * This function is only called when block list is stored in snapshot
   * diffs. Note that this can only happen when truncation happens with
   * snapshots. Since we do not support truncation with striped blocks,
   * we only need to handle contiguous blocks here.
   */
  public void collectBlocksBeyondSnapshot(BlockInfo[] snapshotBlocks,
                                          BlocksMapUpdateInfo collectedBlocks) {
    Preconditions.checkState(!isStriped());
    BlockInfo[] oldBlocks = getBlocks();
    if(snapshotBlocks == null || oldBlocks == null)
      return;
    // Skip blocks in common between the file and the snapshot
    int n = 0;
    while(n < oldBlocks.length && n < snapshotBlocks.length &&
          oldBlocks[n] == snapshotBlocks[n]) {
      n++;
    }
    truncateBlocksTo(n);
    // Collect the remaining blocks of the file
    while(n < oldBlocks.length) {
      collectedBlocks.addDeleteBlock(oldBlocks[n++]);
    }
  }

  /** Exclude blocks collected for deletion that belong to a snapshot. */
  void excludeSnapshotBlocks(int snapshotId,
                             BlocksMapUpdateInfo collectedBlocks) {
    if(collectedBlocks == null || collectedBlocks.getToDeleteList().isEmpty())
      return;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if(sf == null)
      return;
    BlockInfo[] snapshotBlocks =
        getDiffs().findEarlierSnapshotBlocks(snapshotId);
    if(snapshotBlocks == null)
      return;
    List<BlockInfo> toDelete = collectedBlocks.getToDeleteList();
    for(BlockInfo blk : snapshotBlocks) {
      if(toDelete.contains(blk))
        collectedBlocks.removeDeleteBlock(blk);
    }
  }

  /**
   * @return true if the block is contained in a snapshot or false otherwise.
   */
  boolean isBlockInLatestSnapshot(BlockInfo block) {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf == null || sf.getDiffs() == null) {
      return false;
    }
    BlockInfo[] snapshotBlocks = getDiffs()
        .findEarlierSnapshotBlocks(getDiffs().getLastSnapshotId());
    return snapshotBlocks != null &&
        Arrays.asList(snapshotBlocks).contains(block);
  }
}
