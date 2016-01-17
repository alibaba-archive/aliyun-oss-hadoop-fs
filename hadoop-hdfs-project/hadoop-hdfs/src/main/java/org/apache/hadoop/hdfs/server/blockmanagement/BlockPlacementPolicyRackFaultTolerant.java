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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.*;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas.
 * The strategy is that it tries its best to place the replicas to most racks.
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyRackFaultTolerant extends BlockPlacementPolicyDefault {

  @Override
  protected int[] getMaxNodesPerRack(int numOfChosen, int numOfReplicas) {
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = numOfChosen + numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas-clusterSize);
      totalNumOfReplicas = clusterSize;
    }
    // No calculation needed when there is only one rack or picking one node.
    int numOfRacks = clusterMap.getNumOfRacks();
    if (numOfRacks == 1 || totalNumOfReplicas <= 1) {
      return new int[] {numOfReplicas, totalNumOfReplicas};
    }
    if(totalNumOfReplicas<numOfRacks){
      return new int[] {numOfReplicas, 1};
    }
    int maxNodesPerRack = (totalNumOfReplicas - 1) / numOfRacks + 1;
    return new int[] {numOfReplicas, maxNodesPerRack};
  }

  /**
   * Choose numOfReplicas in order:
   * 1. If total replica expected is less than numOfRacks in cluster, it choose
   * randomly.
   * 2. If total replica expected is bigger than numOfRacks, it choose:
   *  2a. Fill each rack exactly (maxNodesPerRack-1) replicas.
   *  2b. For some random racks, place one more replica to each one of them, until
   *  numOfReplicas have been chosen. <br>
   * In the end, the difference of the numbers of replicas for each two racks
   * is no more than 1.
   * Either way it always prefer local storage.
   * @return local node of writer
   */
  @Override
  protected Node chooseTargetInOrder(int numOfReplicas,
                                 Node writer,
                                 final Set<Node> excludedNodes,
                                 final long blocksize,
                                 final int maxNodesPerRack,
                                 final List<DatanodeStorageInfo> results,
                                 final boolean avoidStaleNodes,
                                 final boolean newBlock,
                                 EnumMap<StorageType, Integer> storageTypes)
                                 throws NotEnoughReplicasException {
    int totalReplicaExpected = results.size() + numOfReplicas;
    int numOfRacks = clusterMap.getNumOfRacks();
    if (totalReplicaExpected < numOfRacks ||
        totalReplicaExpected % numOfRacks == 0) {
      writer = chooseOnce(numOfReplicas, writer, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
      return writer;
    }

    assert totalReplicaExpected > (maxNodesPerRack -1) * numOfRacks;

    // Calculate numOfReplicas for filling each rack exactly (maxNodesPerRack-1)
    // replicas.
    HashMap<String, Integer> rackCounts = new HashMap<>();
    for (DatanodeStorageInfo dsInfo : results) {
      String rack = dsInfo.getDatanodeDescriptor().getNetworkLocation();
      Integer count = rackCounts.get(rack);
      if (count != null) {
        rackCounts.put(rack, count + 1);
      } else {
        rackCounts.put(rack, 1);
      }
    }
    int excess = 0; // Sum of the above (maxNodesPerRack-1) part of nodes in results
    for (int count : rackCounts.values()) {
      if (count > maxNodesPerRack -1) {
        excess += count - (maxNodesPerRack -1);
      }
    }
    numOfReplicas = Math.min(totalReplicaExpected - results.size(),
        (maxNodesPerRack -1) * numOfRacks - (results.size() - excess));

    // Fill each rack exactly (maxNodesPerRack-1) replicas.
    writer = chooseOnce(numOfReplicas, writer, new HashSet<>(excludedNodes),
        blocksize, maxNodesPerRack -1, results, avoidStaleNodes, storageTypes);

    for (DatanodeStorageInfo resultStorage : results) {
      addToExcludedNodes(resultStorage.getDatanodeDescriptor(), excludedNodes);
    }

    // For some racks, place one more replica to each one of them.
    numOfReplicas = totalReplicaExpected - results.size();
    chooseOnce(numOfReplicas, writer, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);

    return writer;
  }

  /**
   * Randomly choose <i>numOfReplicas</i> targets from the given <i>scope</i>.
   * Except that 1st replica prefer local storage.
   * @return local node of writer.
   */
  private Node chooseOnce(int numOfReplicas,
                            Node writer,
                            final Set<Node> excludedNodes,
                            final long blocksize,
                            final int maxNodesPerRack,
                            final List<DatanodeStorageInfo> results,
                            final boolean avoidStaleNodes,
                            EnumMap<StorageType, Integer> storageTypes)
                            throws NotEnoughReplicasException {
    if (numOfReplicas == 0) {
      return writer;
    }
    writer = chooseLocalStorage(writer, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes, true)
        .getDatanodeDescriptor();
    if (--numOfReplicas == 0) {
      return writer;
    }
    chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    return writer;
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    if (locs == null)
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    if (!clusterMap.hasClusterEverBeenMultiRack()) {
      // only one rack
      return new BlockPlacementStatusDefault(1, 1);
    }
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    Set<String> racks = new TreeSet<String>();
    for (DatanodeInfo dn : locs)
      racks.add(dn.getNetworkLocation());
    return new BlockPlacementStatusDefault(racks.size(), numberOfReplicas);
  }

  @Override
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    return moreThanOne.isEmpty() ? exactlyOne : moreThanOne;
  }
}
