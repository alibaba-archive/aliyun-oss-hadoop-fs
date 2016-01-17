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

import java.beans.ConstructorProperties;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Statistics per StorageType.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class StorageTypeStats {
  private long capacityTotal = 0L;
  private long capacityUsed = 0L;
  private long capacityRemaining = 0L;
  private long blockPoolUsed = 0L;
  private int nodesInService = 0;

  @ConstructorProperties({"capacityTotal",
      "capacityUsed", "capacityRemaining",  "blockPoolUsed", "nodesInService"})
  public StorageTypeStats(long capacityTotal, long capacityUsed,
      long capacityRemaining, long blockPoolUsed, int nodesInService) {
    this.capacityTotal = capacityTotal;
    this.capacityUsed = capacityUsed;
    this.capacityRemaining = capacityRemaining;
    this.blockPoolUsed = blockPoolUsed;
    this.nodesInService = nodesInService;
  }

  public long getCapacityTotal() {
    return capacityTotal;
  }

  public long getCapacityUsed() {
    return capacityUsed;
  }

  public long getCapacityRemaining() {
    return capacityRemaining;
  }

  public long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  public int getNodesInService() {
    return nodesInService;
  }

  StorageTypeStats() {}

  StorageTypeStats(StorageTypeStats other) {
    capacityTotal = other.capacityTotal;
    capacityUsed = other.capacityUsed;
    capacityRemaining = other.capacityRemaining;
    blockPoolUsed = other.blockPoolUsed;
    nodesInService = other.nodesInService;
  }

  void addStorage(final DatanodeStorageInfo info,
      final DatanodeDescriptor node) {
    capacityUsed += info.getDfsUsed();
    blockPoolUsed += info.getBlockPoolUsed();
    if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
      capacityTotal += info.getCapacity();
      capacityRemaining += info.getRemaining();
    } else {
      capacityTotal += info.getDfsUsed();
    }
  }

  void addNode(final DatanodeDescriptor node) {
    if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
      nodesInService++;
    }
  }

  void subtractStorage(final DatanodeStorageInfo info,
      final DatanodeDescriptor node) {
    capacityUsed -= info.getDfsUsed();
    blockPoolUsed -= info.getBlockPoolUsed();
    if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
      capacityTotal -= info.getCapacity();
      capacityRemaining -= info.getRemaining();
    } else {
      capacityTotal -= info.getDfsUsed();
    }
  }

  void subtractNode(final DatanodeDescriptor node) {
    if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
      nodesInService--;
    }
  }
}
