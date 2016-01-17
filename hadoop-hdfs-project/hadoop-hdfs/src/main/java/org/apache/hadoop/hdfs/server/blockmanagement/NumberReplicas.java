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

/**
 * A immutable object that stores the number of live replicas and
 * the number of decommissioned Replicas.
 */
public class NumberReplicas {
  private int liveReplicas;
  private int readOnlyReplicas;

  // Tracks only the decommissioning replicas
  private int decommissioning;
  // Tracks only the decommissioned replicas
  private int decommissioned;
  private int corruptReplicas;
  private int excessReplicas;
  private int replicasOnStaleNodes;

  NumberReplicas() {
    this(0, 0, 0, 0, 0, 0, 0);
  }

  NumberReplicas(int live, int readonly, int decommissioned,
      int decommissioning, int corrupt, int excess, int stale) {
    set(live, readonly, decommissioned, decommissioning, corrupt, excess, stale);
  }

  void set(int live, int readonly, int decommissioned, int decommissioning,
      int corrupt, int excess, int stale) {
    liveReplicas = live;
    readOnlyReplicas = readonly;
    this.decommissioning = decommissioning;
    this.decommissioned = decommissioned;
    corruptReplicas = corrupt;
    excessReplicas = excess;
    replicasOnStaleNodes = stale;
  }

  public int liveReplicas() {
    return liveReplicas;
  }

  public int readOnlyReplicas() {
    return readOnlyReplicas;
  }

  /**
   *
   * @return decommissioned replicas + decommissioning replicas
   * It is deprecated by decommissionedAndDecommissioning
   * due to its misleading name.
   */
  @Deprecated
  public int decommissionedReplicas() {
    return decommissionedAndDecommissioning();
  }

  /**
   *
   * @return decommissioned and decommissioning replicas
   */
  public int decommissionedAndDecommissioning() {
    return decommissioned + decommissioning;
  }

  /**
   *
   * @return decommissioned replicas only
   */
  public int decommissioned() {
    return decommissioned;
  }

  /**
   *
   * @return decommissioning replicas only
   */
  public int decommissioning() {
    return decommissioning;
  }

  public int corruptReplicas() {
    return corruptReplicas;
  }

  public int excessReplicas() {
    return excessReplicas;
  }
  
  /**
   * @return the number of replicas which are on stale nodes.
   * This is not mutually exclusive with the other counts -- ie a
   * replica may count as both "live" and "stale".
   */
  public int replicasOnStaleNodes() {
    return replicasOnStaleNodes;
  }
} 
