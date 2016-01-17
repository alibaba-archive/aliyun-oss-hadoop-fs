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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AssignmentInformation {

  public enum Operation {
    ALLOCATION, RESERVATION
  }

  public static class AssignmentDetails {
    public ContainerId containerId;
    public String queue;

    public AssignmentDetails(ContainerId containerId, String queue) {
      this.containerId = containerId;
      this.queue = queue;
    }
  }

  private final Map<Operation, Integer> operationCounts;
  private final Map<Operation, Resource> operationResources;
  private final Map<Operation, List<AssignmentDetails>> operationDetails;

  public AssignmentInformation() {
    this.operationCounts = new HashMap<>();
    this.operationResources = new HashMap<>();
    this.operationDetails = new HashMap<>();
    for (Operation op : Operation.values()) {
      operationCounts.put(op, 0);
      operationResources.put(op, Resource.newInstance(0, 0));
      operationDetails.put(op, new ArrayList<AssignmentDetails>());
    }
  }

  public int getNumAllocations() {
    return operationCounts.get(Operation.ALLOCATION);
  }

  public void incrAllocations() {
    increment(Operation.ALLOCATION, 1);
  }

  public void incrAllocations(int by) {
    increment(Operation.ALLOCATION, by);
  }

  public int getNumReservations() {
    return operationCounts.get(Operation.RESERVATION);
  }

  public void incrReservations() {
    increment(Operation.RESERVATION, 1);
  }

  public void incrReservations(int by) {
    increment(Operation.RESERVATION, by);
  }

  private void increment(Operation op, int by) {
    operationCounts.put(op, operationCounts.get(op) + by);
  }

  public Resource getAllocated() {
    return operationResources.get(Operation.ALLOCATION);
  }

  public Resource getReserved() {
    return operationResources.get(Operation.RESERVATION);
  }

  private void addAssignmentDetails(Operation op, ContainerId containerId,
      String queue) {
    operationDetails.get(op).add(new AssignmentDetails(containerId, queue));
  }

  public void addAllocationDetails(ContainerId containerId, String queue) {
    addAssignmentDetails(Operation.ALLOCATION, containerId, queue);
  }

  public void addReservationDetails(ContainerId containerId, String queue) {
    addAssignmentDetails(Operation.RESERVATION, containerId, queue);
  }

  public List<AssignmentDetails> getAllocationDetails() {
    return operationDetails.get(Operation.ALLOCATION);
  }

  public List<AssignmentDetails> getReservationDetails() {
    return operationDetails.get(Operation.RESERVATION);
  }

  private ContainerId getFirstContainerIdFromOperation(Operation op) {
    if (null != operationDetails.get(Operation.ALLOCATION)) {
      List<AssignmentDetails> assignDetails =
          operationDetails.get(Operation.ALLOCATION);
      if (!assignDetails.isEmpty()) {
        return assignDetails.get(0).containerId;
      }
    }
    return null;
  }

  public ContainerId getFirstAllocatedOrReservedContainerId() {
    ContainerId containerId = null;
    containerId = getFirstContainerIdFromOperation(Operation.ALLOCATION);
    if (null != containerId) {
      return containerId;
    }
    return getFirstContainerIdFromOperation(Operation.RESERVATION);
  }
}
