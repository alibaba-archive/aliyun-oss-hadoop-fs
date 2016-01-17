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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;


import java.util.ArrayList;
import java.util.Collection;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.resource.Resources;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({FairSchedulerLeafQueueInfo.class})
public class FairSchedulerQueueInfo {  
  private int maxApps;
  
  @XmlTransient
  private float fractionMemUsed;
  @XmlTransient
  private float fractionMemSteadyFairShare;
  @XmlTransient
  private float fractionMemFairShare;
  @XmlTransient
  private float fractionMemMinShare;
  @XmlTransient
  private float fractionMemMaxShare;
  
  private ResourceInfo minResources;
  private ResourceInfo maxResources;
  private ResourceInfo usedResources;
  private ResourceInfo steadyFairResources;
  private ResourceInfo fairResources;
  private ResourceInfo clusterResources;

  private long pendingContainers;
  private long allocatedContainers;
  private long reservedContainers;

  private String queueName;
  private String schedulingPolicy;

  private FairSchedulerQueueInfoList childQueues;

  public FairSchedulerQueueInfo() {
  }
  
  public FairSchedulerQueueInfo(FSQueue queue, FairScheduler scheduler) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    
    queueName = queue.getName();
    schedulingPolicy = queue.getPolicy().getName();
    
    clusterResources = new ResourceInfo(scheduler.getClusterResource());
    
    usedResources = new ResourceInfo(queue.getResourceUsage());
    fractionMemUsed = (float)usedResources.getMemory() /
        clusterResources.getMemory();

    steadyFairResources = new ResourceInfo(queue.getSteadyFairShare());
    fairResources = new ResourceInfo(queue.getFairShare());
    minResources = new ResourceInfo(queue.getMinShare());
    maxResources = new ResourceInfo(queue.getMaxShare());
    maxResources = new ResourceInfo(
        Resources.componentwiseMin(queue.getMaxShare(),
            scheduler.getClusterResource()));

    fractionMemSteadyFairShare =
        (float)steadyFairResources.getMemory() / clusterResources.getMemory();
    fractionMemFairShare = (float) fairResources.getMemory()
        / clusterResources.getMemory();
    fractionMemMinShare = (float)minResources.getMemory() / clusterResources.getMemory();
    fractionMemMaxShare = (float)maxResources.getMemory() / clusterResources.getMemory();
    
    maxApps = allocConf.getQueueMaxApps(queueName);

    pendingContainers = queue.getMetrics().getPendingContainers();
    allocatedContainers = queue.getMetrics().getAllocatedContainers();
    reservedContainers = queue.getMetrics().getReservedContainers();

    if (allocConf.isReservable(queueName) &&
        !allocConf.getShowReservationAsQueues(queueName)) {
      return;
    }

    childQueues = getChildQueues(queue, scheduler);
  }

  public long getPendingContainers() {
    return pendingContainers;
  }

  public long getAllocatedContainers() {
    return allocatedContainers;
  }

  public long getReservedContainers() {
    return reservedContainers;
  }

  protected FairSchedulerQueueInfoList getChildQueues(FSQueue queue,
                                                      FairScheduler scheduler) {
    // Return null to omit 'childQueues' field from the return value of
    // REST API if it is empty. We omit the field to keep the consistency
    // with CapacitySchedulerQueueInfo, which omits 'queues' field if empty.
    Collection<FSQueue> children = queue.getChildQueues();
    if (children.isEmpty()) {
      return null;
    }
    FairSchedulerQueueInfoList list = new FairSchedulerQueueInfoList();
    for (FSQueue child : children) {
      if (child instanceof FSLeafQueue) {
        list.addToQueueInfoList(
            new FairSchedulerLeafQueueInfo((FSLeafQueue) child, scheduler));
      } else {
        list.addToQueueInfoList(
            new FairSchedulerQueueInfo(child, scheduler));
      }
    }
    return list;
  }
  
  /**
   * Returns the steady fair share as a fraction of the entire cluster capacity.
   */
  public float getSteadyFairShareMemoryFraction() {
    return fractionMemSteadyFairShare;
  }

  /**
   * Returns the fair share as a fraction of the entire cluster capacity.
   */
  public float getFairShareMemoryFraction() {
    return fractionMemFairShare;
  }

  /**
   * Returns the steady fair share of this queue in megabytes.
   */
  public ResourceInfo getSteadyFairShare() {
    return steadyFairResources;
  }

  /**
   * Returns the fair share of this queue in megabytes
   */
  public ResourceInfo getFairShare() {
    return fairResources;
  }

  public ResourceInfo getMinResources() {
    return minResources;
  }
  
  public ResourceInfo getMaxResources() {
    return maxResources;
  }
  
  public int getMaxApplications() {
    return maxApps;
  }
  
  public String getQueueName() {
    return queueName;
  }
  
  public ResourceInfo getUsedResources() {
    return usedResources;
  }

  /**
   * Returns the queue's min share in as a fraction of the entire
   * cluster capacity.
   */
  public float getMinShareMemoryFraction() {
    return fractionMemMinShare;
  }
  
  /**
   * Returns the memory used by this queue as a fraction of the entire 
   * cluster capacity.
   */
  public float getUsedMemoryFraction() {
    return fractionMemUsed;
  }
  
  /**
   * Returns the capacity of this queue as a fraction of the entire cluster 
   * capacity.
   */
  public float getMaxResourcesFraction() {
    return fractionMemMaxShare;
  }
  
  /**
   * Returns the name of the scheduling policy used by this queue.
   */
  public String getSchedulingPolicy() {
    return schedulingPolicy;
  }

  public Collection<FairSchedulerQueueInfo> getChildQueues() {
    return childQueues != null ? childQueues.getQueueInfoList() :
        new ArrayList<FairSchedulerQueueInfo>();
  }
}
