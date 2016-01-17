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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;

import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFSAppAttempt extends FairSchedulerTestBase {

  @Before
  public void setup() {
    Configuration conf = createConfiguration();
    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
  }

  @Test
  public void testDelayScheduling() {
    FSLeafQueue queue = Mockito.mock(FSLeafQueue.class);
    Priority prio = Mockito.mock(Priority.class);
    Mockito.when(prio.getPriority()).thenReturn(1);
    double nodeLocalityThreshold = .5;
    double rackLocalityThreshold = .6;

    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    RMContext rmContext = resourceManager.getRMContext();
    FSAppAttempt schedulerApp =
        new FSAppAttempt(scheduler, applicationAttemptId, "user1", queue ,
            null, rmContext);

    // Default level should be node-local
    assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    // First five scheduling opportunities should remain node local
    for (int i = 0; i < 5; i++) {
      schedulerApp.addSchedulingOpportunity(prio);
      assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
          prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
    }

    // After five it should switch to rack local
    schedulerApp.addSchedulingOpportunity(prio);
    assertEquals(NodeType.RACK_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    // Manually set back to node local
    schedulerApp.resetAllowedLocalityLevel(prio, NodeType.NODE_LOCAL);
    schedulerApp.resetSchedulingOpportunities(prio);
    assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    // Now escalate again to rack-local, then to off-switch
    for (int i = 0; i < 5; i++) {
      schedulerApp.addSchedulingOpportunity(prio);
      assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
          prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
    }

    schedulerApp.addSchedulingOpportunity(prio);
    assertEquals(NodeType.RACK_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    for (int i = 0; i < 6; i++) {
      schedulerApp.addSchedulingOpportunity(prio);
      assertEquals(NodeType.RACK_LOCAL, schedulerApp.getAllowedLocalityLevel(
          prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
    }

    schedulerApp.addSchedulingOpportunity(prio);
    assertEquals(NodeType.OFF_SWITCH, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
  }

  @Test
  public void testDelaySchedulingForContinuousScheduling()
          throws InterruptedException {
    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue("queue", true);
    Priority prio = Mockito.mock(Priority.class);
    Mockito.when(prio.getPriority()).thenReturn(1);

    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    long nodeLocalityDelayMs = 5 * 1000L;    // 5 seconds
    long rackLocalityDelayMs = 6 * 1000L;    // 6 seconds

    RMContext rmContext = resourceManager.getRMContext();
    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    FSAppAttempt schedulerApp =
            new FSAppAttempt(scheduler, applicationAttemptId, "user1", queue,
                    null, rmContext);

    // Default level should be node-local
    assertEquals(NodeType.NODE_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // after 4 seconds should remain node local
    clock.tickSec(4);
    assertEquals(NodeType.NODE_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // after 6 seconds should switch to rack local
    clock.tickSec(2);
    assertEquals(NodeType.RACK_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // manually set back to node local
    schedulerApp.resetAllowedLocalityLevel(prio, NodeType.NODE_LOCAL);
    schedulerApp.resetSchedulingOpportunities(prio, clock.getTime());
    assertEquals(NodeType.NODE_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // Now escalate again to rack-local, then to off-switch
    clock.tickSec(6);
    assertEquals(NodeType.RACK_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    clock.tickSec(7);
    assertEquals(NodeType.OFF_SWITCH,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));
  }

  @Test
  /**
   * Ensure that when negative paramaters are given (signaling delay scheduling
   * no tin use), the least restrictive locality level is returned.
   */
  public void testLocalityLevelWithoutDelays() {
    FSLeafQueue queue = Mockito.mock(FSLeafQueue.class);
    Priority prio = Mockito.mock(Priority.class);
    Mockito.when(prio.getPriority()).thenReturn(1);

    RMContext rmContext = resourceManager.getRMContext();
    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    FSAppAttempt schedulerApp =
        new FSAppAttempt(scheduler, applicationAttemptId, "user1", queue ,
            null, rmContext);
    assertEquals(NodeType.OFF_SWITCH, schedulerApp.getAllowedLocalityLevel(
        prio, 10, -1.0, -1.0));
  }

  @Test
  public void testHeadroom() {
    final FairScheduler mockScheduler = Mockito.mock(FairScheduler.class);
    Mockito.when(mockScheduler.getClock()).thenReturn(scheduler.getClock());

    final FSLeafQueue mockQueue = Mockito.mock(FSLeafQueue.class);

    final Resource queueMaxResources = Resource.newInstance(5 * 1024, 3);
    final Resource queueFairShare = Resources.createResource(4096, 2);
    final Resource queueUsage = Resource.newInstance(2048, 2);

    final Resource queueStarvation =
        Resources.subtract(queueFairShare, queueUsage);
    final Resource queueMaxResourcesAvailable =
        Resources.subtract(queueMaxResources, queueUsage);

    final Resource clusterResource = Resources.createResource(8192, 8);
    final Resource clusterUsage = Resources.createResource(2048, 2);
    final Resource clusterAvailable =
        Resources.subtract(clusterResource, clusterUsage);

    final QueueMetrics fakeRootQueueMetrics = Mockito.mock(QueueMetrics.class);

    Mockito.when(mockQueue.getMaxShare()).thenReturn(queueMaxResources);
    Mockito.when(mockQueue.getFairShare()).thenReturn(queueFairShare);
    Mockito.when(mockQueue.getResourceUsage()).thenReturn(queueUsage);
    Mockito.when(mockScheduler.getClusterResource()).thenReturn
        (clusterResource);
    Mockito.when(fakeRootQueueMetrics.getAllocatedResources()).thenReturn
        (clusterUsage);
    Mockito.when(mockScheduler.getRootQueueMetrics()).thenReturn
        (fakeRootQueueMetrics);

    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    RMContext rmContext = resourceManager.getRMContext();
    FSAppAttempt schedulerApp =
        new FSAppAttempt(mockScheduler, applicationAttemptId, "user1", mockQueue ,
            null, rmContext);

    // Min of Memory and CPU across cluster and queue is used in
    // DominantResourceFairnessPolicy
    Mockito.when(mockQueue.getPolicy()).thenReturn(SchedulingPolicy
        .getInstance(DominantResourceFairnessPolicy.class));
    verifyHeadroom(schedulerApp,
        min(queueStarvation.getMemory(),
            clusterAvailable.getMemory(),
            queueMaxResourcesAvailable.getMemory()),
        min(queueStarvation.getVirtualCores(),
            clusterAvailable.getVirtualCores(),
            queueMaxResourcesAvailable.getVirtualCores())
    );

    // Fair and Fifo ignore CPU of queue, so use cluster available CPU
    Mockito.when(mockQueue.getPolicy()).thenReturn(SchedulingPolicy
        .getInstance(FairSharePolicy.class));
    verifyHeadroom(schedulerApp,
        min(queueStarvation.getMemory(),
            clusterAvailable.getMemory(),
            queueMaxResourcesAvailable.getMemory()),
        Math.min(
            clusterAvailable.getVirtualCores(),
            queueMaxResourcesAvailable.getVirtualCores())
    );

    Mockito.when(mockQueue.getPolicy()).thenReturn(SchedulingPolicy
        .getInstance(FifoPolicy.class));
    verifyHeadroom(schedulerApp,
        min(queueStarvation.getMemory(),
            clusterAvailable.getMemory(),
            queueMaxResourcesAvailable.getMemory()),
        Math.min(
            clusterAvailable.getVirtualCores(),
            queueMaxResourcesAvailable.getVirtualCores())
    );
  }

  private static int min(int value1, int value2, int value3) {
    return Math.min(Math.min(value1, value2), value3);
  }

  protected void verifyHeadroom(FSAppAttempt schedulerApp,
                                int expectedMemory, int expectedCPU) {
    Resource headroom = schedulerApp.getHeadroom();
    assertEquals(expectedMemory, headroom.getMemory());
    assertEquals(expectedCPU, headroom.getVirtualCores());
  }
}
