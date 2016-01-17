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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils.ContainerIdComparator;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is used to keep track of all the applications/containers
 * running on a node.
 *
 */
@Private
@Unstable
@SuppressWarnings("unchecked")
public class RMNodeImpl implements RMNode, EventHandler<RMNodeEvent> {

  private static final Log LOG = LogFactory.getLog(RMNodeImpl.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private final ReadLock readLock;
  private final WriteLock writeLock;

  private final ConcurrentLinkedQueue<UpdatedContainerInfo> nodeUpdateQueue;
  private volatile boolean nextHeartBeat = true;

  private final NodeId nodeId;
  private final RMContext context;
  private final String hostName;
  private final int commandPort;
  private int httpPort;
  private final String nodeAddress; // The containerManager address
  private String httpAddress;
  private volatile Resource totalCapability;
  private final Node node;

  private String healthReport;
  private long lastHealthReportTime;
  private String nodeManagerVersion;

  /* Aggregated resource utilization for the containers. */
  private ResourceUtilization containersUtilization;
  /* Resource utilization for the node. */
  private ResourceUtilization nodeUtilization;

  private final ContainerAllocationExpirer containerAllocationExpirer;
  /* set of containers that have just launched */
  private final Set<ContainerId> launchedContainers =
    new HashSet<ContainerId>();

  /* set of containers that need to be cleaned */
  private final Set<ContainerId> containersToClean = new TreeSet<ContainerId>(
      new ContainerIdComparator());

  /* set of containers that need to be signaled */
  private final List<SignalContainerRequest> containersToSignal =
      new ArrayList<SignalContainerRequest>();

  /*
   * set of containers to notify NM to remove them from its context. Currently,
   * this includes containers that were notified to AM about their completion
   */
  private final Set<ContainerId> containersToBeRemovedFromNM =
      new HashSet<ContainerId>();

  /* the list of applications that have finished and need to be purged */
  private final List<ApplicationId> finishedApplications =
      new ArrayList<ApplicationId>();

  /* the list of applications that are running on this node */
  private final List<ApplicationId> runningApplications =
      new ArrayList<ApplicationId>();
  
  private final Map<ContainerId, Container> toBeDecreasedContainers =
      new HashMap<>();
  
  private final Map<ContainerId, Container> nmReportedIncreasedContainers =
      new HashMap<>();

  private NodeHeartbeatResponse latestNodeHeartBeatResponse = recordFactory
      .newRecordInstance(NodeHeartbeatResponse.class);

  private static final StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent> stateMachineFactory 
                 = new StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent>(NodeState.NEW)

      //Transitions from NEW state
      .addTransition(NodeState.NEW, NodeState.RUNNING,
          RMNodeEventType.STARTED, new AddNodeTransition())
      .addTransition(NodeState.NEW, NodeState.NEW,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())

      //Transitions from RUNNING state
      .addTransition(NodeState.RUNNING,
          EnumSet.of(NodeState.RUNNING, NodeState.UNHEALTHY),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenHealthyTransition())
      .addTransition(NodeState.RUNNING, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.RUNNING, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.RUNNING,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.RUNNING, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.RUNNING, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new DeactivateNodeTransition(NodeState.REBOOTED))
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())
      .addTransition(NodeState.RUNNING, EnumSet.of(NodeState.RUNNING),
          RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.RESOURCE_UPDATE, new UpdateNodeResourceWhenRunningTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.DECREASE_CONTAINER,
          new DecreaseContainersTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.SIGNAL_CONTAINER, new SignalContainerTransition())
      .addTransition(NodeState.RUNNING, NodeState.SHUTDOWN,
          RMNodeEventType.SHUTDOWN,
          new DeactivateNodeTransition(NodeState.SHUTDOWN))

      //Transitions from REBOOTED state
      .addTransition(NodeState.REBOOTED, NodeState.REBOOTED,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())

      //Transitions from DECOMMISSIONED state
      .addTransition(NodeState.DECOMMISSIONED, NodeState.DECOMMISSIONED,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.DECOMMISSIONED, NodeState.DECOMMISSIONED,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

       //Transitions from DECOMMISSIONING state
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.RUNNING,
          RMNodeEventType.RECOMMISSION,
          new RecommissionNodeTransition(NodeState.RUNNING))
      .addTransition(NodeState.DECOMMISSIONING,
          EnumSet.of(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenHealthyTransition())
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.DECOMMISSIONING,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new DeactivateNodeTransition(NodeState.REBOOTED))

      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())

      // TODO (in YARN-3223) update resource when container finished.
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
      // TODO (in YARN-3223) update resource when container finished.
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())
      .addTransition(NodeState.DECOMMISSIONING, EnumSet.of(
          NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED),
          RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenRunningTransition())

      //Transitions from LOST state
      .addTransition(NodeState.LOST, NodeState.LOST,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.LOST, NodeState.LOST,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

      //Transitions from UNHEALTHY state
      .addTransition(NodeState.UNHEALTHY,
          EnumSet.of(NodeState.UNHEALTHY, NodeState.RUNNING),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenUnHealthyTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.UNHEALTHY, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.UNHEALTHY,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.UNHEALTHY, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.UNHEALTHY, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new DeactivateNodeTransition(NodeState.REBOOTED))
      .addTransition(NodeState.UNHEALTHY, EnumSet.of(NodeState.UNHEALTHY),
          RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
          RMNodeEventType.SIGNAL_CONTAINER, new SignalContainerTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.SHUTDOWN,
          RMNodeEventType.SHUTDOWN,
          new DeactivateNodeTransition(NodeState.SHUTDOWN))

      //Transitions from SHUTDOWN state
      .addTransition(NodeState.SHUTDOWN, NodeState.SHUTDOWN,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.SHUTDOWN, NodeState.SHUTDOWN,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

      // create the topology tables
      .installTopology();

  private final StateMachine<NodeState, RMNodeEventType,
                             RMNodeEvent> stateMachine;

  public RMNodeImpl(NodeId nodeId, RMContext context, String hostName,
      int cmPort, int httpPort, Node node, Resource capability, String nodeManagerVersion) {
    this.nodeId = nodeId;
    this.context = context;
    this.hostName = hostName;
    this.commandPort = cmPort;
    this.httpPort = httpPort;
    this.totalCapability = capability; 
    this.nodeAddress = hostName + ":" + cmPort;
    this.httpAddress = hostName + ":" + httpPort;
    this.node = node;
    this.healthReport = "Healthy";
    this.lastHealthReportTime = System.currentTimeMillis();
    this.nodeManagerVersion = nodeManagerVersion;

    this.latestNodeHeartBeatResponse.setResponseId(0);

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.stateMachine = stateMachineFactory.make(this);

    this.nodeUpdateQueue = new ConcurrentLinkedQueue<UpdatedContainerInfo>();

    this.containerAllocationExpirer = context.getContainerAllocationExpirer();
  }

  @Override
  public String toString() {
    return this.nodeId.toString();
  }

  @Override
  public String getHostName() {
    return hostName;
  }

  @Override
  public int getCommandPort() {
    return commandPort;
  }

  @Override
  public int getHttpPort() {
    return httpPort;
  }

  // Test only
  public void setHttpPort(int port) {
    this.httpPort = port;
  }

  @Override
  public NodeId getNodeID() {
    return this.nodeId;
  }

  @Override
  public String getNodeAddress() {
    return this.nodeAddress;
  }

  @Override
  public String getHttpAddress() {
    return this.httpAddress;
  }

  @Override
  public Resource getTotalCapability() {
    return this.totalCapability;
  }

  @Override
  public String getRackName() {
    return node.getNetworkLocation();
  }
  
  @Override
  public Node getNode() {
    return this.node;
  }
  
  @Override
  public String getHealthReport() {
    this.readLock.lock();

    try {
      return this.healthReport;
    } finally {
      this.readLock.unlock();
    }
  }
  
  public void setHealthReport(String healthReport) {
    this.writeLock.lock();

    try {
      this.healthReport = healthReport;
    } finally {
      this.writeLock.unlock();
    }
  }
  
  public void setLastHealthReportTime(long lastHealthReportTime) {
    this.writeLock.lock();

    try {
      this.lastHealthReportTime = lastHealthReportTime;
    } finally {
      this.writeLock.unlock();
    }
  }
  
  @Override
  public long getLastHealthReportTime() {
    this.readLock.lock();

    try {
      return this.lastHealthReportTime;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getNodeManagerVersion() {
    return nodeManagerVersion;
  }

  @Override
  public ResourceUtilization getAggregatedContainersUtilization() {
    this.readLock.lock();

    try {
      return this.containersUtilization;
    } finally {
      this.readLock.unlock();
    }
  }

  public void setAggregatedContainersUtilization(
      ResourceUtilization containersUtilization) {
    this.writeLock.lock();

    try {
      this.containersUtilization = containersUtilization;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public ResourceUtilization getNodeUtilization() {
    this.readLock.lock();

    try {
      return this.nodeUtilization;
    } finally {
      this.readLock.unlock();
    }
  }

  public void setNodeUtilization(ResourceUtilization nodeUtilization) {
    this.writeLock.lock();

    try {
      this.nodeUtilization = nodeUtilization;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public NodeState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ApplicationId> getAppsToCleanup() {
    this.readLock.lock();

    try {
      return new ArrayList<ApplicationId>(this.finishedApplications);
    } finally {
      this.readLock.unlock();
    }

  }
  
  @Override
  public List<ApplicationId> getRunningApps() {
    this.readLock.lock();
    try {
      return new ArrayList<ApplicationId>(this.runningApplications);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerId> getContainersToCleanUp() {

    this.readLock.lock();

    try {
      return new ArrayList<ContainerId>(this.containersToClean);
    } finally {
      this.readLock.unlock();
    }
  };

  @Override
  public void updateNodeHeartbeatResponseForCleanup(NodeHeartbeatResponse response) {
    this.writeLock.lock();

    try {
      response.addAllContainersToCleanup(
          new ArrayList<ContainerId>(this.containersToClean));
      response.addAllApplicationsToCleanup(this.finishedApplications);
      response.addContainersToBeRemovedFromNM(
          new ArrayList<ContainerId>(this.containersToBeRemovedFromNM));
      response.addAllContainersToSignal(this.containersToSignal);
      this.containersToClean.clear();
      this.finishedApplications.clear();
      this.containersToSignal.clear();
      this.containersToBeRemovedFromNM.clear();
    } finally {
      this.writeLock.unlock();
    }
  };
  
  @VisibleForTesting
  public Collection<Container> getToBeDecreasedContainers() {
    return toBeDecreasedContainers.values(); 
  }
  
  @Override
  public void updateNodeHeartbeatResponseForContainersDecreasing(
      NodeHeartbeatResponse response) {
    this.writeLock.lock();
    
    try {
      response.addAllContainersToDecrease(toBeDecreasedContainers.values());
      toBeDecreasedContainers.clear();
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public NodeHeartbeatResponse getLastNodeHeartBeatResponse() {

    this.readLock.lock();

    try {
      return this.latestNodeHeartBeatResponse;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void resetLastNodeHeartBeatResponse() {
    this.writeLock.lock();
    try {
      latestNodeHeartBeatResponse.setResponseId(0);
    } finally {
      this.writeLock.unlock();
    }
  }

  public void handle(RMNodeEvent event) {
    LOG.debug("Processing " + event.getNodeId() + " of type " + event.getType());
    try {
      writeLock.lock();
      NodeState oldState = getState();
      try {
         stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.error("Can't handle this event at current state", e);
        LOG.error("Invalid event " + event.getType() + 
            " on Node  " + this.nodeId);
      }
      if (oldState != getState()) {
        LOG.info(nodeId + " Node Transitioned from " + oldState + " to "
                 + getState());
      }
    }
    
    finally {
      writeLock.unlock();
    }
  }

  private void updateMetricsForRejoinedNode(NodeState previousNodeState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    metrics.incrNumActiveNodes();

    switch (previousNodeState) {
    case LOST:
      metrics.decrNumLostNMs();
      break;
    case REBOOTED:
      metrics.decrNumRebootedNMs();
      break;
    case DECOMMISSIONED:
      metrics.decrDecommisionedNMs();
      break;
    case UNHEALTHY:
      metrics.decrNumUnhealthyNMs();
      break;
    case SHUTDOWN:
      metrics.decrNumShutdownNMs();
      break;
    default:
      LOG.debug("Unexpected previous node state");
    }
  }

  // Update metrics when moving to Decommissioning state
  private void updateMetricsForGracefulDecommission(NodeState initialState,
      NodeState finalState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    switch (initialState) {
    case UNHEALTHY :
      metrics.decrNumUnhealthyNMs();
      break;
    case RUNNING :
      metrics.decrNumActiveNodes();
      break;
    case DECOMMISSIONING :
      metrics.decrDecommissioningNMs();
      break;
    default :
      LOG.warn("Unexpcted initial state");
    }

    switch (finalState) {
    case DECOMMISSIONING :
      metrics.incrDecommissioningNMs();
      break;
    case RUNNING :
      metrics.incrNumActiveNodes();
      break;
    default :
      LOG.warn("Unexpected final state");
    }
  }

  private void updateMetricsForDeactivatedNode(NodeState initialState,
                                               NodeState finalState) {
    ClusterMetrics metrics = ClusterMetrics.getMetrics();

    switch (initialState) {
    case RUNNING:
      metrics.decrNumActiveNodes();
      break;
    case DECOMMISSIONING:
      metrics.decrDecommissioningNMs();
      break;
    case UNHEALTHY:
      metrics.decrNumUnhealthyNMs();
      break;
    default:
      LOG.warn("Unexpected initial state");
    }

    switch (finalState) {
    case DECOMMISSIONED:
      metrics.incrDecommisionedNMs();
      break;
    case LOST:
      metrics.incrNumLostNMs();
      break;
    case REBOOTED:
      metrics.incrNumRebootedNMs();
      break;
    case UNHEALTHY:
      metrics.incrNumUnhealthyNMs();
      break;
    case SHUTDOWN:
      metrics.incrNumShutdownNMs();
      break;
    default:
      LOG.warn("Unexpected final state");
    }
  }

  private static void handleRunningAppOnNode(RMNodeImpl rmNode,
      RMContext context, ApplicationId appId, NodeId nodeId) {
    RMApp app = context.getRMApps().get(appId);

    // if we failed getting app by appId, maybe something wrong happened, just
    // add the app to the finishedApplications list so that the app can be
    // cleaned up on the NM
    if (null == app) {
      LOG.warn("Cannot get RMApp by appId=" + appId
          + ", just added it to finishedApplications list for cleanup");
      rmNode.finishedApplications.add(appId);
      rmNode.runningApplications.remove(appId);
      return;
    }

    // Add running applications back due to Node add or Node reconnection.
    rmNode.runningApplications.add(appId);
    context.getDispatcher().getEventHandler()
        .handle(new RMAppRunningOnNodeEvent(appId, nodeId));
  }
  
  private static void updateNodeResourceFromEvent(RMNodeImpl rmNode, 
     RMNodeResourceUpdateEvent event){
      ResourceOption resourceOption = event.getResourceOption();
      // Set resource on RMNode
      rmNode.totalCapability = resourceOption.getResource();
  }

  public static class AddNodeTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // Inform the scheduler
      RMNodeStartedEvent startEvent = (RMNodeStartedEvent) event;
      List<NMContainerStatus> containers = null;

      NodeId nodeId = rmNode.nodeId;
      if (rmNode.context.getInactiveRMNodes().containsKey(nodeId)) {
        // Old node rejoining
        RMNode previouRMNode = rmNode.context.getInactiveRMNodes().get(nodeId);
        rmNode.context.getInactiveRMNodes().remove(nodeId);
        rmNode.updateMetricsForRejoinedNode(previouRMNode.getState());
      } else {
        // Increment activeNodes explicitly because this is a new node.
        ClusterMetrics.getMetrics().incrNumActiveNodes();
        containers = startEvent.getNMContainerStatuses();
        if (containers != null && !containers.isEmpty()) {
          for (NMContainerStatus container : containers) {
            if (container.getContainerState() == ContainerState.RUNNING) {
              rmNode.launchedContainers.add(container.getContainerId());
            }
          }
        }
      }
      
      if (null != startEvent.getRunningApplications()) {
        for (ApplicationId appId : startEvent.getRunningApplications()) {
          handleRunningAppOnNode(rmNode, rmNode.context, appId, rmNode.nodeId);
        }
      }

      rmNode.context.getDispatcher().getEventHandler()
        .handle(new NodeAddedSchedulerEvent(rmNode, containers));
      rmNode.context.getDispatcher().getEventHandler().handle(
        new NodesListManagerEvent(
            NodesListManagerEventType.NODE_USABLE, rmNode));
    }
  }

  public static class ReconnectNodeTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeReconnectEvent reconnectEvent = (RMNodeReconnectEvent) event;
      RMNode newNode = reconnectEvent.getReconnectedNode();
      rmNode.nodeManagerVersion = newNode.getNodeManagerVersion();
      List<ApplicationId> runningApps = reconnectEvent.getRunningApplications();
      boolean noRunningApps = 
          (runningApps == null) || (runningApps.size() == 0);
      
      // No application running on the node, so send node-removal event with 
      // cleaning up old container info.
      if (noRunningApps) {
        if (rmNode.getState() == NodeState.DECOMMISSIONING) {
          // When node in decommissioning, and no running apps on this node,
          // it will return as decommissioned state.
          deactivateNode(rmNode, NodeState.DECOMMISSIONED);
          return NodeState.DECOMMISSIONED;
        }
        rmNode.nodeUpdateQueue.clear();
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeRemovedSchedulerEvent(rmNode));

        if (rmNode.getHttpPort() == newNode.getHttpPort()) {
          if (!rmNode.getTotalCapability().equals(
              newNode.getTotalCapability())) {
            rmNode.totalCapability = newNode.getTotalCapability();
          }
          if (rmNode.getState().equals(NodeState.RUNNING)) {
            // Only add old node if old state is RUNNING
            rmNode.context.getDispatcher().getEventHandler().handle(
                new NodeAddedSchedulerEvent(rmNode));
          }
        } else {
          // Reconnected node differs, so replace old node and start new node
          switch (rmNode.getState()) {
            case RUNNING:
              ClusterMetrics.getMetrics().decrNumActiveNodes();
              break;
            case UNHEALTHY:
              ClusterMetrics.getMetrics().decrNumUnhealthyNMs();
              break;
            default:
              LOG.debug("Unexpected Rmnode state");
            }
            rmNode.context.getRMNodes().put(newNode.getNodeID(), newNode);
            rmNode.context.getDispatcher().getEventHandler().handle(
                new RMNodeStartedEvent(newNode.getNodeID(), null, null));
        }

      } else {
        rmNode.httpPort = newNode.getHttpPort();
        rmNode.httpAddress = newNode.getHttpAddress();
        boolean isCapabilityChanged = false;
        if (!rmNode.getTotalCapability().equals(
            newNode.getTotalCapability())) {
          rmNode.totalCapability = newNode.getTotalCapability();
          isCapabilityChanged = true;
        }
      
        handleNMContainerStatus(reconnectEvent.getNMContainerStatuses(), rmNode);

        for (ApplicationId appId : reconnectEvent.getRunningApplications()) {
          handleRunningAppOnNode(rmNode, rmNode.context, appId, rmNode.nodeId);
        }

        if (isCapabilityChanged
            && rmNode.getState().equals(NodeState.RUNNING)) {
          // Update scheduler node's capacity for reconnect node.
          rmNode.context
              .getDispatcher()
              .getEventHandler()
              .handle(
                  new NodeResourceUpdateSchedulerEvent(rmNode, ResourceOption
                      .newInstance(newNode.getTotalCapability(), -1)));
        }

      }
      return rmNode.getState();
    }

    private void handleNMContainerStatus(
        List<NMContainerStatus> nmContainerStatuses, RMNodeImpl rmnode) {
      if (nmContainerStatuses != null) {
        List<ContainerStatus> containerStatuses =
            new ArrayList<ContainerStatus>();
        for (NMContainerStatus nmContainerStatus : nmContainerStatuses) {
          containerStatuses.add(createContainerStatus(nmContainerStatus));
        }
        rmnode.handleContainerStatus(containerStatuses);
      }
    }

    private ContainerStatus createContainerStatus(
        NMContainerStatus remoteContainer) {
      ContainerStatus cStatus =
          ContainerStatus.newInstance(remoteContainer.getContainerId(),
              remoteContainer.getContainerState(),
              remoteContainer.getDiagnostics(),
              remoteContainer.getContainerExitStatus());
      return cStatus;
    }
  }
  
  public static class UpdateNodeResourceWhenRunningTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeResourceUpdateEvent updateEvent = (RMNodeResourceUpdateEvent)event;
      updateNodeResourceFromEvent(rmNode, updateEvent);
      // Notify new resourceOption to scheduler
      rmNode.context.getDispatcher().getEventHandler().handle(
          new NodeResourceUpdateSchedulerEvent(rmNode, updateEvent.getResourceOption()));
    }
  }
  
  public static class UpdateNodeResourceWhenUnusableTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      // The node is not usable, only log a warn message
      LOG.warn("Try to update resource on a "+ rmNode.getState().toString() +
          " node: "+rmNode.toString());
      updateNodeResourceFromEvent(rmNode, (RMNodeResourceUpdateEvent)event);
      // No need to notify scheduler as schedulerNode is not function now
      // and can sync later from RMnode.
    }
  }
  
  public static class CleanUpAppTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      ApplicationId appId = ((RMNodeCleanAppEvent) event).getAppId();
      rmNode.finishedApplications.add(appId);
      rmNode.runningApplications.remove(appId);
    }
  }

  public static class CleanUpContainerTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.containersToClean.add(((
          RMNodeCleanContainerEvent) event).getContainerId());
    }
  }

  public static class AddContainersToBeRemovedFromNMTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.containersToBeRemovedFromNM.addAll(((
          RMNodeFinishedContainersPulledByAMEvent) event).getContainers());
    }
  }
  
  public static class DecreaseContainersTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {
 
    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeDecreaseContainerEvent de = (RMNodeDecreaseContainerEvent) event;

      for (Container c : de.getToBeDecreasedContainers()) {
        rmNode.toBeDecreasedContainers.put(c.getId(), c);
      }
    }
  }

  public static class DeactivateNodeTransition
    implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    private final NodeState finalState;
    public DeactivateNodeTransition(NodeState finalState) {
      this.finalState = finalState;
    }

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeImpl.deactivateNode(rmNode, finalState);
    }
  }

  /**
   * Put a node in deactivated (decommissioned) status.
   * @param rmNode
   * @param finalState
   */
  public static void deactivateNode(RMNodeImpl rmNode, NodeState finalState) {

    reportNodeUnusable(rmNode, finalState);

    // Deactivate the node
    rmNode.context.getRMNodes().remove(rmNode.nodeId);
    LOG.info("Deactivating Node " + rmNode.nodeId + " as it is now "
        + finalState);
    rmNode.context.getInactiveRMNodes().put(rmNode.nodeId, rmNode);
  }

  /**
   * Report node is UNUSABLE and update metrics.
   * @param rmNode
   * @param finalState
   */
  public static void reportNodeUnusable(RMNodeImpl rmNode,
      NodeState finalState) {
    // Inform the scheduler
    rmNode.nodeUpdateQueue.clear();
    // If the current state is NodeState.UNHEALTHY
    // Then node is already been removed from the
    // Scheduler
    NodeState initialState = rmNode.getState();
    if (!initialState.equals(NodeState.UNHEALTHY)) {
      rmNode.context.getDispatcher().getEventHandler()
        .handle(new NodeRemovedSchedulerEvent(rmNode));
    }
    rmNode.context.getDispatcher().getEventHandler().handle(
        new NodesListManagerEvent(
            NodesListManagerEventType.NODE_UNUSABLE, rmNode));

    //Update the metrics
    rmNode.updateMetricsForDeactivatedNode(initialState, finalState);
  }

  /**
   * The transition to put node in decommissioning state.
   */
  public static class DecommissioningNodeTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {
    private final NodeState initState;
    private final NodeState finalState;

    public DecommissioningNodeTransition(NodeState initState,
        NodeState finalState) {
      this.initState = initState;
      this.finalState = finalState;
    }

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      LOG.info("Put Node " + rmNode.nodeId + " in DECOMMISSIONING.");
      // Update NM metrics during graceful decommissioning.
      rmNode.updateMetricsForGracefulDecommission(initState, finalState);
      // TODO (in YARN-3223) Keep NM's available resource to be 0
    }
  }

  public static class RecommissionNodeTransition
      implements SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    private final NodeState finalState;
    public RecommissionNodeTransition(NodeState finalState) {
      this.finalState = finalState;
    }

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      LOG.info("Node " + rmNode.nodeId + " in DECOMMISSIONING is " +
          "recommissioned back to RUNNING.");
      rmNode
          .updateMetricsForGracefulDecommission(rmNode.getState(), finalState);
      // TODO handle NM resource resume in YARN-3223.
    }
  }

  /**
   * Status update transition when node is healthy.
   */
  public static class StatusUpdateWhenHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {
    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {

      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;

      // Switch the last heartbeatresponse.
      rmNode.latestNodeHeartBeatResponse = statusEvent.getLatestResponse();

      NodeHealthStatus remoteNodeHealthStatus =
          statusEvent.getNodeHealthStatus();
      rmNode.setHealthReport(remoteNodeHealthStatus.getHealthReport());
      rmNode.setLastHealthReportTime(
          remoteNodeHealthStatus.getLastHealthReportTime());
      rmNode.setAggregatedContainersUtilization(
          statusEvent.getAggregatedContainersUtilization());
      rmNode.setNodeUtilization(statusEvent.getNodeUtilization());
      NodeState initialState = rmNode.getState();
      boolean isNodeDecommissioning =
          initialState.equals(NodeState.DECOMMISSIONING);
      if (!remoteNodeHealthStatus.getIsNodeHealthy()) {
        LOG.info("Node " + rmNode.nodeId +
            " reported UNHEALTHY with details: " +
            remoteNodeHealthStatus.getHealthReport());
        // if a node in decommissioning receives an unhealthy report,
        // it will keep decommissioning.
        if (isNodeDecommissioning) {
          return NodeState.DECOMMISSIONING;
        } else {
          reportNodeUnusable(rmNode, NodeState.UNHEALTHY);
          return NodeState.UNHEALTHY;
        }
      }
      if (isNodeDecommissioning) {
        List<ApplicationId> runningApps = rmNode.getRunningApps();

        List<ApplicationId> keepAliveApps = statusEvent.getKeepAliveAppIds();

        // no running (and keeping alive) app on this node, get it
        // decommissioned.
        // TODO may need to check no container is being scheduled on this node
        // as well.
        if ((runningApps == null || runningApps.size() == 0)
            && (keepAliveApps == null || keepAliveApps.size() == 0)) {
          RMNodeImpl.deactivateNode(rmNode, NodeState.DECOMMISSIONED);
          return NodeState.DECOMMISSIONED;
        }

        // TODO (in YARN-3223) if node in decommissioning, get node resource
        // updated if container get finished (keep available resource to be 0)
      }

      rmNode.handleContainerStatus(statusEvent.getContainers());
      rmNode.handleReportedIncreasedContainers(
          statusEvent.getNMReportedIncreasedContainers());

      List<LogAggregationReport> logAggregationReportsForApps =
          statusEvent.getLogAggregationReportsForApps();
      if (logAggregationReportsForApps != null
          && !logAggregationReportsForApps.isEmpty()) {
        rmNode.handleLogAggregationStatus(logAggregationReportsForApps);
      }

      if(rmNode.nextHeartBeat) {
        rmNode.nextHeartBeat = false;
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeUpdateSchedulerEvent(rmNode));
      }

      // Update DTRenewer in secure mode to keep these apps alive. Today this is
      // needed for log-aggregation to finish long after the apps are gone.
      if (UserGroupInformation.isSecurityEnabled()) {
        rmNode.context.getDelegationTokenRenewer().updateKeepAliveApplications(
          statusEvent.getKeepAliveAppIds());
      }

      return initialState;
    }
  }

  public static class StatusUpdateWhenUnHealthyTransition implements
      MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

    @Override
    public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
      RMNodeStatusEvent statusEvent = (RMNodeStatusEvent)event;

      // Switch the last heartbeatresponse.
      rmNode.latestNodeHeartBeatResponse = statusEvent.getLatestResponse();
      NodeHealthStatus remoteNodeHealthStatus =
          statusEvent.getNodeHealthStatus();
      rmNode.setHealthReport(remoteNodeHealthStatus.getHealthReport());
      rmNode.setLastHealthReportTime(
          remoteNodeHealthStatus.getLastHealthReportTime());
      rmNode.setAggregatedContainersUtilization(
          statusEvent.getAggregatedContainersUtilization());
      rmNode.setNodeUtilization(statusEvent.getNodeUtilization());
      if (remoteNodeHealthStatus.getIsNodeHealthy()) {
        rmNode.context.getDispatcher().getEventHandler().handle(
            new NodeAddedSchedulerEvent(rmNode));
        rmNode.context.getDispatcher().getEventHandler().handle(
                new NodesListManagerEvent(
                    NodesListManagerEventType.NODE_USABLE, rmNode));
        // ??? how about updating metrics before notifying to ensure that
        // notifiers get update metadata because they will very likely query it
        // upon notification
        // Update metrics
        rmNode.updateMetricsForRejoinedNode(NodeState.UNHEALTHY);
        return NodeState.RUNNING;
      }

      return NodeState.UNHEALTHY;
    }
  }

  public static class SignalContainerTransition implements
      SingleArcTransition<RMNodeImpl, RMNodeEvent> {

    @Override
    public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
      rmNode.containersToSignal.add(((
          RMNodeSignalContainerEvent) event).getSignalRequest());
    }
  }

  @Override
  public List<UpdatedContainerInfo> pullContainerUpdates() {
    List<UpdatedContainerInfo> latestContainerInfoList = 
        new ArrayList<UpdatedContainerInfo>();
    UpdatedContainerInfo containerInfo;
    while ((containerInfo = nodeUpdateQueue.poll()) != null) {
      latestContainerInfoList.add(containerInfo);
    }
    this.nextHeartBeat = true;
    return latestContainerInfoList;
  }

  @VisibleForTesting
  public void setNextHeartBeat(boolean nextHeartBeat) {
    this.nextHeartBeat = nextHeartBeat;
  }
  
  @VisibleForTesting
  public int getQueueSize() {
    return nodeUpdateQueue.size();
  }

  // For test only.
  @VisibleForTesting
  public Set<ContainerId> getLaunchedContainers() {
    return this.launchedContainers;
  }

  @Override
  public Set<String> getNodeLabels() {
    RMNodeLabelsManager nlm = context.getNodeLabelManager();
    if (nlm == null || nlm.getLabelsOnNode(nodeId) == null) {
      return CommonNodeLabelsManager.EMPTY_STRING_SET;
    }
    return nlm.getLabelsOnNode(nodeId);
  }
  
  private void handleReportedIncreasedContainers(
      List<Container> reportedIncreasedContainers) {
    for (Container container : reportedIncreasedContainers) {
      ContainerId containerId = container.getId();

      // Don't bother with containers already scheduled for cleanup, or for
      // applications already killed. The scheduler doens't need to know any
      // more about this container
      if (containersToClean.contains(containerId)) {
        LOG.info("Container " + containerId + " already scheduled for "
            + "cleanup, no further processing");
        continue;
      }

      ApplicationId containerAppId =
          containerId.getApplicationAttemptId().getApplicationId();

      if (finishedApplications.contains(containerAppId)) {
        LOG.info("Container " + containerId
            + " belongs to an application that is already killed,"
            + " no further processing");
        continue;
      }
      
      this.nmReportedIncreasedContainers.put(containerId, container);
    }
  }

  private void handleContainerStatus(List<ContainerStatus> containerStatuses) {
    // Filter the map to only obtain just launched containers and finished
    // containers.
    List<ContainerStatus> newlyLaunchedContainers =
        new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers =
        new ArrayList<ContainerStatus>();
    for (ContainerStatus remoteContainer : containerStatuses) {
      ContainerId containerId = remoteContainer.getContainerId();

      // Don't bother with containers already scheduled for cleanup, or for
      // applications already killed. The scheduler doens't need to know any
      // more about this container
      if (containersToClean.contains(containerId)) {
        LOG.info("Container " + containerId + " already scheduled for "
            + "cleanup, no further processing");
        continue;
      }

      ApplicationId containerAppId =
          containerId.getApplicationAttemptId().getApplicationId();

      if (finishedApplications.contains(containerAppId)) {
        LOG.info("Container " + containerId
            + " belongs to an application that is already killed,"
            + " no further processing");
        continue;
      } else if (!runningApplications.contains(containerAppId)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Container " + containerId
              + " is the first container get launched for application "
              + containerAppId);
        }
        runningApplications.add(containerAppId);
      }

      // Process running containers
      if (remoteContainer.getState() == ContainerState.RUNNING) {
        if (!launchedContainers.contains(containerId)) {
          // Just launched container. RM knows about it the first time.
          launchedContainers.add(containerId);
          newlyLaunchedContainers.add(remoteContainer);
          // Unregister from containerAllocationExpirer.
          containerAllocationExpirer.unregister(containerId);
        }
      } else {
        // A finished container
        launchedContainers.remove(containerId);
        completedContainers.add(remoteContainer);
        // Unregister from containerAllocationExpirer.
        containerAllocationExpirer.unregister(containerId);
      }
    }
    if (newlyLaunchedContainers.size() != 0 || completedContainers.size() != 0) {
      nodeUpdateQueue.add(new UpdatedContainerInfo(newlyLaunchedContainers,
          completedContainers));
    }
  }

  private void handleLogAggregationStatus(
      List<LogAggregationReport> logAggregationReportsForApps) {
    for (LogAggregationReport report : logAggregationReportsForApps) {
      RMApp rmApp = this.context.getRMApps().get(report.getApplicationId());
      if (rmApp != null) {
        ((RMAppImpl)rmApp).aggregateLogReport(this.nodeId, report);
      }
    }
  }

  @Override
  public List<Container> pullNewlyIncreasedContainers() {
    try {
      writeLock.lock();

      if (nmReportedIncreasedContainers.isEmpty()) {
        return Collections.EMPTY_LIST;
      } else {
        List<Container> container =
            new ArrayList<Container>(nmReportedIncreasedContainers.values());
        nmReportedIncreasedContainers.clear();
        return container;
      }
      
    } finally {
      writeLock.unlock();
    }
   }
 }
