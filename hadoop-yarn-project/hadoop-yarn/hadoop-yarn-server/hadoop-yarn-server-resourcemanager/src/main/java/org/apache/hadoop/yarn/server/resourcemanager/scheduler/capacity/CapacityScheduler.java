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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerResourceChangeRequest;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule.QueueMapping;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeDecreaseContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueInvalidException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedContainerChangeRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.AssignmentInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerRescheduledEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeLabelsUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class CapacityScheduler extends
    AbstractYarnScheduler<FiCaSchedulerApp, FiCaSchedulerNode> implements
    PreemptableResourceScheduler, CapacitySchedulerContext, Configurable {

  private static final Log LOG = LogFactory.getLog(CapacityScheduler.class);
  private YarnAuthorizationProvider authorizer;
 
  private CSQueue root;
  // timeout to join when we stop this service
  protected final long THREAD_JOIN_TIMEOUT_MS = 1000;

  static final Comparator<CSQueue> nonPartitionedQueueComparator =
      new Comparator<CSQueue>() {
    @Override
    public int compare(CSQueue q1, CSQueue q2) {
      if (q1.getUsedCapacity() < q2.getUsedCapacity()) {
        return -1;
      } else if (q1.getUsedCapacity() > q2.getUsedCapacity()) {
        return 1;
      }

      return q1.getQueuePath().compareTo(q2.getQueuePath());
    }
  };
  
  static final PartitionedQueueComparator partitionedQueueComparator =
      new PartitionedQueueComparator();

  @Override
  public void setConf(Configuration conf) {
      yarnConf = conf;
  }
  
  private void validateConf(Configuration conf) {
    // validate scheduler memory allocation setting
    int minMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ", min and max should be greater than 0"
        + ", max should be no smaller than min.");
    }

    // validate scheduler vcores allocation setting
    int minVcores = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    int maxVcores = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    if (minVcores <= 0 || minVcores > maxVcores) {
      throw new YarnRuntimeException("Invalid resource scheduler vcores"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
        + "=" + minVcores
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
        + "=" + maxVcores + ", min and max should be greater than 0"
        + ", max should be no smaller than min.");
    }
  }

  @Override
  public Configuration getConf() {
    return yarnConf;
  }

  private CapacitySchedulerConfiguration conf;
  private Configuration yarnConf;

  private Map<String, CSQueue> queues = new ConcurrentHashMap<String, CSQueue>();

  private AtomicInteger numNodeManagers = new AtomicInteger(0);

  private ResourceCalculator calculator;
  private boolean usePortForNodeName;

  private boolean scheduleAsynchronously;
  private AsyncScheduleThread asyncSchedulerThread;
  private RMNodeLabelsManager labelManager;
  private SchedulerHealth schedulerHealth = new SchedulerHealth();
  long lastNodeUpdateTime;

  /**
   * EXPERT
   */
  private long asyncScheduleInterval;
  private static final String ASYNC_SCHEDULER_INTERVAL =
      CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_PREFIX
          + ".scheduling-interval-ms";
  private static final long DEFAULT_ASYNC_SCHEDULER_INTERVAL = 5;

  public CapacityScheduler() {
    super(CapacityScheduler.class.getName());
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return root.getMetrics();
  }

  public CSQueue getRootQueue() {
    return root;
  }
  
  @Override
  public CapacitySchedulerConfiguration getConfiguration() {
    return conf;
  }

  @Override
  public synchronized RMContainerTokenSecretManager 
  getContainerTokenSecretManager() {
    return this.rmContext.getContainerTokenSecretManager();
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return calculator;
  }

  @Override
  public Comparator<CSQueue> getNonPartitionedQueueComparator() {
    return nonPartitionedQueueComparator;
  }
  
  @Override
  public PartitionedQueueComparator getPartitionedQueueComparator() {
    return partitionedQueueComparator;
  }

  @Override
  public int getNumClusterNodes() {
    return numNodeManagers.get();
  }

  @Override
  public synchronized RMContext getRMContext() {
    return this.rmContext;
  }

  @Override
  public synchronized void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  private synchronized void initScheduler(Configuration configuration) throws
      IOException {
    this.conf = loadCapacitySchedulerConfiguration(configuration);
    validateConf(this.conf);
    this.minimumAllocation = this.conf.getMinimumAllocation();
    initMaximumResourceCapability(this.conf.getMaximumAllocation());
    this.calculator = this.conf.getResourceCalculator();
    this.usePortForNodeName = this.conf.getUsePortForNodeName();
    this.applications =
        new ConcurrentHashMap<ApplicationId,
            SchedulerApplication<FiCaSchedulerApp>>();
    this.labelManager = rmContext.getNodeLabelManager();
    authorizer = YarnAuthorizationProvider.getInstance(yarnConf);
    initializeQueues(this.conf);

    scheduleAsynchronously = this.conf.getScheduleAynschronously();
    asyncScheduleInterval =
        this.conf.getLong(ASYNC_SCHEDULER_INTERVAL,
            DEFAULT_ASYNC_SCHEDULER_INTERVAL);
    if (scheduleAsynchronously) {
      asyncSchedulerThread = new AsyncScheduleThread(this);
    }

    LOG.info("Initialized CapacityScheduler with " +
        "calculator=" + getResourceCalculator().getClass() + ", " +
        "minimumAllocation=<" + getMinimumResourceCapability() + ">, " +
        "maximumAllocation=<" + getMaximumResourceCapability() + ">, " +
        "asynchronousScheduling=" + scheduleAsynchronously + ", " +
        "asyncScheduleInterval=" + asyncScheduleInterval + "ms");
  }

  private synchronized void startSchedulerThreads() {
    if (scheduleAsynchronously) {
      Preconditions.checkNotNull(asyncSchedulerThread,
          "asyncSchedulerThread is null");
      asyncSchedulerThread.start();
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Configuration configuration = new Configuration(conf);
    super.serviceInit(conf);
    initScheduler(configuration);
  }

  @Override
  public void serviceStart() throws Exception {
    startSchedulerThreads();
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    synchronized (this) {
      if (scheduleAsynchronously && asyncSchedulerThread != null) {
        asyncSchedulerThread.interrupt();
        asyncSchedulerThread.join(THREAD_JOIN_TIMEOUT_MS);
      }
    }
    super.serviceStop();
  }

  @Override
  public synchronized void
  reinitialize(Configuration conf, RMContext rmContext) throws IOException {
    Configuration configuration = new Configuration(conf);
    CapacitySchedulerConfiguration oldConf = this.conf;
    this.conf = loadCapacitySchedulerConfiguration(configuration);
    validateConf(this.conf);
    try {
      LOG.info("Re-initializing queues...");
      refreshMaximumAllocation(this.conf.getMaximumAllocation());
      reinitializeQueues(this.conf);
    } catch (Throwable t) {
      this.conf = oldConf;
      refreshMaximumAllocation(this.conf.getMaximumAllocation());
      throw new IOException("Failed to re-init queues", t);
    }
  }
  
  long getAsyncScheduleInterval() {
    return asyncScheduleInterval;
  }

  private final static Random random = new Random(System.currentTimeMillis());
  
  /**
   * Schedule on all nodes by starting at a random point.
   * @param cs
   */
  static void schedule(CapacityScheduler cs) {
    // First randomize the start point
    int current = 0;
    Collection<FiCaSchedulerNode> nodes = cs.getAllNodes().values();
    int start = random.nextInt(nodes.size());
    for (FiCaSchedulerNode node : nodes) {
      if (current++ >= start) {
        cs.allocateContainersToNode(node);
      }
    }
    // Now, just get everyone to be safe
    for (FiCaSchedulerNode node : nodes) {
      cs.allocateContainersToNode(node);
    }
    try {
      Thread.sleep(cs.getAsyncScheduleInterval());
    } catch (InterruptedException e) {}
  }
  
  static class AsyncScheduleThread extends Thread {

    private final CapacityScheduler cs;
    private AtomicBoolean runSchedules = new AtomicBoolean(false);

    public AsyncScheduleThread(CapacityScheduler cs) {
      this.cs = cs;
      setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        if (!runSchedules.get()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException ie) {}
        } else {
          schedule(cs);
        }
      }
    }

    public void beginSchedule() {
      runSchedules.set(true);
    }

    public void suspendSchedule() {
      runSchedules.set(false);
    }

  }
  
  @Private
  public static final String ROOT_QUEUE = 
    CapacitySchedulerConfiguration.PREFIX + CapacitySchedulerConfiguration.ROOT;

  static class QueueHook {
    public CSQueue hook(CSQueue queue) {
      return queue;
    }
  }
  private static final QueueHook noop = new QueueHook();

  @VisibleForTesting
  public synchronized UserGroupMappingPlacementRule
      getUserGroupMappingPlacementRule() throws IOException {
    boolean overrideWithQueueMappings = conf.getOverrideWithQueueMappings();
    LOG.info("Initialized queue mappings, override: "
        + overrideWithQueueMappings);

    // Get new user/group mappings
    List<UserGroupMappingPlacementRule.QueueMapping> newMappings =
        conf.getQueueMappings();
    // check if mappings refer to valid queues
    for (QueueMapping mapping : newMappings) {
      String mappingQueue = mapping.getQueue();
      if (!mappingQueue
          .equals(UserGroupMappingPlacementRule.CURRENT_USER_MAPPING)
          && !mappingQueue
              .equals(UserGroupMappingPlacementRule.PRIMARY_GROUP_MAPPING)) {
        CSQueue queue = queues.get(mappingQueue);
        if (queue == null || !(queue instanceof LeafQueue)) {
          throw new IOException("mapping contains invalid or non-leaf queue "
              + mappingQueue);
        }
      }
    }

    // initialize groups if mappings are present
    if (newMappings.size() > 0) {
      Groups groups = new Groups(conf);
      return new UserGroupMappingPlacementRule(overrideWithQueueMappings,
          newMappings, groups);
    }

    return null;
  }

  private void updatePlacementRules() throws IOException {
    List<PlacementRule> placementRules = new ArrayList<>();
    
    // Initialize UserGroupMappingPlacementRule
    // TODO, need make this defineable by configuration.
    UserGroupMappingPlacementRule ugRule = getUserGroupMappingPlacementRule();
    if (null != ugRule) {
      placementRules.add(ugRule);
    }
    
    rmContext.getQueuePlacementManager().updateRules(placementRules);
  }

  @Lock(CapacityScheduler.class)
  private void initializeQueues(CapacitySchedulerConfiguration conf)
    throws IOException {

    root = 
        parseQueue(this, conf, null, CapacitySchedulerConfiguration.ROOT, 
            queues, queues, noop);
    labelManager.reinitializeQueueLabels(getQueueToLabels());
    LOG.info("Initialized root queue " + root);
    updatePlacementRules();
    setQueueAcls(authorizer, queues);
  }

  @Lock(CapacityScheduler.class)
  private void reinitializeQueues(CapacitySchedulerConfiguration conf) 
  throws IOException {
    // Parse new queues
    Map<String, CSQueue> newQueues = new HashMap<String, CSQueue>();
    CSQueue newRoot = 
        parseQueue(this, conf, null, CapacitySchedulerConfiguration.ROOT, 
            newQueues, queues, noop); 
    
    // Ensure all existing queues are still present
    validateExistingQueues(queues, newQueues);

    // Add new queues
    addNewQueues(queues, newQueues);
    
    // Re-configure queues
    root.reinitialize(newRoot, clusterResource);
    updatePlacementRules();

    // Re-calculate headroom for active applications
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));

    labelManager.reinitializeQueueLabels(getQueueToLabels());
    setQueueAcls(authorizer, queues);
  }

  @VisibleForTesting
  public static void setQueueAcls(YarnAuthorizationProvider authorizer,
      Map<String, CSQueue> queues) throws IOException {
    for (CSQueue queue : queues.values()) {
      AbstractCSQueue csQueue = (AbstractCSQueue) queue;
      authorizer.setPermission(csQueue.getPrivilegedEntity(),
        csQueue.getACLs(), UserGroupInformation.getCurrentUser());
    }
  }

  private Map<String, Set<String>> getQueueToLabels() {
    Map<String, Set<String>> queueToLabels = new HashMap<String, Set<String>>();
    for (CSQueue queue : queues.values()) {
      queueToLabels.put(queue.getQueueName(), queue.getAccessibleNodeLabels());
    }
    return queueToLabels;
  }

  /**
   * Ensure all existing queues are present. Queues cannot be deleted
   * @param queues existing queues
   * @param newQueues new queues
   */
  @Lock(CapacityScheduler.class)
  private void validateExistingQueues(
      Map<String, CSQueue> queues, Map<String, CSQueue> newQueues) 
  throws IOException {
    // check that all static queues are included in the newQueues list
    for (Map.Entry<String, CSQueue> e : queues.entrySet()) {
      if (!(e.getValue() instanceof ReservationQueue)) {
        String queueName = e.getKey();
        CSQueue oldQueue = e.getValue();
        CSQueue newQueue = newQueues.get(queueName); 
        if (null == newQueue) {
          throw new IOException(queueName + " cannot be found during refresh!");
        } else if (!oldQueue.getQueuePath().equals(newQueue.getQueuePath())) {
          throw new IOException(queueName + " is moved from:"
              + oldQueue.getQueuePath() + " to:" + newQueue.getQueuePath()
              + " after refresh, which is not allowed.");
        }
      }
    }
  }

  /**
   * Add the new queues (only) to our list of queues...
   * ... be careful, do not overwrite existing queues.
   * @param queues
   * @param newQueues
   */
  @Lock(CapacityScheduler.class)
  private void addNewQueues(
      Map<String, CSQueue> queues, Map<String, CSQueue> newQueues) 
  {
    for (Map.Entry<String, CSQueue> e : newQueues.entrySet()) {
      String queueName = e.getKey();
      CSQueue queue = e.getValue();
      if (!queues.containsKey(queueName)) {
        queues.put(queueName, queue);
      }
    }
  }
  
  @Lock(CapacityScheduler.class)
  static CSQueue parseQueue(
      CapacitySchedulerContext csContext,
      CapacitySchedulerConfiguration conf, 
      CSQueue parent, String queueName, Map<String, CSQueue> queues,
      Map<String, CSQueue> oldQueues, 
      QueueHook hook) throws IOException {
    CSQueue queue;
    String fullQueueName =
        (parent == null) ? queueName
            : (parent.getQueuePath() + "." + queueName);
    String[] childQueueNames = 
      conf.getQueues(fullQueueName);
    boolean isReservableQueue = conf.isReservable(fullQueueName);
    if (childQueueNames == null || childQueueNames.length == 0) {
      if (null == parent) {
        throw new IllegalStateException(
            "Queue configuration missing child queue names for " + queueName);
      }
      // Check if the queue will be dynamically managed by the Reservation
      // system
      if (isReservableQueue) {
        queue =
            new PlanQueue(csContext, queueName, parent,
                oldQueues.get(queueName));
      } else {
        queue =
            new LeafQueue(csContext, queueName, parent,
                oldQueues.get(queueName));

        // Used only for unit tests
        queue = hook.hook(queue);
      }
    } else {
      if (isReservableQueue) {
        throw new IllegalStateException(
            "Only Leaf Queues can be reservable for " + queueName);
      }
      ParentQueue parentQueue = 
        new ParentQueue(csContext, queueName, parent, oldQueues.get(queueName));

      // Used only for unit tests
      queue = hook.hook(parentQueue);

      List<CSQueue> childQueues = new ArrayList<CSQueue>();
      for (String childQueueName : childQueueNames) {
        CSQueue childQueue = 
          parseQueue(csContext, conf, queue, childQueueName, 
              queues, oldQueues, hook);
        childQueues.add(childQueue);
      }
      parentQueue.setChildQueues(childQueues);
    }

    if(queue instanceof LeafQueue == true && queues.containsKey(queueName)
      && queues.get(queueName) instanceof LeafQueue == true) {
      throw new IOException("Two leaf queues were named " + queueName
        + ". Leaf queue names must be distinct");
    }
    queues.put(queueName, queue);

    LOG.info("Initialized queue: " + queue);
    return queue;
  }

  public CSQueue getQueue(String queueName) {
    if (queueName == null) {
      return null;
    }
    return queues.get(queueName);
  }

  private synchronized void addApplicationOnRecovery(
      ApplicationId applicationId, String queueName, String user,
      Priority priority) {
    CSQueue queue = getQueue(queueName);
    if (queue == null) {
      //During a restart, this indicates a queue was removed, which is
      //not presently supported
      if (!YarnConfiguration.shouldRMFailFast(getConfig())) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMAppEvent(applicationId, RMAppEventType.KILL,
            "Application killed on recovery as it was submitted to queue " +
            queueName + " which no longer exists after restart."));
        return;
      } else {
        String queueErrorMsg = "Queue named " + queueName
            + " missing during application recovery."
            + " Queue removal during recovery is not presently supported by the"
            + " capacity scheduler, please restart with all queues configured"
            + " which were present before shutdown/restart.";
        LOG.fatal(queueErrorMsg);
        throw new QueueInvalidException(queueErrorMsg);
      }
    }
    if (!(queue instanceof LeafQueue)) {
      // During RM restart, this means leaf queue was converted to a parent
      // queue, which is not supported for running apps.
      if (!YarnConfiguration.shouldRMFailFast(getConfig())) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMAppEvent(applicationId, RMAppEventType.KILL,
            "Application killed on recovery as it was submitted to queue " +
            queueName + " which is no longer a leaf queue after restart."));
        return;
      } else {
        String queueErrorMsg = "Queue named " + queueName
            + " is no longer a leaf queue during application recovery."
            + " Changing a leaf queue to a parent queue during recovery is"
            + " not presently supported by the capacity scheduler. Please"
            + " restart with leaf queues before shutdown/restart continuing"
            + " as leaf queues.";
        LOG.fatal(queueErrorMsg);
        throw new QueueInvalidException(queueErrorMsg);
      }
    }
    // Submit to the queue
    try {
      queue.submitApplication(applicationId, user, queueName);
    } catch (AccessControlException ace) {
      // Ignore the exception for recovered app as the app was previously
      // accepted.
    }
    queue.getMetrics().submitApp(user);
    SchedulerApplication<FiCaSchedulerApp> application =
        new SchedulerApplication<FiCaSchedulerApp>(queue, user, priority);
    applications.put(applicationId, application);
    LOG.info("Accepted application " + applicationId + " from user: " + user
        + ", in queue: " + queueName);
    if (LOG.isDebugEnabled()) {
      LOG.debug(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
    }
  }

  private synchronized void addApplication(ApplicationId applicationId,
      String queueName, String user, Priority priority) {
    // Sanity checks.
    CSQueue queue = getQueue(queueName);
    if (queue == null) {
      String message = "Application " + applicationId +
      " submitted by user " + user + " to unknown queue: " + queueName;
      this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppEvent(applicationId,
              RMAppEventType.APP_REJECTED, message));
      return;
    }
    if (!(queue instanceof LeafQueue)) {
      String message = "Application " + applicationId + 
          " submitted by user " + user + " to non-leaf queue: " + queueName;
      this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppEvent(applicationId,
              RMAppEventType.APP_REJECTED, message));
      return;
    }
    // Submit to the queue
    try {
      queue.submitApplication(applicationId, user, queueName);
    } catch (AccessControlException ace) {
      LOG.info("Failed to submit application " + applicationId + " to queue "
          + queueName + " from user " + user, ace);
      this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppEvent(applicationId,
              RMAppEventType.APP_REJECTED, ace.toString()));
      return;
    }
    // update the metrics
    queue.getMetrics().submitApp(user);
    SchedulerApplication<FiCaSchedulerApp> application =
        new SchedulerApplication<FiCaSchedulerApp>(queue, user, priority);
    applications.put(applicationId, application);
    LOG.info("Accepted application " + applicationId + " from user: " + user
        + ", in queue: " + queueName);
    rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
  }

  private synchronized void addApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      boolean transferStateFromPreviousAttempt,
      boolean isAttemptRecovering) {
    SchedulerApplication<FiCaSchedulerApp> application =
        applications.get(applicationAttemptId.getApplicationId());
    if (application == null) {
      LOG.warn("Application " + applicationAttemptId.getApplicationId() +
          " cannot be found in scheduler.");
      return;
    }
    CSQueue queue = (CSQueue) application.getQueue();

    FiCaSchedulerApp attempt = new FiCaSchedulerApp(applicationAttemptId,
        application.getUser(), queue, queue.getActiveUsersManager(), rmContext,
        application.getPriority());
    if (transferStateFromPreviousAttempt) {
      attempt.transferStateFromPreviousAttempt(
          application.getCurrentAppAttempt());
    }
    application.setCurrentAppAttempt(attempt);

    // Update attempt priority to the latest to avoid race condition i.e
    // SchedulerApplicationAttempt is created with old priority but it is not
    // set to SchedulerApplication#setCurrentAppAttempt.
    // Scenario would occur is
    // 1. SchdulerApplicationAttempt is created with old priority.
    // 2. updateApplicationPriority() updates SchedulerApplication. Since
    // currentAttempt is null, it just return.
    // 3. ScheduelerApplcationAttempt is set in
    // SchedulerApplication#setCurrentAppAttempt.
    attempt.setPriority(application.getPriority());

    queue.submitApplicationAttempt(attempt, application.getUser());
    LOG.info("Added Application Attempt " + applicationAttemptId
        + " to scheduler from user " + application.getUser() + " in queue "
        + queue.getQueueName());
    if (isAttemptRecovering) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(applicationAttemptId
            + " is recovering. Skipping notifying ATTEMPT_ADDED");
      }
    } else {
      rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(applicationAttemptId,
            RMAppAttemptEventType.ATTEMPT_ADDED));
    }
  }

  private synchronized void doneApplication(ApplicationId applicationId,
      RMAppState finalState) {
    SchedulerApplication<FiCaSchedulerApp> application =
        applications.get(applicationId);
    if (application == null){
      // The AppRemovedSchedulerEvent maybe sent on recovery for completed apps,
      // ignore it.
      LOG.warn("Couldn't find application " + applicationId);
      return;
    }
    CSQueue queue = (CSQueue) application.getQueue();
    if (!(queue instanceof LeafQueue)) {
      LOG.error("Cannot finish application " + "from non-leaf queue: "
          + queue.getQueueName());
    } else {
      queue.finishApplication(applicationId, application.getUser());
    }
    application.stop(finalState);
    applications.remove(applicationId);
  }

  private synchronized void doneApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers) {
    LOG.info("Application Attempt " + applicationAttemptId + " is done." +
        " finalState=" + rmAppAttemptFinalState);
    
    FiCaSchedulerApp attempt = getApplicationAttempt(applicationAttemptId);
    SchedulerApplication<FiCaSchedulerApp> application =
        applications.get(applicationAttemptId.getApplicationId());

    if (application == null || attempt == null) {
      LOG.info("Unknown application " + applicationAttemptId + " has completed!");
      return;
    }

    // Release all the allocated, acquired, running containers
    for (RMContainer rmContainer : attempt.getLiveContainers()) {
      if (keepContainers
          && rmContainer.getState().equals(RMContainerState.RUNNING)) {
        // do not kill the running container in the case of work-preserving AM
        // restart.
        LOG.info("Skip killing " + rmContainer.getContainerId());
        continue;
      }
      completedContainer(
        rmContainer,
        SchedulerUtils.createAbnormalContainerStatus(
          rmContainer.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION),
        RMContainerEventType.KILL);
    }

    // Release all reserved containers
    for (RMContainer rmContainer : attempt.getReservedContainers()) {
      completedContainer(
        rmContainer,
        SchedulerUtils.createAbnormalContainerStatus(
          rmContainer.getContainerId(), "Application Complete"),
        RMContainerEventType.KILL);
    }

    // Clean up pending requests, metrics etc.
    attempt.stop(rmAppAttemptFinalState);

    // Inform the queue
    String queueName = attempt.getQueue().getQueueName();
    CSQueue queue = queues.get(queueName);
    if (!(queue instanceof LeafQueue)) {
      LOG.error("Cannot finish application " + "from non-leaf queue: "
          + queueName);
    } else {
      queue.finishApplicationAttempt(attempt, queue.getQueueName());
    }
  }

  @Override
  // Note: when AM asks to decrease container or release container, we will
  // acquire scheduler lock
  @Lock(Lock.NoLock.class)
  public Allocation allocate(ApplicationAttemptId applicationAttemptId,
      List<ResourceRequest> ask, List<ContainerId> release,
      List<String> blacklistAdditions, List<String> blacklistRemovals,
      List<ContainerResourceChangeRequest> increaseRequests,
      List<ContainerResourceChangeRequest> decreaseRequests) {

    FiCaSchedulerApp application = getApplicationAttempt(applicationAttemptId);
    if (application == null) {
      return EMPTY_ALLOCATION;
    }
    
    // Sanity check
    SchedulerUtils.normalizeRequests(
        ask, getResourceCalculator(), getClusterResource(),
        getMinimumResourceCapability(), getMaximumResourceCapability());
    
    // Pre-process increase requests
    List<SchedContainerChangeRequest> normalizedIncreaseRequests =
        checkAndNormalizeContainerChangeRequests(increaseRequests, true);
    
    // Pre-process decrease requests
    List<SchedContainerChangeRequest> normalizedDecreaseRequests =
        checkAndNormalizeContainerChangeRequests(decreaseRequests, false);

    // Release containers
    releaseContainers(release, application);

    Allocation allocation;

    LeafQueue updateDemandForQueue = null;

    synchronized (application) {

      // make sure we aren't stopping/removing the application
      // when the allocate comes in
      if (application.isStopped()) {
        return EMPTY_ALLOCATION;
      }

      // Process resource requests
      if (!ask.isEmpty()) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("allocate: pre-update " + applicationAttemptId +
              " ask size =" + ask.size());
          application.showRequests();
        }

        // Update application requests
        if (application.updateResourceRequests(ask)) {
          updateDemandForQueue = (LeafQueue) application.getQueue();
        }

        if(LOG.isDebugEnabled()) {
          LOG.debug("allocate: post-update");
          application.showRequests();
        }
      }
      
      // Process increase resource requests
      if (application.updateIncreaseRequests(normalizedIncreaseRequests)
          && (updateDemandForQueue == null)) {
        updateDemandForQueue = (LeafQueue) application.getQueue();
      }

      if (application.isWaitingForAMContainer()) {
        // Allocate is for AM and update AM blacklist for this
        application.updateAMBlacklist(
            blacklistAdditions, blacklistRemovals);
      } else {
        application.updateBlacklist(blacklistAdditions, blacklistRemovals);
      }
      
      // Decrease containers
      decreaseContainers(normalizedDecreaseRequests, application);

      allocation = application.getAllocation(getResourceCalculator(),
                   clusterResource, getMinimumResourceCapability());
    }

    if (updateDemandForQueue != null) {
      updateDemandForQueue.getOrderingPolicy().demandUpdated(application);
    }

    return allocation;

  }

  @Override
  @Lock(Lock.NoLock.class)
  public QueueInfo getQueueInfo(String queueName, 
      boolean includeChildQueues, boolean recursive) 
  throws IOException {
    CSQueue queue = null;
    queue = this.queues.get(queueName);
    if (queue == null) {
      throw new IOException("Unknown queue: " + queueName);
    }
    return queue.getQueueInfo(includeChildQueues, recursive);
  }

  @Override
  @Lock(Lock.NoLock.class)
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    UserGroupInformation user = null;
    try {
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      // should never happen
      return new ArrayList<QueueUserACLInfo>();
    }

    return root.getQueueUserAclInfo(user);
  }

  private synchronized void nodeUpdate(RMNode nm) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("nodeUpdate: " + nm + " clusterResources: " + clusterResource);
    }

    Resource releaseResources = Resource.newInstance(0, 0);

    FiCaSchedulerNode node = getNode(nm.getNodeID());
    
    List<UpdatedContainerInfo> containerInfoList = nm.pullContainerUpdates();
    List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    for(UpdatedContainerInfo containerInfo : containerInfoList) {
      newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
      completedContainers.addAll(containerInfo.getCompletedContainers());
    }
    
    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(), node);
    }
    
    // Processing the newly increased containers
    List<Container> newlyIncreasedContainers =
        nm.pullNewlyIncreasedContainers();
    for (Container container : newlyIncreasedContainers) {
      containerIncreasedOnNode(container.getId(), node, container);
    }

    // Process completed containers
    int releasedContainers = 0;
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      RMContainer container = getRMContainer(containerId);
      completedContainer(container, completedContainer,
        RMContainerEventType.FINISHED);
      if (container != null) {
        releasedContainers++;
        Resource rs = container.getAllocatedResource();
        if (rs != null) {
          Resources.addTo(releaseResources, rs);
        }
        rs = container.getReservedResource();
        if (rs != null) {
          Resources.addTo(releaseResources, rs);
        }
      }
    }

    schedulerHealth.updateSchedulerReleaseDetails(lastNodeUpdateTime,
      releaseResources);
    schedulerHealth.updateSchedulerReleaseCounts(releasedContainers);

    // Updating node resource utilization
    node.setAggregatedContainersUtilization(
        nm.getAggregatedContainersUtilization());
    node.setNodeUtilization(nm.getNodeUtilization());

    // Now node data structures are upto date and ready for scheduling.
    if(LOG.isDebugEnabled()) {
      LOG.debug("Node being looked for scheduling " + nm
        + " availableResource: " + node.getAvailableResource());
    }
  }
  
  /**
   * Process resource update on a node.
   */
  private synchronized void updateNodeAndQueueResource(RMNode nm, 
      ResourceOption resourceOption) {
    updateNodeResource(nm, resourceOption);
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));
  }
  
  /**
   * Process node labels update on a node.
   */
  private synchronized void updateLabelsOnNode(NodeId nodeId,
      Set<String> newLabels) {
    FiCaSchedulerNode node = nodes.get(nodeId);
    if (null == node) {
      return;
    }
    
    // Get new partition, we have only one partition per node
    String newPartition;
    if (newLabels.isEmpty()) {
      newPartition = RMNodeLabelsManager.NO_LABEL;
    } else {
      newPartition = newLabels.iterator().next();
    }

    // old partition as well
    String oldPartition = node.getPartition();

    // Update resources of these containers
    for (RMContainer rmContainer : node.getRunningContainers()) {
      FiCaSchedulerApp application =
          getApplicationAttempt(rmContainer.getApplicationAttemptId());
      if (null != application) {
        application.nodePartitionUpdated(rmContainer, oldPartition,
            newPartition);
      } else {
        LOG.warn("There's something wrong, some RMContainers running on"
            + " a node, but we cannot find SchedulerApplicationAttempt for it. Node="
            + node.getNodeID() + " applicationAttemptId="
            + rmContainer.getApplicationAttemptId());
        continue;
      }
    }
    
    // Unreserve container on this node
    RMContainer reservedContainer = node.getReservedContainer();
    if (null != reservedContainer) {
      dropContainerReservation(reservedContainer);
    }
    
    // Update node labels after we've done this
    node.updateLabels(newLabels);
  }

  private void updateSchedulerHealth(long now, FiCaSchedulerNode node,
      CSAssignment assignment) {

    NodeId nodeId = node.getNodeID();
    List<AssignmentInformation.AssignmentDetails> allocations =
        assignment.getAssignmentInformation().getAllocationDetails();
    List<AssignmentInformation.AssignmentDetails> reservations =
        assignment.getAssignmentInformation().getReservationDetails();
    if (!allocations.isEmpty()) {
      ContainerId allocatedContainerId =
          allocations.get(allocations.size() - 1).containerId;
      String allocatedQueue = allocations.get(allocations.size() - 1).queue;
      schedulerHealth.updateAllocation(now, nodeId, allocatedContainerId,
        allocatedQueue);
    }
    if (!reservations.isEmpty()) {
      ContainerId reservedContainerId =
          reservations.get(reservations.size() - 1).containerId;
      String reservedQueue = reservations.get(reservations.size() - 1).queue;
      schedulerHealth.updateReservation(now, nodeId, reservedContainerId,
        reservedQueue);
    }
    schedulerHealth.updateSchedulerReservationCounts(assignment
      .getAssignmentInformation().getNumReservations());
    schedulerHealth.updateSchedulerAllocationCounts(assignment
      .getAssignmentInformation().getNumAllocations());
    schedulerHealth.updateSchedulerRunDetails(now, assignment
      .getAssignmentInformation().getAllocated(), assignment
      .getAssignmentInformation().getReserved());
 }

  private synchronized void allocateContainersToNode(FiCaSchedulerNode node) {
    if (rmContext.isWorkPreservingRecoveryEnabled()
        && !rmContext.isSchedulerReadyForAllocatingContainers()) {
      return;
    }
    // reset allocation and reservation stats before we start doing any work
    updateSchedulerHealth(lastNodeUpdateTime, node,
      new CSAssignment(Resources.none(), NodeType.NODE_LOCAL));

    CSAssignment assignment;

    // Assign new containers...
    // 1. Check for reserved applications
    // 2. Schedule if there are no reservations

    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      FiCaSchedulerApp reservedApplication =
          getCurrentAttemptForContainer(reservedContainer.getContainerId());

      // Try to fulfill the reservation
      LOG.info("Trying to fulfill reservation for application "
          + reservedApplication.getApplicationId() + " on node: "
          + node.getNodeID());

      LeafQueue queue = ((LeafQueue) reservedApplication.getQueue());
      assignment =
          queue.assignContainers(
              clusterResource,
              node,
              // TODO, now we only consider limits for parent for non-labeled
              // resources, should consider labeled resources as well.
              new ResourceLimits(labelManager.getResourceByLabel(
                  RMNodeLabelsManager.NO_LABEL, clusterResource)),
              SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      if (assignment.isFulfilledReservation()) {
        CSAssignment tmp =
            new CSAssignment(reservedContainer.getReservedResource(),
                assignment.getType());
        Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
            reservedContainer.getReservedResource());
        tmp.getAssignmentInformation().addAllocationDetails(
            reservedContainer.getContainerId(), queue.getQueuePath());
        tmp.getAssignmentInformation().incrAllocations();
        updateSchedulerHealth(lastNodeUpdateTime, node, tmp);
        schedulerHealth.updateSchedulerFulfilledReservationCounts(1);
      }
    }

    // Try to schedule more if there are no reservations to fulfill
    if (node.getReservedContainer() == null) {
      if (calculator.computeAvailableContainers(node.getAvailableResource(),
        minimumAllocation) > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Trying to schedule on node: " + node.getNodeName() +
              ", available: " + node.getAvailableResource());
        }

        assignment = root.assignContainers(
            clusterResource,
            node,
            // TODO, now we only consider limits for parent for non-labeled
            // resources, should consider labeled resources as well.
            new ResourceLimits(labelManager.getResourceByLabel(
                RMNodeLabelsManager.NO_LABEL, clusterResource)),
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
        if (Resources.greaterThan(calculator, clusterResource,
            assignment.getResource(), Resources.none())) {
          updateSchedulerHealth(lastNodeUpdateTime, node, assignment);
          return;
        }
        
        // Only do non-exclusive allocation when node has node-labels.
        if (StringUtils.equals(node.getPartition(),
            RMNodeLabelsManager.NO_LABEL)) {
          return;
        }
        
        // Only do non-exclusive allocation when the node-label supports that
        try {
          if (rmContext.getNodeLabelManager().isExclusiveNodeLabel(
              node.getPartition())) {
            return;
          }
        } catch (IOException e) {
          LOG.warn("Exception when trying to get exclusivity of node label="
              + node.getPartition(), e);
          return;
        }
        
        // Try to use NON_EXCLUSIVE
        assignment = root.assignContainers(
            clusterResource,
            node,
            // TODO, now we only consider limits for parent for non-labeled
            // resources, should consider labeled resources as well.
            new ResourceLimits(labelManager.getResourceByLabel(
                RMNodeLabelsManager.NO_LABEL, clusterResource)),
            SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY);
        updateSchedulerHealth(lastNodeUpdateTime, node, assignment);
      }
    } else {
      LOG.info("Skipping scheduling since node "
          + node.getNodeID()
          + " is reserved by application "
          + node.getReservedContainer().getContainerId()
              .getApplicationAttemptId());
    }
  }

  @Override
  public void handle(SchedulerEvent event) {
    switch(event.getType()) {
    case NODE_ADDED:
    {
      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
      addNode(nodeAddedEvent.getAddedRMNode());
      recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
        nodeAddedEvent.getAddedRMNode());
    }
    break;
    case NODE_REMOVED:
    {
      NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;
      removeNode(nodeRemovedEvent.getRemovedRMNode());
    }
    break;
    case NODE_RESOURCE_UPDATE:
    {
      NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent = 
          (NodeResourceUpdateSchedulerEvent)event;
      updateNodeAndQueueResource(nodeResourceUpdatedEvent.getRMNode(),
        nodeResourceUpdatedEvent.getResourceOption());
    }
    break;
    case NODE_LABELS_UPDATE:
    {
      NodeLabelsUpdateSchedulerEvent labelUpdateEvent =
          (NodeLabelsUpdateSchedulerEvent) event;
      
      for (Entry<NodeId, Set<String>> entry : labelUpdateEvent
          .getUpdatedNodeToLabels().entrySet()) {
        NodeId id = entry.getKey();
        Set<String> labels = entry.getValue();
        updateLabelsOnNode(id, labels);
      }
    }
    break;
    case NODE_UPDATE:
    {
      NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
      RMNode node = nodeUpdatedEvent.getRMNode();
      setLastNodeUpdateTime(Time.now());
      nodeUpdate(node);
      if (!scheduleAsynchronously) {
        allocateContainersToNode(getNode(node.getNodeID()));
      }
    }
    break;
    case APP_ADDED:
    {
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
      String queueName = resolveReservationQueueName(appAddedEvent.getQueue(),
          appAddedEvent.getApplicationId(), appAddedEvent.getReservationID(),
          appAddedEvent.getIsAppRecovering());
      if (queueName != null) {
        if (!appAddedEvent.getIsAppRecovering()) {
          addApplication(appAddedEvent.getApplicationId(), queueName,
              appAddedEvent.getUser(), appAddedEvent.getApplicatonPriority());
        } else {
          addApplicationOnRecovery(appAddedEvent.getApplicationId(), queueName,
              appAddedEvent.getUser(), appAddedEvent.getApplicatonPriority());
        }
      }
    }
    break;
    case APP_REMOVED:
    {
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      doneApplication(appRemovedEvent.getApplicationID(),
        appRemovedEvent.getFinalState());
    }
    break;
    case APP_ATTEMPT_ADDED:
    {
      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
      addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
        appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
        appAttemptAddedEvent.getIsAttemptRecovering());
    }
    break;
    case APP_ATTEMPT_REMOVED:
    {
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
      doneApplicationAttempt(appAttemptRemovedEvent.getApplicationAttemptID(),
        appAttemptRemovedEvent.getFinalAttemptState(),
        appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
    }
    break;
    case CONTAINER_EXPIRED:
    {
      ContainerExpiredSchedulerEvent containerExpiredEvent = 
          (ContainerExpiredSchedulerEvent) event;
      ContainerId containerId = containerExpiredEvent.getContainerId();
      completedContainer(getRMContainer(containerId), 
          SchedulerUtils.createAbnormalContainerStatus(
              containerId, 
              SchedulerUtils.EXPIRED_CONTAINER), 
          RMContainerEventType.EXPIRE);
    }
    break;
    case DROP_RESERVATION:
    {
      ContainerPreemptEvent dropReservationEvent = (ContainerPreemptEvent)event;
      RMContainer container = dropReservationEvent.getContainer();
      dropContainerReservation(container);
    }
    break;
    case PREEMPT_CONTAINER:
    {
      ContainerPreemptEvent preemptContainerEvent =
          (ContainerPreemptEvent)event;
      ApplicationAttemptId aid = preemptContainerEvent.getAppId();
      RMContainer containerToBePreempted = preemptContainerEvent.getContainer();
      preemptContainer(aid, containerToBePreempted);
    }
    break;
    case KILL_CONTAINER:
    {
      ContainerPreemptEvent killContainerEvent = (ContainerPreemptEvent)event;
      RMContainer containerToBeKilled = killContainerEvent.getContainer();
      killContainer(containerToBeKilled);
    }
    break;
    case CONTAINER_RESCHEDULED:
    {
      ContainerRescheduledEvent containerRescheduledEvent =
          (ContainerRescheduledEvent) event;
      RMContainer container = containerRescheduledEvent.getContainer();
      recoverResourceRequestForContainer(container);
    }
    break;
    default:
      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }

  private synchronized void addNode(RMNode nodeManager) {
    FiCaSchedulerNode schedulerNode = new FiCaSchedulerNode(nodeManager,
        usePortForNodeName, nodeManager.getNodeLabels());
    this.nodes.put(nodeManager.getNodeID(), schedulerNode);
    Resources.addTo(clusterResource, schedulerNode.getTotalResource());

    // update this node to node label manager
    if (labelManager != null) {
      labelManager.activateNode(nodeManager.getNodeID(),
          schedulerNode.getTotalResource());
    }
    
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));
    int numNodes = numNodeManagers.incrementAndGet();
    updateMaximumAllocation(schedulerNode, true);
    
    LOG.info("Added node " + nodeManager.getNodeAddress() + 
        " clusterResource: " + clusterResource);

    if (scheduleAsynchronously && numNodes == 1) {
      asyncSchedulerThread.beginSchedule();
    }
  }

  private synchronized void removeNode(RMNode nodeInfo) {
    // update this node to node label manager
    if (labelManager != null) {
      labelManager.deactivateNode(nodeInfo.getNodeID());
    }
    
    FiCaSchedulerNode node = nodes.get(nodeInfo.getNodeID());
    if (node == null) {
      return;
    }
    Resources.subtractFrom(clusterResource, node.getTotalResource());
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));
    int numNodes = numNodeManagers.decrementAndGet();

    if (scheduleAsynchronously && numNodes == 0) {
      asyncSchedulerThread.suspendSchedule();
    }
    
    // Remove running containers
    List<RMContainer> runningContainers = node.getRunningContainers();
    for (RMContainer container : runningContainers) {
      completedContainer(container, 
          SchedulerUtils.createAbnormalContainerStatus(
              container.getContainerId(), 
              SchedulerUtils.LOST_CONTAINER), 
          RMContainerEventType.KILL);
    }
    
    // Remove reservations, if any
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      completedContainer(reservedContainer, 
          SchedulerUtils.createAbnormalContainerStatus(
              reservedContainer.getContainerId(), 
              SchedulerUtils.LOST_CONTAINER), 
          RMContainerEventType.KILL);
    }

    this.nodes.remove(nodeInfo.getNodeID());
    updateMaximumAllocation(node, false);

    LOG.info("Removed node " + nodeInfo.getNodeAddress() + 
        " clusterResource: " + clusterResource);
  }
  
  @Lock(CapacityScheduler.class)
  @Override
  protected synchronized void completedContainer(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    if (rmContainer == null) {
      LOG.info("Container " + containerStatus.getContainerId() +
          " completed with event " + event);
      return;
    }
    
    Container container = rmContainer.getContainer();
    
    // Get the application for the finished container
    FiCaSchedulerApp application =
        getCurrentAttemptForContainer(container.getId());
    ApplicationId appId =
        container.getId().getApplicationAttemptId().getApplicationId();
    if (application == null) {
      LOG.info("Container " + container + " of" + " finished application "
          + appId + " completed with event " + event);
      return;
    }
    
    // Get the node on which the container was allocated
    FiCaSchedulerNode node = getNode(container.getNodeId());
    
    // Inform the queue
    LeafQueue queue = (LeafQueue)application.getQueue();
    queue.completedContainer(clusterResource, application, node, 
        rmContainer, containerStatus, event, null, true);

    if (containerStatus.getExitStatus() == ContainerExitStatus.PREEMPTED) {
      schedulerHealth.updatePreemption(Time.now(), container.getNodeId(),
        container.getId(), queue.getQueuePath());
      schedulerHealth.updateSchedulerPreemptionCounts(1);
    } else {
      schedulerHealth.updateRelease(lastNodeUpdateTime, container.getNodeId(),
        container.getId(), queue.getQueuePath());
    }
  }
  
  @Lock(CapacityScheduler.class)
  @Override
  protected synchronized void decreaseContainer(
      SchedContainerChangeRequest decreaseRequest,
      SchedulerApplicationAttempt attempt) {
    RMContainer rmContainer = decreaseRequest.getRMContainer();

    // Check container status before doing decrease
    if (rmContainer.getState() != RMContainerState.RUNNING) {
      LOG.info("Trying to decrease a container not in RUNNING state, container="
          + rmContainer + " state=" + rmContainer.getState().name());
      return;
    }
    
    // Delta capacity of this decrease request is 0, this decrease request may
    // just to cancel increase request
    if (Resources.equals(decreaseRequest.getDeltaCapacity(), Resources.none())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Decrease target resource equals to existing resource for container:"
            + decreaseRequest.getContainerId()
            + " ignore this decrease request.");
      }
      return;
    }

    // Save resource before decrease
    Resource resourceBeforeDecrease =
        Resources.clone(rmContainer.getContainer().getResource());

    FiCaSchedulerApp app = (FiCaSchedulerApp)attempt;
    LeafQueue queue = (LeafQueue) attempt.getQueue();
    queue.decreaseContainer(clusterResource, decreaseRequest, app);
    
    // Notify RMNode the container will be decreased
    this.rmContext.getDispatcher().getEventHandler()
        .handle(new RMNodeDecreaseContainerEvent(decreaseRequest.getNodeId(),
            Arrays.asList(rmContainer.getContainer())));
    
    LOG.info("Application attempt " + app.getApplicationAttemptId()
        + " decreased container:" + decreaseRequest.getContainerId() + " from "
        + resourceBeforeDecrease + " to "
        + decreaseRequest.getTargetCapacity());
  }

  @Lock(Lock.NoLock.class)
  @VisibleForTesting
  @Override
  public FiCaSchedulerApp getApplicationAttempt(
      ApplicationAttemptId applicationAttemptId) {
    return super.getApplicationAttempt(applicationAttemptId);
  }
  
  @Lock(Lock.NoLock.class)
  public FiCaSchedulerNode getNode(NodeId nodeId) {
    return nodes.get(nodeId);
  }
  
  @Lock(Lock.NoLock.class)
  Map<NodeId, FiCaSchedulerNode> getAllNodes() {
    return nodes;
  }

  @Override
  @Lock(Lock.NoLock.class)
  public void recover(RMState state) throws Exception {
    // NOT IMPLEMENTED
  }

  @Override
  public void dropContainerReservation(RMContainer container) {
    if(LOG.isDebugEnabled()){
      LOG.debug("DROP_RESERVATION:" + container.toString());
    }
    completedContainer(container,
        SchedulerUtils.createAbnormalContainerStatus(
            container.getContainerId(),
            SchedulerUtils.UNRESERVED_CONTAINER),
        RMContainerEventType.KILL);
  }

  @Override
  public void preemptContainer(ApplicationAttemptId aid, RMContainer cont) {
    if(LOG.isDebugEnabled()){
      LOG.debug("PREEMPT_CONTAINER: application:" + aid.toString() +
          " container: " + cont.toString());
    }
    FiCaSchedulerApp app = getApplicationAttempt(aid);
    if (app != null) {
      app.addPreemptContainer(cont.getContainerId());
    }
  }

  @Override
  public void killContainer(RMContainer cont) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("KILL_CONTAINER: container" + cont.toString());
    }
    completedContainer(cont, SchedulerUtils.createPreemptedContainerStatus(
      cont.getContainerId(), SchedulerUtils.PREEMPTED_CONTAINER),
      RMContainerEventType.KILL);
  }

  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    CSQueue queue = getQueue(queueName);
    if (queue == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ACL not found for queue access-type " + acl
            + " for queue " + queueName);
      }
      return false;
    }
    return queue.hasAccess(acl, callerUGI);
  }

  @Override
  public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
    CSQueue queue = queues.get(queueName);
    if (queue == null) {
      return null;
    }
    List<ApplicationAttemptId> apps = new ArrayList<ApplicationAttemptId>();
    queue.collectSchedulerApplications(apps);
    return apps;
  }

  private CapacitySchedulerConfiguration loadCapacitySchedulerConfiguration(
      Configuration configuration) throws IOException {
    try {
      InputStream CSInputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(configuration,
                  YarnConfiguration.CS_CONFIGURATION_FILE);
      if (CSInputStream != null) {
        configuration.addResource(CSInputStream);
        return new CapacitySchedulerConfiguration(configuration, false);
      }
      return new CapacitySchedulerConfiguration(configuration, true);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private String getDefaultReservationQueueName(String planQueueName) {
    return planQueueName + ReservationConstants.DEFAULT_QUEUE_SUFFIX;
  }

  private synchronized String resolveReservationQueueName(String queueName,
      ApplicationId applicationId, ReservationId reservationID,
      boolean isRecovering) {
    CSQueue queue = getQueue(queueName);
    // Check if the queue is a plan queue
    if ((queue == null) || !(queue instanceof PlanQueue)) {
      return queueName;
    }
    if (reservationID != null) {
      String resQName = reservationID.toString();
      queue = getQueue(resQName);
      if (queue == null) {
        // reservation has terminated during failover
        if (isRecovering
            && conf.getMoveOnExpiry(getQueue(queueName).getQueuePath())) {
          // move to the default child queue of the plan
          return getDefaultReservationQueueName(queueName);
        }
        String message =
            "Application " + applicationId
                + " submitted to a reservation which is not currently active: "
                + resQName;
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppEvent(applicationId,
                RMAppEventType.APP_REJECTED, message));
        return null;
      }
      if (!queue.getParent().getQueueName().equals(queueName)) {
        String message =
            "Application: " + applicationId + " submitted to a reservation "
                + resQName + " which does not belong to the specified queue: "
                + queueName;
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppEvent(applicationId,
                RMAppEventType.APP_REJECTED, message));
        return null;
      }
      // use the reservation queue to run the app
      queueName = resQName;
    } else {
      // use the default child queue of the plan for unreserved apps
      queueName = getDefaultReservationQueueName(queueName);
    }
    return queueName;
  }

  @Override
  public synchronized void removeQueue(String queueName)
      throws SchedulerDynamicEditException {
    LOG.info("Removing queue: " + queueName);
    CSQueue q = this.getQueue(queueName);
    if (!(q instanceof ReservationQueue)) {
      throw new SchedulerDynamicEditException("The queue that we are asked "
          + "to remove (" + queueName + ") is not a ReservationQueue");
    }
    ReservationQueue disposableLeafQueue = (ReservationQueue) q;
    // at this point we should have no more apps
    if (disposableLeafQueue.getNumApplications() > 0) {
      throw new SchedulerDynamicEditException("The queue " + queueName
          + " is not empty " + disposableLeafQueue.getApplications().size()
          + " active apps " + disposableLeafQueue.getPendingApplications().size()
          + " pending apps");
    }

    ((PlanQueue) disposableLeafQueue.getParent()).removeChildQueue(q);
    this.queues.remove(queueName);
    LOG.info("Removal of ReservationQueue " + queueName + " has succeeded");
  }

  @Override
  public synchronized void addQueue(Queue queue)
      throws SchedulerDynamicEditException {

    if (!(queue instanceof ReservationQueue)) {
      throw new SchedulerDynamicEditException("Queue " + queue.getQueueName()
          + " is not a ReservationQueue");
    }

    ReservationQueue newQueue = (ReservationQueue) queue;

    if (newQueue.getParent() == null
        || !(newQueue.getParent() instanceof PlanQueue)) {
      throw new SchedulerDynamicEditException("ParentQueue for "
          + newQueue.getQueueName()
          + " is not properly set (should be set and be a PlanQueue)");
    }

    PlanQueue parentPlan = (PlanQueue) newQueue.getParent();
    String queuename = newQueue.getQueueName();
    parentPlan.addChildQueue(newQueue);
    this.queues.put(queuename, newQueue);
    LOG.info("Creation of ReservationQueue " + newQueue + " succeeded");
  }

  @Override
  public synchronized void setEntitlement(String inQueue,
      QueueEntitlement entitlement) throws SchedulerDynamicEditException,
      YarnException {
    LeafQueue queue = getAndCheckLeafQueue(inQueue);
    ParentQueue parent = (ParentQueue) queue.getParent();

    if (!(queue instanceof ReservationQueue)) {
      throw new SchedulerDynamicEditException("Entitlement can not be"
          + " modified dynamically since queue " + inQueue
          + " is not a ReservationQueue");
    }

    if (!(parent instanceof PlanQueue)) {
      throw new SchedulerDynamicEditException("The parent of ReservationQueue "
          + inQueue + " must be an PlanQueue");
    }

    ReservationQueue newQueue = (ReservationQueue) queue;

    float sumChilds = ((PlanQueue) parent).sumOfChildCapacities();
    float newChildCap = sumChilds - queue.getCapacity() + entitlement.getCapacity();

    if (newChildCap >= 0 && newChildCap < 1.0f + CSQueueUtils.EPSILON) {
      // note: epsilon checks here are not ok, as the epsilons might accumulate
      // and become a problem in aggregate
      if (Math.abs(entitlement.getCapacity() - queue.getCapacity()) == 0
          && Math.abs(entitlement.getMaxCapacity() - queue.getMaximumCapacity()) == 0) {
        return;
      }
      newQueue.setEntitlement(entitlement);
    } else {
      throw new SchedulerDynamicEditException(
          "Sum of child queues would exceed 100% for PlanQueue: "
              + parent.getQueueName());
    }
    LOG.info("Set entitlement for ReservationQueue " + inQueue + "  to "
        + queue.getCapacity() + " request was (" + entitlement.getCapacity() + ")");
  }

  @Override
  public synchronized String moveApplication(ApplicationId appId,
      String targetQueueName) throws YarnException {
    FiCaSchedulerApp app =
        getApplicationAttempt(ApplicationAttemptId.newInstance(appId, 0));
    String sourceQueueName = app.getQueue().getQueueName();
    LeafQueue source = getAndCheckLeafQueue(sourceQueueName);
    String destQueueName = handleMoveToPlanQueue(targetQueueName);
    LeafQueue dest = getAndCheckLeafQueue(destQueueName);
    // Validation check - ACLs, submission limits for user & queue
    String user = app.getUser();
    try {
      dest.submitApplication(appId, user, destQueueName);
    } catch (AccessControlException e) {
      throw new YarnException(e);
    }
    // Move all live containers
    for (RMContainer rmContainer : app.getLiveContainers()) {
      source.detachContainer(clusterResource, app, rmContainer);
      // attach the Container to another queue
      dest.attachContainer(clusterResource, app, rmContainer);
    }
    // Detach the application..
    source.finishApplicationAttempt(app, sourceQueueName);
    source.getParent().finishApplication(appId, app.getUser());
    // Finish app & update metrics
    app.move(dest);
    // Submit to a new queue
    dest.submitApplicationAttempt(app, user);
    applications.get(appId).setQueue(dest);
    LOG.info("App: " + app.getApplicationId() + " successfully moved from "
        + sourceQueueName + " to: " + destQueueName);
    return targetQueueName;
  }

  /**
   * Check that the String provided in input is the name of an existing,
   * LeafQueue, if successful returns the queue.
   *
   * @param queue
   * @return the LeafQueue
   * @throws YarnException
   */
  private LeafQueue getAndCheckLeafQueue(String queue) throws YarnException {
    CSQueue ret = this.getQueue(queue);
    if (ret == null) {
      throw new YarnException("The specified Queue: " + queue
          + " doesn't exist");
    }
    if (!(ret instanceof LeafQueue)) {
      throw new YarnException("The specified Queue: " + queue
          + " is not a Leaf Queue. Move is supported only for Leaf Queues.");
    }
    return (LeafQueue) ret;
  }

  /** {@inheritDoc} */
  @Override
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes() {
    if (calculator.getClass().getName()
      .equals(DefaultResourceCalculator.class.getName())) {
      return EnumSet.of(SchedulerResourceTypes.MEMORY);
    }
    return EnumSet.of(SchedulerResourceTypes.MEMORY, SchedulerResourceTypes.CPU);
  }
  
  @Override
  public Resource getMaximumResourceCapability(String queueName) {
    CSQueue queue = getQueue(queueName);
    if (queue == null) {
      LOG.error("Unknown queue: " + queueName);
      return getMaximumResourceCapability();
    }
    if (!(queue instanceof LeafQueue)) {
      LOG.error("queue " + queueName + " is not an leaf queue");
      return getMaximumResourceCapability();
    }
    return ((LeafQueue)queue).getMaximumAllocation();
  }

  private String handleMoveToPlanQueue(String targetQueueName) {
    CSQueue dest = getQueue(targetQueueName);
    if (dest != null && dest instanceof PlanQueue) {
      // use the default child reservation queue of the plan
      targetQueueName = targetQueueName + ReservationConstants.DEFAULT_QUEUE_SUFFIX;
    }
    return targetQueueName;
  }

  @Override
  public Set<String> getPlanQueues() {
    Set<String> ret = new HashSet<String>();
    for (Map.Entry<String, CSQueue> l : queues.entrySet()) {
      if (l.getValue() instanceof PlanQueue) {
        ret.add(l.getKey());
      }
    }
    return ret;
  }

  public SchedulerHealth getSchedulerHealth() {
    return this.schedulerHealth;
  }

  private synchronized void setLastNodeUpdateTime(long time) {
    this.lastNodeUpdateTime = time;
  }

  @Override
  public Priority checkAndGetApplicationPriority(Priority priorityFromContext,
      String user, String queueName, ApplicationId applicationId)
      throws YarnException {
    Priority appPriority = null;

    // ToDo: Verify against priority ACLs

    // Verify the scenario where priority is null from submissionContext.
    if (null == priorityFromContext) {
      // Get the default priority for the Queue. If Queue is non-existent, then
      // use default priority
      priorityFromContext = getDefaultPriorityForQueue(queueName);

      LOG.info("Application '" + applicationId
          + "' is submitted without priority "
          + "hence considering default queue/cluster priority: "
          + priorityFromContext.getPriority());
    }

    // Verify whether submitted priority is lesser than max priority
    // in the cluster. If it is out of found, defining a max cap.
    if (priorityFromContext.compareTo(getMaxClusterLevelAppPriority()) < 0) {
      priorityFromContext = Priority
          .newInstance(getMaxClusterLevelAppPriority().getPriority());
    }

    appPriority = priorityFromContext;

    LOG.info("Priority '" + appPriority.getPriority()
        + "' is acceptable in queue : " + queueName + " for application: "
        + applicationId + " for the user: " + user);

    return appPriority;
  }

  private Priority getDefaultPriorityForQueue(String queueName) {
    Queue queue = getQueue(queueName);
    if (null == queue || null == queue.getDefaultApplicationPriority()) {
      // Return with default application priority
      return Priority.newInstance(CapacitySchedulerConfiguration
          .DEFAULT_CONFIGURATION_APPLICATION_PRIORITY);
    }

    return Priority.newInstance(queue.getDefaultApplicationPriority()
        .getPriority());
  }

  @Override
  public void updateApplicationPriority(Priority newPriority,
      ApplicationId applicationId) throws YarnException {
    Priority appPriority = null;
    SchedulerApplication<FiCaSchedulerApp> application = applications
        .get(applicationId);

    if (application == null) {
      throw new YarnException("Application '" + applicationId
          + "' is not present, hence could not change priority.");
    }

    RMApp rmApp = rmContext.getRMApps().get(applicationId);
    appPriority = checkAndGetApplicationPriority(newPriority, rmApp.getUser(),
        rmApp.getQueue(), applicationId);

    if (application.getPriority().equals(appPriority)) {
      return;
    }

    // Update new priority in Submission Context to keep track in HA
    rmApp.getApplicationSubmissionContext().setPriority(appPriority);

    // Update to state store
    ApplicationStateData appState =
        ApplicationStateData.newInstance(rmApp.getSubmitTime(),
            rmApp.getStartTime(), rmApp.getApplicationSubmissionContext(),
            rmApp.getUser(), rmApp.getCallerContext());
    rmContext.getStateStore().updateApplicationStateSynchronously(appState,
        false);

    // As we use iterator over a TreeSet for OrderingPolicy, once we change
    // priority then reinsert back to make order correct.
    LeafQueue queue = (LeafQueue) getQueue(rmApp.getQueue());
    synchronized (queue) {
      queue.getOrderingPolicy().removeSchedulableEntity(
          application.getCurrentAppAttempt());

      // Update new priority in SchedulerApplication
      application.setPriority(appPriority);

      queue.getOrderingPolicy().addSchedulableEntity(
          application.getCurrentAppAttempt());
    }

    // Update the changed application state to timeline server
    rmContext.getSystemMetricsPublisher().appUpdated(rmApp,
        System.currentTimeMillis());

    LOG.info("Priority '" + appPriority + "' is updated in queue :"
        + rmApp.getQueue() + " for application: " + applicationId
        + " for the user: " + rmApp.getUser());
  }
}
