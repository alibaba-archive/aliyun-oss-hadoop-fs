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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.nodelabels.NodeLabelTestBase;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NodeLabelsUtils;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestResourceTrackerService extends NodeLabelTestBase {

  private final static File TEMP_DIR = new File(System.getProperty(
      "test.build.data", "/tmp"), "decommision");
  private final File hostFile = new File(TEMP_DIR + File.separator + "hostFile.txt");
  private MockRM rm;

  /**
   * Test RM read NM next heartBeat Interval correctly from Configuration file,
   * and NM get next heartBeat Interval from RM correctly
   */
  @Test (timeout = 50000)
  public void testGetNextHeartBeatInterval() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");

    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);

    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(4000, nodeHeartbeat.getNextHeartBeatInterval());

    NodeHeartbeatResponse nodeHeartbeat2 = nm2.nodeHeartbeat(true);
    Assert.assertEquals(4000, nodeHeartbeat2.getNextHeartBeatInterval());

  }

  /**
   * Decommissioning using a pre-configured include hosts file
   */
  @Test
  public void testDecommissionWithIncludeHosts() throws Exception {

    writeToHostsFile("localhost", "host1", "host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());

    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    MockNM nm3 = rm.registerNode("localhost:4433", 1024);
    
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    assert(metrics != null);
    int metricCount = metrics.getNumDecommisionedNMs();

    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    // To test that IPs also work
    String ip = NetUtils.normalizeHostName("localhost");
    writeToHostsFile("host1", ip);

    rm.getNodesListManager().refreshNodes(conf);

    checkDecommissionedNMCount(rm, ++metricCount);

    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    Assert
        .assertEquals(1, ClusterMetrics.getMetrics().getNumDecommisionedNMs());

    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue("Node is not decommisioned.", NodeAction.SHUTDOWN
        .equals(nodeHeartbeat.getNodeAction()));

    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    Assert.assertEquals(metricCount, ClusterMetrics.getMetrics()
      .getNumDecommisionedNMs());
  }

  /**
   * Decommissioning using a pre-configured exclude hosts file
   */
  @Test
  public void testDecommissionWithExcludeHosts() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());

    writeToHostsFile("");
    final DrainDispatcher dispatcher = new DrainDispatcher();
    rm = new MockRM(conf) {
      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    MockNM nm3 = rm.registerNode("localhost:4433", 1024);

    dispatcher.await();

    int metricCount = ClusterMetrics.getMetrics().getNumDecommisionedNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    dispatcher.await();

    // To test that IPs also work
    String ip = NetUtils.normalizeHostName("localhost");
    writeToHostsFile("host2", ip);

    rm.getNodesListManager().refreshNodes(conf);

    checkDecommissionedNMCount(rm, metricCount + 2);

    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertTrue("The decommisioned metrics are not updated",
        NodeAction.SHUTDOWN.equals(nodeHeartbeat.getNodeAction()));

    nodeHeartbeat = nm3.nodeHeartbeat(true);
    Assert.assertTrue("The decommisioned metrics are not updated",
        NodeAction.SHUTDOWN.equals(nodeHeartbeat.getNodeAction()));
    dispatcher.await();

    writeToHostsFile("");
    rm.getNodesListManager().refreshNodes(conf);

    nm3 = rm.registerNode("localhost:4433", 1024);
    dispatcher.await();
    nodeHeartbeat = nm3.nodeHeartbeat(true);
    dispatcher.await();
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));
    // decommissined node is 1 since 1 node is rejoined after updating exclude
    // file
    checkDecommissionedNMCount(rm, metricCount + 1);
  }

  /**
  * Decommissioning using a post-configured include hosts file
  */
  @Test
  public void testAddNewIncludePathToConfiguration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    assert(metrics != null);
    int initialMetricCount = metrics.getNumDecommisionedNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    writeToHostsFile("host1");
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    checkDecommissionedNMCount(rm, ++initialMetricCount);
    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        "Node should not have been decomissioned.",
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertEquals("Node should have been decomissioned but is in state" +
        nodeHeartbeat.getNodeAction(),
        NodeAction.SHUTDOWN, nodeHeartbeat.getNodeAction());
  }
  
  /**
   * Decommissioning using a post-configured exclude hosts file
   */
  @Test
  public void testAddNewExcludePathToConfiguration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 10240);
    ClusterMetrics metrics = ClusterMetrics.getMetrics();
    assert(metrics != null);
    int initialMetricCount = metrics.getNumDecommisionedNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertEquals(
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    writeToHostsFile("host2");
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    checkDecommissionedNMCount(rm, ++initialMetricCount);
    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertEquals(
        "Node should not have been decomissioned.",
        NodeAction.NORMAL,
        nodeHeartbeat.getNodeAction());
    nodeHeartbeat = nm2.nodeHeartbeat(true);
    Assert.assertEquals("Node should have been decomissioned but is in state" +
        nodeHeartbeat.getNodeAction(),
        NodeAction.SHUTDOWN, nodeHeartbeat.getNodeAction());
  }

  @Test
  public void testNodeRegistrationSuccess() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    // trying to register a invalid node.
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.NORMAL, response.getNodeAction());
  }

  @Test
  public void testNodeRegistrationWithLabels() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }

    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest registerReq =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    registerReq.setResource(capability);
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toSet(NodeLabel.newInstance("A")));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(registerReq);

    Assert.assertEquals("Action should be normal on valid Node Labels",
        NodeAction.NORMAL, response.getNodeAction());
    assertCollectionEquals(nodeLabelsMgr.getNodeLabels().get(nodeId),
        NodeLabelsUtils.convertToStringSet(registerReq.getNodeLabels()));
    Assert.assertTrue("Valid Node Labels were not accepted by RM",
        response.getAreNodeLabelsAcceptedByRM());
    rm.stop();
  }

  @Test
  public void testNodeRegistrationWithInvalidLabels() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("X", "Y", "Z"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }

    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest registerReq =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    registerReq.setResource(capability);
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toNodeLabelSet("A", "B", "C"));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(registerReq);

    Assert.assertEquals(
        "On Invalid Node Labels action is expected to be normal",
        NodeAction.NORMAL, response.getNodeAction());
    Assert.assertNull(nodeLabelsMgr.getNodeLabels().get(nodeId));
    Assert.assertNotNull(response.getDiagnosticsMessage());
    Assert.assertFalse("Node Labels should not accepted by RM If Invalid",
        response.getAreNodeLabelsAcceptedByRM());

    if (rm != null) {
      rm.stop();
    }
  }

  @Test
  public void testNodeRegistrationWithInvalidLabelsSyntax() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("X", "Y", "Z"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }

    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest req =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    req.setNodeLabels(toNodeLabelSet("#Y"));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(req);

    Assert.assertEquals(
        "On Invalid Node Labels action is expected to be normal",
        NodeAction.NORMAL, response.getNodeAction());
    Assert.assertNull(nodeLabelsMgr.getNodeLabels().get(nodeId));
    Assert.assertNotNull(response.getDiagnosticsMessage());
    Assert.assertFalse("Node Labels should not accepted by RM If Invalid",
        response.getAreNodeLabelsAcceptedByRM());

    if (rm != null) {
      rm.stop();
    }
  }

  @Test
  public void testNodeRegistrationWithCentralLabelConfig() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DEFAULT_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();
    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }
    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest req =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    req.setNodeLabels(toNodeLabelSet("A"));
    RegisterNodeManagerResponse response =
        resourceTrackerService.registerNodeManager(req);
    // registered to RM with central label config
    Assert.assertEquals(NodeAction.NORMAL, response.getNodeAction());
    Assert.assertNull(nodeLabelsMgr.getNodeLabels().get(nodeId));
    Assert
        .assertFalse(
            "Node Labels should not accepted by RM If its configured with Central configuration",
            response.getAreNodeLabelsAcceptedByRM());
    if (rm != null) {
      rm.stop();
    }
  }

  @SuppressWarnings("unchecked")
  private NodeStatus getNodeStatusObject(NodeId nodeId) {
    NodeStatus status = Records.newRecord(NodeStatus.class);
    status.setNodeId(nodeId);
    status.setResponseId(0);
    status.setContainersStatuses(Collections.EMPTY_LIST);
    status.setKeepAliveApplications(Collections.EMPTY_LIST);
    return status;
  }

  @Test
  public void testNodeHeartBeatWithLabels() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();
    // adding valid labels
    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }

    // Registering of labels and other required info to RM
    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest registerReq =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    registerReq.setResource(capability);
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toNodeLabelSet("A")); // Node register label
    RegisterNodeManagerResponse registerResponse =
        resourceTrackerService.registerNodeManager(registerReq);

    // modification of labels during heartbeat
    NodeHeartbeatRequest heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    heartbeatReq.setNodeLabels(toNodeLabelSet("B")); // Node heartbeat label update
    NodeStatus nodeStatusObject = getNodeStatusObject(nodeId);
    heartbeatReq.setNodeStatus(nodeStatusObject);
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
        .getNMTokenMasterKey());
    heartbeatReq.setLastKnownContainerTokenMasterKey(registerResponse
        .getContainerTokenMasterKey());
    NodeHeartbeatResponse nodeHeartbeatResponse =
        resourceTrackerService.nodeHeartbeat(heartbeatReq);

    Assert.assertEquals("InValid Node Labels were not accepted by RM",
        NodeAction.NORMAL, nodeHeartbeatResponse.getNodeAction());
    assertCollectionEquals(nodeLabelsMgr.getNodeLabels().get(nodeId),
        NodeLabelsUtils.convertToStringSet(heartbeatReq.getNodeLabels()));
    Assert.assertTrue("Valid Node Labels were not accepted by RM",
        nodeHeartbeatResponse.getAreNodeLabelsAcceptedByRM());
    
    // After modification of labels next heartbeat sends null informing no update
    Set<String> oldLabels = nodeLabelsMgr.getNodeLabels().get(nodeId);
    int responseId = nodeStatusObject.getResponseId();
    heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    heartbeatReq.setNodeLabels(null); // Node heartbeat label update
    nodeStatusObject = getNodeStatusObject(nodeId);
    nodeStatusObject.setResponseId(responseId+2);
    heartbeatReq.setNodeStatus(nodeStatusObject);
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
        .getNMTokenMasterKey());
    heartbeatReq.setLastKnownContainerTokenMasterKey(registerResponse
        .getContainerTokenMasterKey());
    nodeHeartbeatResponse = resourceTrackerService.nodeHeartbeat(heartbeatReq);

    Assert.assertEquals("InValid Node Labels were not accepted by RM",
        NodeAction.NORMAL, nodeHeartbeatResponse.getNodeAction());
    assertCollectionEquals(nodeLabelsMgr.getNodeLabels().get(nodeId),
        oldLabels);
    Assert.assertFalse("Node Labels should not accepted by RM",
        nodeHeartbeatResponse.getAreNodeLabelsAcceptedByRM());
    rm.stop();
  }

  @Test
  public void testNodeHeartBeatWithInvalidLabels() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    }

    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest registerReq =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    registerReq.setResource(capability);
    registerReq.setNodeId(nodeId);
    registerReq.setHttpPort(1234);
    registerReq.setNMVersion(YarnVersionInfo.getVersion());
    registerReq.setNodeLabels(toNodeLabelSet("A"));
    RegisterNodeManagerResponse registerResponse =
        resourceTrackerService.registerNodeManager(registerReq);

    NodeHeartbeatRequest heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    heartbeatReq.setNodeLabels(toNodeLabelSet("B", "#C")); // Invalid heart beat labels
    heartbeatReq.setNodeStatus(getNodeStatusObject(nodeId));
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
        .getNMTokenMasterKey());
    heartbeatReq.setLastKnownContainerTokenMasterKey(registerResponse
        .getContainerTokenMasterKey());
    NodeHeartbeatResponse nodeHeartbeatResponse =
        resourceTrackerService.nodeHeartbeat(heartbeatReq);

    // response should be NORMAL when RM heartbeat labels are rejected
    Assert.assertEquals("Response should be NORMAL when RM heartbeat labels"
        + " are rejected", NodeAction.NORMAL,
        nodeHeartbeatResponse.getNodeAction());
    Assert.assertFalse(nodeHeartbeatResponse.getAreNodeLabelsAcceptedByRM());
    Assert.assertNotNull(nodeHeartbeatResponse.getDiagnosticsMessage());
    rm.stop();
  }

  @Test
  public void testNodeHeartbeatWithCentralLabelConfig() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DEFAULT_NODELABEL_CONFIGURATION_TYPE);

    final RMNodeLabelsManager nodeLabelsMgr = new NullRMNodeLabelsManager();

    rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return nodeLabelsMgr;
      }
    };
    rm.start();

    ResourceTrackerService resourceTrackerService =
        rm.getResourceTrackerService();
    RegisterNodeManagerRequest req =
        Records.newRecord(RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(YarnVersionInfo.getVersion());
    req.setNodeLabels(toNodeLabelSet("A", "B", "C"));
    RegisterNodeManagerResponse registerResponse =
        resourceTrackerService.registerNodeManager(req);

    NodeHeartbeatRequest heartbeatReq =
        Records.newRecord(NodeHeartbeatRequest.class);
    heartbeatReq.setNodeLabels(toNodeLabelSet("B")); // Valid heart beat labels
    heartbeatReq.setNodeStatus(getNodeStatusObject(nodeId));
    heartbeatReq.setLastKnownNMTokenMasterKey(registerResponse
        .getNMTokenMasterKey());
    heartbeatReq.setLastKnownContainerTokenMasterKey(registerResponse
        .getContainerTokenMasterKey());
    NodeHeartbeatResponse nodeHeartbeatResponse =
        resourceTrackerService.nodeHeartbeat(heartbeatReq);

    // response should be ok but the RMacceptNodeLabelsUpdate should be false
    Assert.assertEquals(NodeAction.NORMAL,
        nodeHeartbeatResponse.getNodeAction());
    // no change in the labels,
    Assert.assertNull(nodeLabelsMgr.getNodeLabels().get(nodeId));
    // heartbeat labels rejected
    Assert.assertFalse("Invalid Node Labels should not accepted by RM",
        nodeHeartbeatResponse.getAreNodeLabelsAcceptedByRM());
    if (rm != null) {
      rm.stop();
    }
  }

  @Test
  public void testNodeRegistrationVersionLessThanRM() throws Exception {
    writeToHostsFile("host2");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    conf.set(YarnConfiguration.RM_NODEMANAGER_MINIMUM_VERSION,"EqualToRM" );
    rm = new MockRM(conf);
    rm.start();
    String nmVersion = "1.9.9";

    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    req.setNMVersion(nmVersion);
    // trying to register a invalid node.
    RegisterNodeManagerResponse response = resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response.getNodeAction());
    Assert.assertTrue("Diagnostic message did not contain: 'Disallowed NodeManager " +
        "Version "+ nmVersion + ", is less than the minimum version'",
        response.getDiagnosticsMessage().contains("Disallowed NodeManager Version " +
            nmVersion + ", is less than the minimum version "));

  }

  @Test
  public void testNodeRegistrationFailure() throws Exception {
    writeToHostsFile("host1");
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());
    rm = new MockRM(conf);
    rm.start();
    
    ResourceTrackerService resourceTrackerService = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = NodeId.newInstance("host2", 1234);
    req.setNodeId(nodeId);
    req.setHttpPort(1234);
    // trying to register a invalid node.
    RegisterNodeManagerResponse response = resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response.getNodeAction());
    Assert
      .assertEquals(
        "Disallowed NodeManager from  host2, Sending SHUTDOWN signal to the NodeManager.",
        response.getDiagnosticsMessage());
  }

  @Test
  public void testSetRMIdentifierInRegistration() throws Exception {

    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    MockNM nm = new MockNM("host1:1234", 5120, rm.getResourceTrackerService());
    RegisterNodeManagerResponse response = nm.registerNode();

    // Verify the RMIdentifier is correctly set in RegisterNodeManagerResponse
    Assert.assertEquals(ResourceManager.getClusterTimeStamp(),
      response.getRMIdentifier());
  }

  @Test
  public void testNodeRegistrationWithMinimumAllocations() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "2048");
    conf.set(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, "4");
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService
      = rm.getResourceTrackerService();
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    NodeId nodeId = BuilderUtils.newNodeId("host", 1234);
    req.setNodeId(nodeId);

    Resource capability = BuilderUtils.newResource(1024, 1);
    req.setResource(capability);
    RegisterNodeManagerResponse response1 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response1.getNodeAction());
    
    capability.setMemory(2048);
    capability.setVirtualCores(1);
    req.setResource(capability);
    RegisterNodeManagerResponse response2 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response2.getNodeAction());
    
    capability.setMemory(1024);
    capability.setVirtualCores(4);
    req.setResource(capability);
    RegisterNodeManagerResponse response3 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.SHUTDOWN,response3.getNodeAction());
    
    capability.setMemory(2048);
    capability.setVirtualCores(4);
    req.setResource(capability);
    RegisterNodeManagerResponse response4 =
        resourceTrackerService.registerNodeManager(req);
    Assert.assertEquals(NodeAction.NORMAL,response4.getNodeAction());
  }

  @Test
  public void testReboot() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:1234", 2048);

    int initialMetricCount = ClusterMetrics.getMetrics().getNumRebootedNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    nodeHeartbeat = nm2.nodeHeartbeat(
      new HashMap<ApplicationId, List<ContainerStatus>>(), true, -100);
    Assert.assertTrue(NodeAction.RESYNC.equals(nodeHeartbeat.getNodeAction()));
    Assert.assertEquals("Too far behind rm response id:0 nm response id:-100",
      nodeHeartbeat.getDiagnosticsMessage());
    checkRebootedNMCount(rm, ++initialMetricCount);
  }

  private void checkRebootedNMCount(MockRM rm2, int count)
      throws InterruptedException {
    
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumRebootedNMs() != count
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals("The rebooted metrics are not updated", count,
        ClusterMetrics.getMetrics().getNumRebootedNMs());
  }

  @Test
  public void testUnhealthyNodeStatus() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, hostFile
        .getAbsolutePath());

    rm = new MockRM(conf);
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    Assert.assertEquals(0, ClusterMetrics.getMetrics().getUnhealthyNMs());
    // node healthy
    nm1.nodeHeartbeat(true);

    // node unhealthy
    nm1.nodeHeartbeat(false);
    checkUnealthyNMCount(rm, nm1, true, 1);

    // node healthy again
    nm1.nodeHeartbeat(true);
    checkUnealthyNMCount(rm, nm1, false, 0);
  }
  
  private void checkUnealthyNMCount(MockRM rm, MockNM nm1, boolean health,
      int count) throws Exception {
    
    int waitCount = 0;
    while((rm.getRMContext().getRMNodes().get(nm1.getNodeId())
        .getState() != NodeState.UNHEALTHY) == health
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertFalse((rm.getRMContext().getRMNodes().get(nm1.getNodeId())
        .getState() != NodeState.UNHEALTHY) == health);
    Assert.assertEquals("Unhealthy metrics not incremented", count,
        ClusterMetrics.getMetrics().getUnhealthyNMs());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testHandleContainerStatusInvalidCompletions() throws Exception {
    rm = new MockRM(new YarnConfiguration());
    rm.start();

    EventHandler handler =
        spy(rm.getRMContext().getDispatcher().getEventHandler());

    // Case 1: Unmanaged AM
    RMApp app = rm.submitApp(1024, true);

    // Case 1.1: AppAttemptId is null
    NMContainerStatus report =
        NMContainerStatus.newInstance(
          ContainerId.newContainerId(
            ApplicationAttemptId.newInstance(app.getApplicationId(), 2), 1),
          ContainerState.COMPLETE, Resource.newInstance(1024, 1),
          "Dummy Completed", 0, Priority.newInstance(10), 1234);
    rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    verify(handler, never()).handle((Event) any());

    // Case 1.2: Master container is null
    RMAppAttemptImpl currentAttempt =
        (RMAppAttemptImpl) app.getCurrentAppAttempt();
    currentAttempt.setMasterContainer(null);
    report = NMContainerStatus.newInstance(
          ContainerId.newContainerId(currentAttempt.getAppAttemptId(), 0),
          ContainerState.COMPLETE, Resource.newInstance(1024, 1),
          "Dummy Completed", 0, Priority.newInstance(10), 1234);
    rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    verify(handler, never()).handle((Event)any());

    // Case 2: Managed AM
    app = rm.submitApp(1024);

    // Case 2.1: AppAttemptId is null
    report = NMContainerStatus.newInstance(
          ContainerId.newContainerId(
            ApplicationAttemptId.newInstance(app.getApplicationId(), 2), 1),
          ContainerState.COMPLETE, Resource.newInstance(1024, 1),
          "Dummy Completed", 0, Priority.newInstance(10), 1234);
    try {
      rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    } catch (Exception e) {
      // expected - ignore
    }
    verify(handler, never()).handle((Event)any());

    // Case 2.2: Master container is null
    currentAttempt =
        (RMAppAttemptImpl) app.getCurrentAppAttempt();
    currentAttempt.setMasterContainer(null);
    report = NMContainerStatus.newInstance(
      ContainerId.newContainerId(currentAttempt.getAppAttemptId(), 0),
      ContainerState.COMPLETE, Resource.newInstance(1024, 1),
      "Dummy Completed", 0, Priority.newInstance(10), 1234);
    try {
      rm.getResourceTrackerService().handleNMContainerStatus(report, null);
    } catch (Exception e) {
      // expected - ignore
    }
    verify(handler, never()).handle((Event)any());
  }

  @Test
  public void testReconnectNode() throws Exception {
    final DrainDispatcher dispatcher = new DrainDispatcher();
    rm = new MockRM() {
      @Override
      protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
        return new SchedulerEventDispatcher(this.scheduler) {
          @Override
          public void handle(SchedulerEvent event) {
            scheduler.handle(event);
          }
        };
      }

      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.start();

    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    MockNM nm2 = rm.registerNode("host2:5678", 5120);
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(false);
    dispatcher.await();
    checkUnealthyNMCount(rm, nm2, true, 1);
    final int expectedNMs = ClusterMetrics.getMetrics().getNumActiveNMs();
    QueueMetrics metrics = rm.getResourceScheduler().getRootQueueMetrics();
    // TODO Metrics incorrect in case of the FifoScheduler
    Assert.assertEquals(5120, metrics.getAvailableMB());

    // reconnect of healthy node
    nm1 = rm.registerNode("host1:1234", 5120);
    NodeHeartbeatResponse response = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    dispatcher.await();
    Assert.assertEquals(expectedNMs, ClusterMetrics.getMetrics().getNumActiveNMs());
    checkUnealthyNMCount(rm, nm2, true, 1);

    // reconnect of unhealthy node
    nm2 = rm.registerNode("host2:5678", 5120);
    response = nm2.nodeHeartbeat(false);
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    dispatcher.await();
    Assert.assertEquals(expectedNMs, ClusterMetrics.getMetrics().getNumActiveNMs());
    checkUnealthyNMCount(rm, nm2, true, 1);
    
    // unhealthy node changed back to healthy
    nm2 = rm.registerNode("host2:5678", 5120);
    dispatcher.await();
    response = nm2.nodeHeartbeat(true);
    response = nm2.nodeHeartbeat(true);
    dispatcher.await();
    Assert.assertEquals(5120 + 5120, metrics.getAvailableMB());

    // reconnect of node with changed capability
    nm1 = rm.registerNode("host2:5678", 10240);
    dispatcher.await();
    response = nm1.nodeHeartbeat(true);
    dispatcher.await();
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    Assert.assertEquals(5120 + 10240, metrics.getAvailableMB());

    // reconnect of node with changed capability and running applications
    List<ApplicationId> runningApps = new ArrayList<ApplicationId>();
    runningApps.add(ApplicationId.newInstance(1, 0));
    nm1 = rm.registerNode("host2:5678", 15360, 2, runningApps);
    dispatcher.await();
    response = nm1.nodeHeartbeat(true);
    dispatcher.await();
    Assert.assertTrue(NodeAction.NORMAL.equals(response.getNodeAction()));
    Assert.assertEquals(5120 + 15360, metrics.getAvailableMB());
    
    // reconnect healthy node changing http port
    nm1 = new MockNM("host1:1234", 5120, rm.getResourceTrackerService());
    nm1.setHttpPort(3);
    nm1.registerNode();
    dispatcher.await();
    response = nm1.nodeHeartbeat(true);
    response = nm1.nodeHeartbeat(true);
    dispatcher.await();
    RMNode rmNode = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    Assert.assertEquals(3, rmNode.getHttpPort());
    Assert.assertEquals(5120, rmNode.getTotalCapability().getMemory());
    Assert.assertEquals(5120 + 15360, metrics.getAvailableMB());

  }

  @Test
  public void testNMUnregistration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService = rm
        .getResourceTrackerService();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);

    int shutdownNMsCount = ClusterMetrics.getMetrics()
        .getNumShutdownNMs();
    NodeHeartbeatResponse nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.NORMAL.equals(nodeHeartbeat.getNodeAction()));

    UnRegisterNodeManagerRequest request = Records
        .newRecord(UnRegisterNodeManagerRequest.class);
    request.setNodeId(nm1.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, ++shutdownNMsCount);

    // The RM should remove the node after unregistration, hence send a reboot
    // command.
    nodeHeartbeat = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.RESYNC.equals(nodeHeartbeat.getNodeAction()));
  }

  @Test
  public void testUnhealthyNMUnregistration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();

    ResourceTrackerService resourceTrackerService = rm
        .getResourceTrackerService();
    MockNM nm1 = rm.registerNode("host1:1234", 5120);
    Assert.assertEquals(0, ClusterMetrics.getMetrics().getUnhealthyNMs());
    // node healthy
    nm1.nodeHeartbeat(true);
    int shutdownNMsCount = ClusterMetrics.getMetrics().getNumShutdownNMs();

    // node unhealthy
    nm1.nodeHeartbeat(false);
    checkUnealthyNMCount(rm, nm1, true, 1);
    UnRegisterNodeManagerRequest request = Records
        .newRecord(UnRegisterNodeManagerRequest.class);
    request.setNodeId(nm1.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, ++shutdownNMsCount);
  }

  @Test
  public void testInvalidNMUnregistration() throws Exception {
    Configuration conf = new Configuration();
    rm = new MockRM(conf);
    rm.start();
    ResourceTrackerService resourceTrackerService = rm
        .getResourceTrackerService();
    int shutdownNMsCount = ClusterMetrics.getMetrics()
        .getNumShutdownNMs();
    int decommisionedNMsCount = ClusterMetrics.getMetrics()
        .getNumDecommisionedNMs();

    // Node not found for unregister
    UnRegisterNodeManagerRequest request = Records
        .newRecord(UnRegisterNodeManagerRequest.class);
    request.setNodeId(BuilderUtils.newNodeId("host", 1234));
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, 0);
    checkDecommissionedNMCount(rm, 0);

    // 1. Register the Node Manager
    // 2. Exclude the same Node Manager host
    // 3. Give NM heartbeat to RM
    // 4. Unregister the Node Manager
    MockNM nm1 = new MockNM("host1:1234", 5120, resourceTrackerService);
    RegisterNodeManagerResponse response = nm1.registerNode();
    Assert.assertEquals(NodeAction.NORMAL, response.getNodeAction());
    writeToHostsFile("host2");
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    NodeHeartbeatResponse heartbeatResponse = nm1.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.SHUTDOWN, heartbeatResponse.getNodeAction());
    checkShutdownNMCount(rm, shutdownNMsCount);
    checkDecommissionedNMCount(rm, ++decommisionedNMsCount);
    request.setNodeId(nm1.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, shutdownNMsCount);
    checkDecommissionedNMCount(rm, decommisionedNMsCount);

    // 1. Register the Node Manager
    // 2. Exclude the same Node Manager host
    // 3. Unregister the Node Manager
    MockNM nm2 = new MockNM("host2:1234", 5120, resourceTrackerService);
    RegisterNodeManagerResponse response2 = nm2.registerNode();
    Assert.assertEquals(NodeAction.NORMAL, response2.getNodeAction());
    writeToHostsFile("host1");
    conf.set(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        hostFile.getAbsolutePath());
    rm.getNodesListManager().refreshNodes(conf);
    request.setNodeId(nm2.getNodeId());
    resourceTrackerService.unRegisterNodeManager(request);
    checkShutdownNMCount(rm, shutdownNMsCount);
    checkDecommissionedNMCount(rm, ++decommisionedNMsCount);
  }

  private void writeToHostsFile(String... hosts) throws IOException {
    if (!hostFile.exists()) {
      TEMP_DIR.mkdirs();
      hostFile.createNewFile();
    }
    FileOutputStream fStream = null;
    try {
      fStream = new FileOutputStream(hostFile);
      for (int i = 0; i < hosts.length; i++) {
        fStream.write(hosts[i].getBytes());
        fStream.write("\n".getBytes());
      }
    } finally {
      if (fStream != null) {
        IOUtils.closeStream(fStream);
        fStream = null;
      }
    }
  }

  private void checkDecommissionedNMCount(MockRM rm, int count)
      throws InterruptedException {
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumDecommisionedNMs() != count
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals(count, ClusterMetrics.getMetrics()
        .getNumDecommisionedNMs());
    Assert.assertEquals("The decommisioned metrics are not updated", count,
        ClusterMetrics.getMetrics().getNumDecommisionedNMs());
  }

  private void checkShutdownNMCount(MockRM rm, int count)
      throws InterruptedException {
    int waitCount = 0;
    while (ClusterMetrics.getMetrics().getNumShutdownNMs() != count
        && waitCount++ < 20) {
      synchronized (this) {
        wait(100);
      }
    }
    Assert.assertEquals("The shutdown metrics are not updated", count,
        ClusterMetrics.getMetrics().getNumShutdownNMs());
  }

  @After
  public void tearDown() {
    if (hostFile != null && hostFile.exists()) {
      hostFile.delete();
    }

    ClusterMetrics.destroy();
    if (rm != null) {
      rm.stop();
    }

    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms.getSource("ClusterMetrics") != null) {
      DefaultMetricsSystem.shutdown();
    }
  }
}
