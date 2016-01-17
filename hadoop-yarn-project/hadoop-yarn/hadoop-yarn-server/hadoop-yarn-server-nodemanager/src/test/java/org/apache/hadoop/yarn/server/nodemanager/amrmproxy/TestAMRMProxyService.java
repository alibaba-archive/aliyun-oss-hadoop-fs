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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;

public class TestAMRMProxyService extends BaseAMRMProxyTest {

  private static final Log LOG = LogFactory
      .getLog(TestAMRMProxyService.class);

  /**
   * Test if the pipeline is created properly.
   */
  @Test
  public void testRequestInterceptorChainCreation() throws Exception {
    RequestInterceptor root =
        super.getAMRMProxyService().createRequestInterceptorChain();
    int index = 0;
    while (root != null) {
      switch (index) {
      case 0:
      case 1:
      case 2:
        Assert.assertEquals(PassThroughRequestInterceptor.class.getName(),
            root.getClass().getName());
        break;
      case 3:
        Assert.assertEquals(MockRequestInterceptor.class.getName(), root
            .getClass().getName());
        break;
      }

      root = root.getNextInterceptor();
      index++;
    }

    Assert.assertEquals(
        "The number of interceptors in chain does not match",
        Integer.toString(4), Integer.toString(index));

  }

  /**
   * Tests registration of a single application master.
   * 
   * @throws Exception
   */
  @Test
  public void testRegisterOneApplicationMaster() throws Exception {
    // The testAppId identifier is used as host name and the mock resource
    // manager return it as the queue name. Assert that we received the queue
    // name
    int testAppId = 1;
    RegisterApplicationMasterResponse response1 =
        registerApplicationMaster(testAppId);
    Assert.assertNotNull(response1);
    Assert.assertEquals(Integer.toString(testAppId), response1.getQueue());
  }

  /**
   * Tests the registration of multiple application master serially one at a
   * time.
   * 
   * @throws Exception
   */
  @Test
  public void testRegisterMulitpleApplicationMasters() throws Exception {
    for (int testAppId = 0; testAppId < 3; testAppId++) {
      RegisterApplicationMasterResponse response =
          registerApplicationMaster(testAppId);
      Assert.assertNotNull(response);
      Assert
          .assertEquals(Integer.toString(testAppId), response.getQueue());
    }
  }

  /**
   * Tests the registration of multiple application masters using multiple
   * threads in parallel.
   * 
   * @throws Exception
   */
  @Test
  public void testRegisterMulitpleApplicationMastersInParallel()
      throws Exception {
    int numberOfRequests = 5;
    ArrayList<String> testContexts =
        CreateTestRequestIdentifiers(numberOfRequests);
    super.registerApplicationMastersInParallel(testContexts);
  }

  private ArrayList<String> CreateTestRequestIdentifiers(
      int numberOfRequests) {
    ArrayList<String> testContexts = new ArrayList<String>();
    LOG.info("Creating " + numberOfRequests + " contexts for testing");
    for (int ep = 0; ep < numberOfRequests; ep++) {
      testContexts.add("test-endpoint-" + Integer.toString(ep));
      LOG.info("Created test context: " + testContexts.get(ep));
    }
    return testContexts;
  }

  @Test
  public void testFinishOneApplicationMasterWithSuccess() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    Assert.assertNotNull(registerResponse);
    Assert.assertEquals(Integer.toString(testAppId),
        registerResponse.getQueue());

    FinishApplicationMasterResponse finshResponse =
        finishApplicationMaster(testAppId,
            FinalApplicationStatus.SUCCEEDED);

    Assert.assertNotNull(finshResponse);
    Assert.assertEquals(true, finshResponse.getIsUnregistered());
  }

  @Test
  public void testFinishOneApplicationMasterWithFailure() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    Assert.assertNotNull(registerResponse);
    Assert.assertEquals(Integer.toString(testAppId),
        registerResponse.getQueue());

    FinishApplicationMasterResponse finshResponse =
        finishApplicationMaster(testAppId, FinalApplicationStatus.FAILED);

    Assert.assertNotNull(finshResponse);
    Assert.assertEquals(false, finshResponse.getIsUnregistered());

    try {
      // Try to finish an application master that is already finished.
      finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
      Assert
          .fail("The request to finish application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("Finish registration failed as expected because it was not registered");
    }
  }

  @Test
  public void testFinishInvalidApplicationMaster() throws Exception {
    try {
      // Try to finish an application master that was not registered.
      finishApplicationMaster(4, FinalApplicationStatus.SUCCEEDED);
      Assert
          .fail("The request to finish application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("Finish registration failed as expected because it was not registered");
    }
  }

  @Test
  public void testFinishMulitpleApplicationMasters() throws Exception {
    int numberOfRequests = 3;
    for (int index = 0; index < numberOfRequests; index++) {
      RegisterApplicationMasterResponse registerResponse =
          registerApplicationMaster(index);
      Assert.assertNotNull(registerResponse);
      Assert.assertEquals(Integer.toString(index),
          registerResponse.getQueue());
    }

    // Finish in reverse sequence
    for (int index = numberOfRequests - 1; index >= 0; index--) {
      FinishApplicationMasterResponse finshResponse =
          finishApplicationMaster(index, FinalApplicationStatus.SUCCEEDED);

      Assert.assertNotNull(finshResponse);
      Assert.assertEquals(true, finshResponse.getIsUnregistered());

      // Assert that the application has been removed from the collection
      Assert.assertTrue(this.getAMRMProxyService()
          .getPipelines().size() == index);
    }

    try {
      // Try to finish an application master that is already finished.
      finishApplicationMaster(1, FinalApplicationStatus.SUCCEEDED);
      Assert
          .fail("The request to finish application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("Finish registration failed as expected because it was not registered");
    }

    try {
      // Try to finish an application master that was not registered.
      finishApplicationMaster(4, FinalApplicationStatus.SUCCEEDED);
      Assert
          .fail("The request to finish application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("Finish registration failed as expected because it was not registered");
    }
  }

  @Test
  public void testFinishMulitpleApplicationMastersInParallel()
      throws Exception {
    int numberOfRequests = 5;
    ArrayList<String> testContexts = new ArrayList<String>();
    LOG.info("Creating " + numberOfRequests + " contexts for testing");
    for (int i = 0; i < numberOfRequests; i++) {
      testContexts.add("test-endpoint-" + Integer.toString(i));
      LOG.info("Created test context: " + testContexts.get(i));

      RegisterApplicationMasterResponse registerResponse =
          registerApplicationMaster(i);
      Assert.assertNotNull(registerResponse);
      Assert
          .assertEquals(Integer.toString(i), registerResponse.getQueue());
    }

    finishApplicationMastersInParallel(testContexts);
  }

  @Test
  public void testAllocateRequestWithNullValues() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    Assert.assertNotNull(registerResponse);
    Assert.assertEquals(Integer.toString(testAppId),
        registerResponse.getQueue());

    AllocateResponse allocateResponse = allocate(testAppId);
    Assert.assertNotNull(allocateResponse);

    FinishApplicationMasterResponse finshResponse =
        finishApplicationMaster(testAppId,
            FinalApplicationStatus.SUCCEEDED);

    Assert.assertNotNull(finshResponse);
    Assert.assertEquals(true, finshResponse.getIsUnregistered());
  }

  @Test
  public void testAllocateRequestWithoutRegistering() throws Exception {

    try {
      // Try to allocate an application master without registering.
      allocate(1);
      Assert
          .fail("The request to allocate application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("AllocateRequest failed as expected because AM was not registered");
    }
  }

  @Test
  public void testAllocateWithOneResourceRequest() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    Assert.assertNotNull(registerResponse);
    getContainersAndAssert(testAppId, 1);
    finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
  }

  @Test
  public void testAllocateWithMultipleResourceRequest() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    Assert.assertNotNull(registerResponse);
    getContainersAndAssert(testAppId, 10);
    finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
  }

  @Test
  public void testAllocateAndReleaseContainers() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    Assert.assertNotNull(registerResponse);
    List<Container> containers = getContainersAndAssert(testAppId, 10);
    releaseContainersAndAssert(testAppId, containers);
    finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
  }

  @Test
  public void testAllocateAndReleaseContainersForMultipleAM()
      throws Exception {
    int numberOfApps = 5;
    for (int testAppId = 0; testAppId < numberOfApps; testAppId++) {
      RegisterApplicationMasterResponse registerResponse =
          registerApplicationMaster(testAppId);
      Assert.assertNotNull(registerResponse);
      List<Container> containers = getContainersAndAssert(testAppId, 10);
      releaseContainersAndAssert(testAppId, containers);
    }
    for (int testAppId = 0; testAppId < numberOfApps; testAppId++) {
      finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
    }
  }

  @Test
  public void testAllocateAndReleaseContainersForMultipleAMInParallel()
      throws Exception {
    int numberOfApps = 6;
    ArrayList<Integer> tempAppIds = new ArrayList<Integer>();
    for (int i = 0; i < numberOfApps; i++) {
      tempAppIds.add(new Integer(i));
    }

    final ArrayList<Integer> appIds = tempAppIds;
    List<Integer> responses =
        runInParallel(appIds, new Function<Integer, Integer>() {
          @Override
          public Integer invoke(Integer testAppId) {
            try {
              RegisterApplicationMasterResponse registerResponse =
                  registerApplicationMaster(testAppId);
              Assert.assertNotNull("response is null", registerResponse);
              List<Container> containers =
                  getContainersAndAssert(testAppId, 10);
              releaseContainersAndAssert(testAppId, containers);

              LOG.info("Sucessfully registered application master with appId: "
                  + testAppId);
            } catch (Throwable ex) {
              LOG.error(
                  "Failed to register application master with appId: "
                      + testAppId, ex);
              testAppId = null;
            }

            return testAppId;
          }
        });

    Assert.assertEquals(
        "Number of responses received does not match with request",
        appIds.size(), responses.size());

    for (Integer testAppId : responses) {
      Assert.assertNotNull(testAppId);
      finishApplicationMaster(testAppId.intValue(),
          FinalApplicationStatus.SUCCEEDED);
    }
  }

  private List<Container> getContainersAndAssert(int appId,
      int numberOfResourceRequests) throws Exception {
    AllocateRequest allocateRequest =
        Records.newRecord(AllocateRequest.class);
    allocateRequest.setResponseId(1);

    List<Container> containers =
        new ArrayList<Container>(numberOfResourceRequests);
    List<ResourceRequest> askList =
        new ArrayList<ResourceRequest>(numberOfResourceRequests);
    for (int testAppId = 0; testAppId < numberOfResourceRequests; testAppId++) {
      askList.add(createResourceRequest(
          "test-node-" + Integer.toString(testAppId), 6000, 2,
          testAppId % 5, 1));
    }

    allocateRequest.setAskList(askList);

    AllocateResponse allocateResponse = allocate(appId, allocateRequest);
    Assert.assertNotNull("allocate() returned null response",
        allocateResponse);

    containers.addAll(allocateResponse.getAllocatedContainers());

    // Send max 10 heart beats to receive all the containers. If not, we will
    // fail the test
    int numHeartbeat = 0;
    while (containers.size() < askList.size() && numHeartbeat++ < 10) {
      allocateResponse =
          allocate(appId, Records.newRecord(AllocateRequest.class));
      Assert.assertNotNull("allocate() returned null response",
          allocateResponse);

      containers.addAll(allocateResponse.getAllocatedContainers());

      LOG.info("Number of allocated containers in this request: "
          + Integer.toString(allocateResponse.getAllocatedContainers()
              .size()));
      LOG.info("Total number of allocated containers: "
          + Integer.toString(containers.size()));
      Thread.sleep(10);
    }

    // We broadcast the request, the number of containers we received will be
    // higher than we ask
    Assert.assertTrue("The asklist count is not same as response",
        askList.size() <= containers.size());
    return containers;
  }

  private void releaseContainersAndAssert(int appId,
      List<Container> containers) throws Exception {
    Assert.assertTrue(containers.size() > 0);
    AllocateRequest allocateRequest =
        Records.newRecord(AllocateRequest.class);
    allocateRequest.setResponseId(1);

    List<ContainerId> relList =
        new ArrayList<ContainerId>(containers.size());
    for (Container container : containers) {
      relList.add(container.getId());
    }

    allocateRequest.setReleaseList(relList);

    AllocateResponse allocateResponse = allocate(appId, allocateRequest);
    Assert.assertNotNull(allocateResponse);

    // The way the mock resource manager is setup, it will return the containers
    // that were released in the response. This is done because the UAMs run
    // asynchronously and we need to if all the resource managers received the
    // release it. The containers sent by the mock resource managers will be
    // aggregated and returned back to us and we can assert if all the release
    // lists reached the sub-clusters
    List<Container> containersForReleasedContainerIds =
        new ArrayList<Container>();
    containersForReleasedContainerIds.addAll(allocateResponse
        .getAllocatedContainers());

    // Send max 10 heart beats to receive all the containers. If not, we will
    // fail the test
    int numHeartbeat = 0;
    while (containersForReleasedContainerIds.size() < relList.size()
        && numHeartbeat++ < 10) {
      allocateResponse =
          allocate(appId, Records.newRecord(AllocateRequest.class));
      Assert.assertNotNull(allocateResponse);
      containersForReleasedContainerIds.addAll(allocateResponse
          .getAllocatedContainers());

      LOG.info("Number of containers received in this request: "
          + Integer.toString(allocateResponse.getAllocatedContainers()
              .size()));
      LOG.info("Total number of containers received: "
          + Integer.toString(containersForReleasedContainerIds.size()));
      Thread.sleep(10);
    }

    Assert.assertEquals(relList.size(),
        containersForReleasedContainerIds.size());
  }
}
