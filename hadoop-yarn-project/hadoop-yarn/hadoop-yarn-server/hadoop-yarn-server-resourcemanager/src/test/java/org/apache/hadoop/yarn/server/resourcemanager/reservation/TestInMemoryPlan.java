/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *  
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.Planner;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestInMemoryPlan {

  private String user = "yarn";
  private String planName = "test-reservation";
  private ResourceCalculator resCalc;
  private Resource minAlloc;
  private Resource maxAlloc;
  private Resource totalCapacity;

  private Clock clock;
  private QueueMetrics queueMetrics;
  private SharingPolicy policy;
  private ReservationAgent agent;
  private Planner replanner;
  private RMContext context;

  @Before
  public void setUp() throws PlanningException {
    resCalc = new DefaultResourceCalculator();
    minAlloc = Resource.newInstance(1024, 1);
    maxAlloc = Resource.newInstance(64 * 1024, 20);
    totalCapacity = Resource.newInstance(100 * 1024, 100);

    clock = mock(Clock.class);
    queueMetrics = mock(QueueMetrics.class);
    policy = mock(SharingPolicy.class);
    replanner = mock(Planner.class);

    when(clock.getTime()).thenReturn(1L);

    context = ReservationSystemTestUtil.createMockRMContext();
  }

  @After
  public void tearDown() {
    resCalc = null;
    minAlloc = null;
    maxAlloc = null;
    totalCapacity = null;

    clock = null;
    queueMetrics = null;
    policy = null;
    replanner = null;
  }

  @Test
  public void testAddReservation() {
    Plan plan =
        new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L,
            resCalc, minAlloc, maxAlloc, planName, replanner, true, context);
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Map<ReservationInterval, ReservationRequest> allocations =
        generateAllocation(start, alloc, false);
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs =
        ReservationSystemUtil.toResources(allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.addReservation(rAllocation, false);
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    doAssertions(plan, rAllocation);
    checkAllocation(plan, alloc, start);
  }

  private void checkAllocation(Plan plan, int[] alloc, int start) {
    RLESparseResourceAllocation userCons =
        plan.getConsumptionForUserOverTime(user, start, start + alloc.length);

    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(Resource.newInstance(1024 * (alloc[i]), (alloc[i])),
          plan.getTotalCommittedResources(start + i));
      Assert.assertEquals(Resource.newInstance(1024 * (alloc[i]), (alloc[i])),
          userCons.getCapacityAtTime(start + i));
    }
  }

  @Test
  public void testAddEmptyReservation() {
    Plan plan =
        new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L,
            resCalc, minAlloc, maxAlloc, planName, replanner, true, context);
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    int[] alloc = {};
    int start = 100;
    Map<ReservationInterval, ReservationRequest> allocations =
        new HashMap<ReservationInterval, ReservationRequest>();
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs = ReservationSystemUtil.toResources
        (allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.addReservation(rAllocation, false);
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testAddReservationAlreadyExists() {
    // First add a reservation
    Plan plan =
        new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L,
            resCalc, minAlloc, maxAlloc, planName, replanner, true, context);
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Map<ReservationInterval, ReservationRequest> allocations =
        generateAllocation(start, alloc, false);
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs = ReservationSystemUtil.toResources
        (allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.addReservation(rAllocation, false);
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    doAssertions(plan, rAllocation);
    checkAllocation(plan, alloc, start);

    // Try to add it again
    try {
      plan.addReservation(rAllocation, false);
      Assert.fail("Add should fail as it already exists");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().endsWith("already exists"));
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    doAssertions(plan, rAllocation);
  }

  @Test
  public void testUpdateReservation() {
    Plan plan =
        new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L,
            resCalc, minAlloc, maxAlloc, planName, replanner, true, context);
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    // First add a reservation
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Map<ReservationInterval, ReservationRequest> allocations =
        generateAllocation(start, alloc, false);
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs = ReservationSystemUtil.toResources
        (allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.addReservation(rAllocation, false);
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    doAssertions(plan, rAllocation);

    RLESparseResourceAllocation userCons =
        plan.getConsumptionForUserOverTime(user, start, start + alloc.length);
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(Resource.newInstance(1024 * (alloc[i]), (alloc[i])),
          plan.getTotalCommittedResources(start + i));
      Assert.assertEquals(Resource.newInstance(1024 * (alloc[i]), (alloc[i])),
          userCons.getCapacityAtTime(start + i));
    }

    // Now update it
    start = 110;
    int[] updatedAlloc = { 0, 5, 10, 10, 5, 0 };
    allocations = generateAllocation(start, updatedAlloc, true);
    rDef =
        createSimpleReservationDefinition(start, start + updatedAlloc.length,
            updatedAlloc.length, allocations.values());
    Map<ReservationInterval, Resource> updatedAllocs =
        ReservationSystemUtil.toResources(allocations);
    rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + updatedAlloc.length, updatedAllocs, resCalc,
            minAlloc);
    try {
      plan.updateReservation(rAllocation);
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    doAssertions(plan, rAllocation);

    userCons =
        plan.getConsumptionForUserOverTime(user, start, start
            + updatedAlloc.length);

    for (int i = 0; i < updatedAlloc.length; i++) {
      Assert.assertEquals(
     Resource.newInstance(1024 * (updatedAlloc[i] + i), updatedAlloc[i]
              + i), plan.getTotalCommittedResources(start + i));
      Assert.assertEquals(
          Resource.newInstance(1024 * (updatedAlloc[i] + i), updatedAlloc[i]
              + i), userCons.getCapacityAtTime(start + i));
    }
  }

  @Test
  public void testUpdateNonExistingReservation() {
    Plan plan =
        new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L,
            resCalc, minAlloc, maxAlloc, planName, replanner, true, context);
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    // Try to update a reservation without adding
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Map<ReservationInterval, ReservationRequest> allocations =
        generateAllocation(start, alloc, false);
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs =
        ReservationSystemUtil.toResources(allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.updateReservation(rAllocation);
      Assert.fail("Update should fail as it does not exist in the plan");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().endsWith("does not exist in the plan"));
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNull(plan.getReservationById(reservationID));
  }

  @Test
  public void testDeleteReservation() {
    // First add a reservation
    Plan plan =
        new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L,
            resCalc, minAlloc, maxAlloc, planName, replanner, true, context);
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Map<ReservationInterval, ReservationRequest> allocations =
        generateAllocation(start, alloc, true);
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs =
        ReservationSystemUtil.toResources(allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.addReservation(rAllocation, false);
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    doAssertions(plan, rAllocation);

    RLESparseResourceAllocation userCons =
        plan.getConsumptionForUserOverTime(user, start, start + alloc.length);

    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(
          Resource.newInstance(1024 * (alloc[i] + i), (alloc[i] + i)),
          plan.getTotalCommittedResources(start + i));
      Assert.assertEquals(
          Resource.newInstance(1024 * (alloc[i] + i), (alloc[i] + i)),
          userCons.getCapacityAtTime(start + i));
    }

    // Now delete it
    try {
      plan.deleteReservation(reservationID);
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNull(plan.getReservationById(reservationID));
    userCons =
        plan.getConsumptionForUserOverTime(user, start, start + alloc.length);
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(Resource.newInstance(0, 0),
          plan.getTotalCommittedResources(start + i));
      Assert.assertEquals(Resource.newInstance(0, 0),
          userCons.getCapacityAtTime(start + i));
    }
  }

  @Test
  public void testDeleteNonExistingReservation() {
    Plan plan =
        new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L,
            resCalc, minAlloc, maxAlloc, planName, replanner, true, context);
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    // Try to delete a reservation without adding
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.deleteReservation(reservationID);
      Assert.fail("Delete should fail as it does not exist in the plan");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().endsWith("does not exist in the plan"));
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNull(plan.getReservationById(reservationID));
  }

  @Test
  public void testArchiveCompletedReservations() {
    Plan plan =
        new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L,
            resCalc, minAlloc, maxAlloc, planName, replanner, true, context);
    ReservationId reservationID1 =
        ReservationSystemTestUtil.getNewReservationId();
    // First add a reservation
    int[] alloc1 = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Map<ReservationInterval, ReservationRequest> allocations1 =
        generateAllocation(start, alloc1, false);
    ReservationDefinition rDef1 =
        createSimpleReservationDefinition(start, start + alloc1.length,
            alloc1.length, allocations1.values());
    Map<ReservationInterval, Resource> allocs1 =
        ReservationSystemUtil.toResources(allocations1);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID1, rDef1, user,
            planName, start, start + alloc1.length, allocs1, resCalc,
            minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID1));
    try {
      plan.addReservation(rAllocation, false);
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    doAssertions(plan, rAllocation);
    checkAllocation(plan, alloc1, start);


    // Now add another one
    ReservationId reservationID2 =
        ReservationSystemTestUtil.getNewReservationId();
    int[] alloc2 = { 0, 5, 10, 5, 0 };
    Map<ReservationInterval, ReservationRequest> allocations2 =
        generateAllocation(start, alloc2, true);
    ReservationDefinition rDef2 =
        createSimpleReservationDefinition(start, start + alloc2.length,
            alloc2.length, allocations2.values());
    Map<ReservationInterval, Resource> allocs2 =
        ReservationSystemUtil.toResources(allocations2);
    rAllocation =
        new InMemoryReservationAllocation(reservationID2, rDef2, user,
            planName, start, start + alloc2.length, allocs2, resCalc,
            minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID2));
    try {
      plan.addReservation(rAllocation, false);
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(plan.getReservationById(reservationID2));

    RLESparseResourceAllocation userCons =
        plan.getConsumptionForUserOverTime(user, start, start + alloc2.length);

    for (int i = 0; i < alloc2.length; i++) {
      Assert.assertEquals(
          Resource.newInstance(1024 * (alloc1[i] + alloc2[i] + i), alloc1[i]
              + alloc2[i] + i), plan.getTotalCommittedResources(start + i));
      Assert.assertEquals(
          Resource.newInstance(1024 * (alloc1[i] + alloc2[i] + i), alloc1[i]
              + alloc2[i] + i), userCons.getCapacityAtTime(start + i));
    }

    // Now archive completed reservations
    when(clock.getTime()).thenReturn(106L);
    when(policy.getValidWindow()).thenReturn(1L);
    try {
      // will only remove 2nd reservation as only that has fallen out of the
      // archival window
      plan.archiveCompletedReservations(clock.getTime());
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(plan.getReservationById(reservationID1));
    Assert.assertNull(plan.getReservationById(reservationID2));
    checkAllocation(plan, alloc1, start);

    when(clock.getTime()).thenReturn(107L);
    try {
      // will remove 1st reservation also as it has fallen out of the archival
      // window
      plan.archiveCompletedReservations(clock.getTime());
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }

    userCons =
        plan.getConsumptionForUserOverTime(user, start, start + alloc1.length);

    Assert.assertNull(plan.getReservationById(reservationID1));
    for (int i = 0; i < alloc1.length; i++) {
      Assert.assertEquals(Resource.newInstance(0, 0),
          plan.getTotalCommittedResources(start + i));
      Assert.assertEquals(Resource.newInstance(0, 0),
          userCons.getCapacityAtTime(start + i));
    }
  }

  private void doAssertions(Plan plan, ReservationAllocation rAllocation) {
    ReservationId reservationID = rAllocation.getReservationId();
    Assert.assertNotNull(plan.getReservationById(reservationID));
    Assert.assertEquals(rAllocation, plan.getReservationById(reservationID));
    Assert.assertTrue(((InMemoryPlan) plan).getAllReservations().size() == 1);
    Assert.assertEquals(rAllocation.getEndTime(), plan.getLastEndTime());
    Assert.assertEquals(totalCapacity, plan.getTotalCapacity());
    Assert.assertEquals(minAlloc, plan.getMinimumAllocation());
    Assert.assertEquals(maxAlloc, plan.getMaximumAllocation());
    Assert.assertEquals(resCalc, plan.getResourceCalculator());
    Assert.assertEquals(planName, plan.getQueueName());
    Assert.assertTrue(plan.getMoveOnExpiry());
  }

  private ReservationDefinition createSimpleReservationDefinition(long arrival,
      long deadline, long duration, Collection<ReservationRequest> resources) {
    // create a request with a single atomic ask
    ReservationDefinition rDef = new ReservationDefinitionPBImpl();
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(new ArrayList<ReservationRequest>(resources));
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    rDef.setReservationRequests(reqs);
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    return rDef;
  }

  private Map<ReservationInterval, ReservationRequest> generateAllocation(
      int startTime, int[] alloc, boolean isStep) {
    Map<ReservationInterval, ReservationRequest> req =
        new HashMap<ReservationInterval, ReservationRequest>();
    int numContainers = 0;
    for (int i = 0; i < alloc.length; i++) {
      if (isStep) {
        numContainers = alloc[i] + i;
      } else {
        numContainers = alloc[i];
      }
      ReservationRequest rr =
          ReservationRequest.newInstance(Resource.newInstance(1024, 1),
              (numContainers));
      req.put(new ReservationInterval(startTime + i, startTime + i + 1), rr);
    }
    return req;
  }

}
