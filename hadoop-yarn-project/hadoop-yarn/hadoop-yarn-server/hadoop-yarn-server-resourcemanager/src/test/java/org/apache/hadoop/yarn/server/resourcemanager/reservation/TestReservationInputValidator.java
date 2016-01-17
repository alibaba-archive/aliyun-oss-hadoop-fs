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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationDeleteRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationSubmissionRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationUpdateRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestReservationInputValidator {

  private static final Log LOG = LogFactory
      .getLog(TestReservationInputValidator.class);

  private static final String PLAN_NAME = "test-reservation";

  private Clock clock;
  private Map<String, Plan> plans = new HashMap<String, Plan>(1);
  private ReservationSystem rSystem;
  private Plan plan;

  private ReservationInputValidator rrValidator;

  @Before
  public void setUp() {
    clock = mock(Clock.class);
    plan = mock(Plan.class);
    rSystem = mock(ReservationSystem.class);
    plans.put(PLAN_NAME, plan);
    rrValidator = new ReservationInputValidator(clock);
    when(clock.getTime()).thenReturn(1L);
    ResourceCalculator rCalc = new DefaultResourceCalculator();
    Resource resource = Resource.newInstance(10240, 10);
    when(plan.getResourceCalculator()).thenReturn(rCalc);
    when(plan.getTotalCapacity()).thenReturn(resource);
    when(rSystem.getQueueForReservation(any(ReservationId.class))).thenReturn(
        PLAN_NAME);
    when(rSystem.getPlan(PLAN_NAME)).thenReturn(plan);
  }

  @After
  public void tearDown() {
    rrValidator = null;
    clock = null;
    plan = null;
  }

  @Test
  public void testSubmitReservationNormal() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 5, 3);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(plan);
  }

  @Test
  public void testSubmitReservationDoesnotExist() {
    ReservationSubmissionRequest request =
        new ReservationSubmissionRequestPBImpl();
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message
              .equals("The queue to submit is not specified. Please try again with a valid reservable queue."));
      LOG.info(message);
    }
  }

  @Test
  public void testSubmitReservationInvalidPlan() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 5, 3);
    when(rSystem.getPlan(PLAN_NAME)).thenReturn(null);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message
              .endsWith(" is not managed by reservation system. Please try again with a valid reservable queue."));
      LOG.info(message);
    }
  }

  @Test
  public void testSubmitReservationNoDefinition() {
    ReservationSubmissionRequest request =
        new ReservationSubmissionRequestPBImpl();
    request.setQueue(PLAN_NAME);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message
              .equals("Missing reservation definition. Please try again by specifying a reservation definition."));
      LOG.info(message);
    }
  }

  @Test
  public void testSubmitReservationInvalidDeadline() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 0, 3);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert.assertTrue(message
          .startsWith("The specified deadline: 0 is the past"));
      LOG.info(message);
    }
  }

  @Test
  public void testSubmitReservationInvalidRR() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(0, 0, 1, 5, 3);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert.assertTrue(message
          .startsWith("No resources have been specified to reserve"));
      LOG.info(message);
    }
  }

  @Test
  public void testSubmitReservationEmptyRR() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 0, 1, 5, 3);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert.assertTrue(message
          .startsWith("No resources have been specified to reserve"));
      LOG.info(message);
    }
  }

  @Test
  public void testSubmitReservationInvalidDuration() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 3, 4);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert.assertTrue(message.startsWith("The time difference"));
      Assert
          .assertTrue(message
              .contains("must  be greater or equal to the minimum resource duration"));
      LOG.info(message);
    }
  }

  @Test
  public void testSubmitReservationExceedsGangSize() {
    ReservationSubmissionRequest request =
        createSimpleReservationSubmissionRequest(1, 1, 1, 5, 4);
    Resource resource = Resource.newInstance(512, 1);
    when(plan.getTotalCapacity()).thenReturn(resource);
    Plan plan = null;
    try {
      plan =
          rrValidator.validateReservationSubmissionRequest(rSystem, request,
              ReservationSystemTestUtil.getNewReservationId());
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message
              .startsWith("The size of the largest gang in the reservation refinition"));
      Assert.assertTrue(message.contains("exceed the capacity available "));
      LOG.info(message);
    }
  }

  @Test
  public void testUpdateReservationNormal() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 5, 3);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(plan);
  }

  @Test
  public void testUpdateReservationNoID() {
    ReservationUpdateRequest request = new ReservationUpdateRequestPBImpl();
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message
              .startsWith("Missing reservation id. Please try again by specifying a reservation id."));
      LOG.info(message);
    }
  }

  @Test
  public void testUpdateReservationDoesnotExist() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 5, 4);
    ReservationId rId = request.getReservationId();
    when(rSystem.getQueueForReservation(rId)).thenReturn(null);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message.equals(MessageFormat
              .format(
                  "The specified reservation with ID: {0} is unknown. Please try again with a valid reservation.",
                  rId)));
      LOG.info(message);
    }
  }

  @Test
  public void testUpdateReservationInvalidPlan() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 5, 4);
    when(rSystem.getPlan(PLAN_NAME)).thenReturn(null);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message
              .endsWith(" is not associated with any valid plan. Please try again with a valid reservation."));
      LOG.info(message);
    }
  }

  @Test
  public void testUpdateReservationNoDefinition() {
    ReservationUpdateRequest request = new ReservationUpdateRequestPBImpl();
    request.setReservationId(ReservationSystemTestUtil.getNewReservationId());
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message
              .startsWith("Missing reservation definition. Please try again by specifying a reservation definition."));
      LOG.info(message);
    }
  }

  @Test
  public void testUpdateReservationInvalidDeadline() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 0, 3);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert.assertTrue(message
          .startsWith("The specified deadline: 0 is the past"));
      LOG.info(message);
    }
  }

  @Test
  public void testUpdateReservationInvalidRR() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(0, 0, 1, 5, 3);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert.assertTrue(message
          .startsWith("No resources have been specified to reserve"));
      LOG.info(message);
    }
  }

  @Test
  public void testUpdateReservationEmptyRR() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 0, 1, 5, 3);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert.assertTrue(message
          .startsWith("No resources have been specified to reserve"));
      LOG.info(message);
    }
  }

  @Test
  public void testUpdateReservationInvalidDuration() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 3, 4);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message
              .contains("must  be greater or equal to the minimum resource duration"));
      LOG.info(message);
    }
  }

  @Test
  public void testUpdateReservationExceedsGangSize() {
    ReservationUpdateRequest request =
        createSimpleReservationUpdateRequest(1, 1, 1, 5, 4);
    Resource resource = Resource.newInstance(512, 1);
    when(plan.getTotalCapacity()).thenReturn(resource);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationUpdateRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message
              .startsWith("The size of the largest gang in the reservation refinition"));
      Assert.assertTrue(message.contains("exceed the capacity available "));
      LOG.info(message);
    }
  }

  @Test
  public void testDeleteReservationNormal() {
    ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    request.setReservationId(reservationID);
    ReservationAllocation reservation = mock(ReservationAllocation.class);
    when(plan.getReservationById(reservationID)).thenReturn(reservation);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationDeleteRequest(rSystem, request);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(plan);
  }

  @Test
  public void testDeleteReservationNoID() {
    ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationDeleteRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message
              .startsWith("Missing reservation id. Please try again by specifying a reservation id."));
      LOG.info(message);
    }
  }

  @Test
  public void testDeleteReservationDoesnotExist() {
    ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
    ReservationId rId = ReservationSystemTestUtil.getNewReservationId();
    request.setReservationId(rId);
    when(rSystem.getQueueForReservation(rId)).thenReturn(null);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationDeleteRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message.equals(MessageFormat
              .format(
                  "The specified reservation with ID: {0} is unknown. Please try again with a valid reservation.",
                  rId)));
      LOG.info(message);
    }
  }

  @Test
  public void testDeleteReservationInvalidPlan() {
    ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    request.setReservationId(reservationID);
    when(rSystem.getPlan(PLAN_NAME)).thenReturn(null);
    Plan plan = null;
    try {
      plan = rrValidator.validateReservationDeleteRequest(rSystem, request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertNull(plan);
      String message = e.getMessage();
      Assert
          .assertTrue(message
              .endsWith(" is not associated with any valid plan. Please try again with a valid reservation."));
      LOG.info(message);
    }
  }

  private ReservationSubmissionRequest createSimpleReservationSubmissionRequest(
      int numRequests, int numContainers, long arrival, long deadline,
      long duration) {
    // create a request with a single atomic ask
    ReservationSubmissionRequest request =
        new ReservationSubmissionRequestPBImpl();
    ReservationDefinition rDef = new ReservationDefinitionPBImpl();
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    if (numRequests > 0) {
      ReservationRequests reqs = new ReservationRequestsPBImpl();
      rDef.setReservationRequests(reqs);
      if (numContainers > 0) {
        ReservationRequest r =
            ReservationRequest.newInstance(Resource.newInstance(1024, 1),
                numContainers, 1, duration);

        reqs.setReservationResources(Collections.singletonList(r));
        reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
      }
    }
    request.setQueue(PLAN_NAME);
    request.setReservationDefinition(rDef);
    return request;
  }

  private ReservationUpdateRequest createSimpleReservationUpdateRequest(
      int numRequests, int numContainers, long arrival, long deadline,
      long duration) {
    // create a request with a single atomic ask
    ReservationUpdateRequest request = new ReservationUpdateRequestPBImpl();
    ReservationDefinition rDef = new ReservationDefinitionPBImpl();
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    if (numRequests > 0) {
      ReservationRequests reqs = new ReservationRequestsPBImpl();
      rDef.setReservationRequests(reqs);
      if (numContainers > 0) {
        ReservationRequest r =
            ReservationRequest.newInstance(Resource.newInstance(1024, 1),
                numContainers, 1, duration);

        reqs.setReservationResources(Collections.singletonList(r));
        reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
      }
    }
    request.setReservationDefinition(rDef);
    request.setReservationId(ReservationSystemTestUtil.getNewReservationId());
    return request;
  }

}
