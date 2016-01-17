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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntry;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONException;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServicesNodeLabels extends JerseyTestBase {

  private static final Log LOG = LogFactory
      .getLog(TestRMWebServicesNodeLabels.class);

  private static MockRM rm;
  private YarnConfiguration conf;

  private String userName;
  private String notUserName;
  private RMWebServices rmWebService;

  private Injector injector = Guice.createInjector(new ServletModule() {

    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      try {
        userName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ioe) {
        throw new RuntimeException("Unable to get current user name "
            + ioe.getMessage(), ioe);
      }
      notUserName = userName + "abc123";
      conf = new YarnConfiguration();
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);
      rm = new MockRM(conf);
      rmWebService = new RMWebServices(rm,conf);
      bind(RMWebServices.class).toInstance(rmWebService);
      bind(GenericExceptionHandler.class);
      bind(ResourceManager.class).toInstance(rm);
      filter("/*").through(
          TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
      serve("/*").with(GuiceContainer.class);
    }
  });

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  public TestRMWebServicesNodeLabels() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testNodeLabels() throws JSONException, Exception {
    WebResource r = resource();

    ClientResponse response;

    // Add a label
    NodeLabelsInfo nlsifo = new NodeLabelsInfo();
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("a"));
    response =
        r.path("ws").path("v1").path("cluster")
            .path("add-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nlsifo, NodeLabelsInfo.class), MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(1, nlsifo.getNodeLabels().size());
    for (NodeLabelInfo nl : nlsifo.getNodeLabelsInfo()) {
      assertEquals("a", nl.getName());
      assertTrue(nl.getExclusivity());
    }
    
    // Add another
    nlsifo = new NodeLabelsInfo();
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("b", false));
    response =
        r.path("ws").path("v1").path("cluster")
            .path("add-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nlsifo, NodeLabelsInfo.class), MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(2, nlsifo.getNodeLabels().size());
    // Verify exclusivity for 'y' as false
    for (NodeLabelInfo nl : nlsifo.getNodeLabelsInfo()) {
      if (nl.getName().equals("b")) {
        assertFalse(nl.getExclusivity());
      }
    }
    
    // Add labels to a node
    MultivaluedMapImpl params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    // Add labels to another node
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid1:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    // Add labels to another node
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid2:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    // Verify, using get-labels-to-Nodes
    response =
        r.path("ws").path("v1").path("cluster")
            .path("label-mappings").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    LabelsToNodesInfo ltni = response.getEntity(LabelsToNodesInfo.class);
    assertEquals(2, ltni.getLabelsToNodes().size());
    NodeIDsInfo nodes = ltni.getLabelsToNodes().get(
        new NodeLabelInfo("b", false));
    assertTrue(nodes.getNodeIDs().contains("nid2:0"));
    assertTrue(nodes.getNodeIDs().contains("nid1:0"));
    nodes = ltni.getLabelsToNodes().get(new NodeLabelInfo("a"));
    assertTrue(nodes.getNodeIDs().contains("nid:0"));

    // Verify, using get-labels-to-Nodes for specified set of labels
    params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("label-mappings").queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ltni = response.getEntity(LabelsToNodesInfo.class);
    assertEquals(1, ltni.getLabelsToNodes().size());
    nodes = ltni.getLabelsToNodes().get(new NodeLabelInfo("a"));
    assertTrue(nodes.getNodeIDs().contains("nid:0"));

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().contains(new NodeLabelInfo("a")));

    
    // Replace
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().contains(
        new NodeLabelInfo("b", false)));
            
    // Replace labels using node-to-labels
    NodeToLabelsEntryList ntli = new NodeToLabelsEntryList();
    ArrayList<String> labels = new ArrayList<String>();
    labels.add("a");
    NodeToLabelsEntry nli = new NodeToLabelsEntry("nid:0", labels);
    ntli.getNodeToLabels().add(nli);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("replace-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(ntli, NodeToLabelsEntryList.class),
              MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
        
    // Verify, using node-to-labels
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-to-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    NodeToLabelsInfo ntlinfo = response.getEntity(NodeToLabelsInfo.class);
    NodeLabelsInfo nlinfo = ntlinfo.getNodeToLabels().get("nid:0");
    assertEquals(1, nlinfo.getNodeLabels().size());
    assertTrue(nlinfo.getNodeLabelsInfo().contains(new NodeLabelInfo("a")));
    
    // Remove all
    params = new MultivaluedMapImpl();
    params.add("labels", "");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");
    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().isEmpty());
    
    // Add a label back for auth tests
    params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().contains(new NodeLabelInfo("a")));
    
    // Auth fail replace labels on node
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", notUserName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().contains(new NodeLabelInfo("a")));
    
    // Fail to add a label with post
    response =
        r.path("ws").path("v1").path("cluster")
            .path("add-node-labels").queryParam("user.name", notUserName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\":\"c\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(2, nlsifo.getNodeLabels().size());
    
    // Remove cluster label (succeed, we no longer need it)
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(1, nlsifo.getNodeLabels().size());
    for (NodeLabelInfo nl : nlsifo.getNodeLabelsInfo()) {
      assertEquals("a", nl.getName());
      assertTrue(nl.getExclusivity());
    }
    
    // Remove cluster label with post
    params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(0, nlsifo.getNodeLabels().size());

    // Following test cases are to test replace when distributed node label
    // configuration is on
    // Reset for testing : add cluster labels
    nlsifo = new NodeLabelsInfo();
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("x", false));
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("y", false));
    response =
        r.path("ws")
            .path("v1")
            .path("cluster")
            .path("add-node-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nlsifo, NodeLabelsInfo.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);
    // Reset for testing : Add labels to a node
    params = new MultivaluedMapImpl();
    params.add("labels", "y");
    response =
        r.path("ws").path("v1").path("cluster").path("nodes").path("nid:0")
            .path("replace-labels").queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    //setting rmWebService for non Centralized NodeLabel Configuration
    rmWebService.isCentralizedNodeLabelConfiguration = false;

    // Case1 : Replace labels using node-to-labels
    ntli = new NodeToLabelsEntryList();
    labels = new ArrayList<String>();
    labels.add("x");
    nli = new NodeToLabelsEntry("nid:0", labels);
    ntli.getNodeToLabels().add(nli);
    response =
        r.path("ws")
            .path("v1")
            .path("cluster")
            .path("replace-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(ntli, NodeToLabelsEntryList.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);

    // Verify, using node-to-labels that previous operation has failed
    response =
        r.path("ws").path("v1").path("cluster").path("get-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ntlinfo = response.getEntity(NodeToLabelsInfo.class);
    nlinfo = ntlinfo.getNodeToLabels().get("nid:0");
    assertEquals(1, nlinfo.getNodeLabels().size());
    assertFalse(nlinfo.getNodeLabelsInfo().contains(
        new NodeLabelInfo("x", false)));

    // Case2 : failure to Replace labels using replace-labels
    response =
        r.path("ws").path("v1").path("cluster").path("nodes").path("nid:0")
            .path("replace-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabelName\": [\"x\"]}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    // Verify, using node-to-labels that previous operation has failed
    response =
        r.path("ws").path("v1").path("cluster").path("get-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ntlinfo = response.getEntity(NodeToLabelsInfo.class);
    nlinfo = ntlinfo.getNodeToLabels().get("nid:0");
    assertEquals(1, nlinfo.getNodeLabels().size());
    assertFalse(nlinfo.getNodeLabelsInfo().contains(
        new NodeLabelInfo("x", false)));

    //  Case3 : Remove cluster label should be successful
    params = new MultivaluedMapImpl();
    params.add("labels", "x");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(new NodeLabelInfo("y", false),
        nlsifo.getNodeLabelsInfo().get(0));
    assertEquals("y", nlsifo.getNodeLabelsInfo().get(0).getName());
    assertFalse(nlsifo.getNodeLabelsInfo().get(0).getExclusivity());

    // Remove y
    params = new MultivaluedMapImpl();
    params.add("labels", "y");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertTrue(nlsifo.getNodeLabelsInfo().isEmpty());

    // add a new nodelabel with exclusity
    nlsifo = new NodeLabelsInfo();
    nlsifo.getNodeLabelsInfo().add(new NodeLabelInfo("z", false));
    response =
        r.path("ws")
            .path("v1")
            .path("cluster")
            .path("add-node-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nlsifo, NodeLabelsInfo.class),
                MediaType.APPLICATION_JSON).post(ClientResponse.class);

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    nlsifo = response.getEntity(NodeLabelsInfo.class);
    assertEquals("z", nlsifo.getNodeLabelsInfo().get(0).getName());
    assertFalse(nlsifo.getNodeLabelsInfo().get(0).getExclusivity());
    assertEquals(1, nlsifo.getNodeLabels().size());
  }

  @SuppressWarnings("rawtypes")
  private String toJson(Object nsli, Class klass) throws Exception {
    StringWriter sw = new StringWriter();
    JSONJAXBContext ctx = new JSONJAXBContext(klass);
    JSONMarshaller jm = ctx.createJSONMarshaller();
    jm.marshallToJSON(nsli, sw);
    return sw.toString();
  }
}
