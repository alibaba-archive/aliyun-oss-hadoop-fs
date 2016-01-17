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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * Base writer class to write the Timeline data.
 */
@Private
@Unstable
public abstract class TimelineWriter {

  private static final Log LOG = LogFactory
      .getLog(TimelineWriter.class);

  private UserGroupInformation authUgi;
  private Client client;
  private URI resURI;

  public TimelineWriter(UserGroupInformation authUgi, Client client,
      URI resURI) {
    this.authUgi = authUgi;
    this.client = client;
    this.resURI = resURI;
  }

  public void close() throws Exception {
    // DO NOTHING
  }

  public TimelinePutResponse putEntities(
      TimelineEntity... entities) throws IOException, YarnException {
    TimelineEntities entitiesContainer = new TimelineEntities();
    for (TimelineEntity entity : entities) {
      if (entity.getEntityId() == null || entity.getEntityType() == null) {
        throw new YarnException("Incomplete entity without entity id/type");
      }
      entitiesContainer.addEntity(entity);
    }
    ClientResponse resp = doPosting(entitiesContainer, null);
    return resp.getEntity(TimelinePutResponse.class);
  }

  public void putDomain(TimelineDomain domain) throws IOException,
      YarnException {
    doPosting(domain, "domain");
  }

  public abstract TimelinePutResponse putEntities(
      ApplicationAttemptId appAttemptId, TimelineEntityGroupId groupId,
      TimelineEntity... entities) throws IOException, YarnException;

  public abstract void putDomain(ApplicationAttemptId appAttemptId,
      TimelineDomain domain) throws IOException, YarnException;

  private ClientResponse doPosting(final Object obj, final String path)
      throws IOException, YarnException {
    ClientResponse resp;
    try {
      resp = authUgi.doAs(new PrivilegedExceptionAction<ClientResponse>() {
        @Override
        public ClientResponse run() throws Exception {
          return doPostingObject(obj, path);
        }
      });
    } catch (UndeclaredThrowableException e) {
      throw new IOException(e.getCause());
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
    if (resp == null ||
        resp.getClientResponseStatus() != ClientResponse.Status.OK) {
      String msg =
          "Failed to get the response from the timeline server.";
      LOG.error(msg);
      if (LOG.isDebugEnabled() && resp != null) {
        String output = resp.getEntity(String.class);
        LOG.debug("HTTP error code: " + resp.getStatus()
            + " Server response : \n" + output);
      }
      throw new YarnException(msg);
    }
    return resp;
  }

  @Private
  @VisibleForTesting
  public ClientResponse doPostingObject(Object object, String path) {
    WebResource webResource = client.resource(resURI);
    if (path == null) {
      return webResource.accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .post(ClientResponse.class, object);
    } else if (path.equals("domain")) {
      return webResource.path(path).accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .put(ClientResponse.class, object);
    } else {
      throw new YarnRuntimeException("Unknown resource type");
    }
  }
}
