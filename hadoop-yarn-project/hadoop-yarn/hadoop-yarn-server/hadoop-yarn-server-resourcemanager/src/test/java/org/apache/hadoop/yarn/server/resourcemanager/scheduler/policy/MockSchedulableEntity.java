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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;


public class MockSchedulableEntity implements SchedulableEntity {
  
  private String id;
  private long serial = 0;
  private Priority priority;

  public MockSchedulableEntity() { }
  
  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }
  
  public void setSerial(long serial) {
    this.serial = serial;
  }
  
  public long getSerial() {
    return serial; 
  }
  
  public void setUsed(Resource value) {
    schedulingResourceUsage.setUsed(CommonNodeLabelsManager.ANY, value);
  }
  
  public void setPending(Resource value) {
    schedulingResourceUsage.setPending(CommonNodeLabelsManager.ANY, value);
  }
  
  private ResourceUsage schedulingResourceUsage = new ResourceUsage();
  
  @Override
  public ResourceUsage getSchedulingResourceUsage() {
    return schedulingResourceUsage;
  }
  
  @Override
  public int compareInputOrderTo(SchedulableEntity other) {
    if (other instanceof MockSchedulableEntity) {
      MockSchedulableEntity r2 = (MockSchedulableEntity) other;
      int res = (int) Math.signum(getSerial() - r2.getSerial());
      return res;
    }
    return 1;//let other types go before this, if any
  }

  @Override
  public Priority getPriority() {
    return priority;
  }

  public void setApplicationPriority(Priority priority) {
    this.priority = priority;
  }
}
