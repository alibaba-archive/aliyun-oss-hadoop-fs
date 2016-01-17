/*
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
package org.apache.hadoop.yarn.server.nodemanager.metrics;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.yarn.api.records.Resource;

import com.google.common.annotations.VisibleForTesting;

@Metrics(about="Metrics for node manager", context="yarn")
public class NodeManagerMetrics {
  @Metric MutableCounterInt containersLaunched;
  @Metric MutableCounterInt containersCompleted;
  @Metric MutableCounterInt containersFailed;
  @Metric MutableCounterInt containersKilled;
  @Metric("# of initializing containers")
      MutableGaugeInt containersIniting;
  @Metric MutableGaugeInt containersRunning;
  @Metric("Current allocated memory in GB")
      MutableGaugeInt allocatedGB;
  @Metric("Current # of allocated containers")
      MutableGaugeInt allocatedContainers;
  @Metric MutableGaugeInt availableGB;
  @Metric("Current allocated Virtual Cores")
      MutableGaugeInt allocatedVCores;
  @Metric MutableGaugeInt availableVCores;
  @Metric("Container launch duration")
      MutableRate containerLaunchDuration;
  @Metric("# of bad local dirs")
      MutableGaugeInt badLocalDirs;
  @Metric("# of bad log dirs")
      MutableGaugeInt badLogDirs;
  @Metric("Disk utilization % on good local dirs")
      MutableGaugeInt goodLocalDirsDiskUtilizationPerc;
  @Metric("Disk utilization % on good log dirs")
      MutableGaugeInt goodLogDirsDiskUtilizationPerc;

  private JvmMetrics jvmMetrics = null;

  private long allocatedMB;
  private long availableMB;

  public NodeManagerMetrics(JvmMetrics jvmMetrics) {
    this.jvmMetrics = jvmMetrics;
  }

  public static NodeManagerMetrics create() {
    return create(DefaultMetricsSystem.instance());
  }

  static NodeManagerMetrics create(MetricsSystem ms) {
    JvmMetrics jm = JvmMetrics.create("NodeManager", null, ms);
    return ms.register(new NodeManagerMetrics(jm));
  }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }

  // Potential instrumentation interface methods

  public void launchedContainer() {
    containersLaunched.incr();
  }

  public void completedContainer() {
    containersCompleted.incr();
  }

  public void failedContainer() {
    containersFailed.incr();
  }

  public void killedContainer() {
    containersKilled.incr();
  }

  public void initingContainer() {
    containersIniting.incr();
  }

  public void endInitingContainer() {
    containersIniting.decr();
  }

  public void runningContainer() {
    containersRunning.incr();
  }

  public void endRunningContainer() {
    containersRunning.decr();
  }

  public void allocateContainer(Resource res) {
    allocatedContainers.incr();
    allocatedMB = allocatedMB + res.getMemory();
    allocatedGB.set((int)Math.ceil(allocatedMB/1024d));
    availableMB = availableMB - res.getMemory();
    availableGB.set((int)Math.floor(availableMB/1024d));
    allocatedVCores.incr(res.getVirtualCores());
    availableVCores.decr(res.getVirtualCores());
  }

  public void releaseContainer(Resource res) {
    allocatedContainers.decr();
    allocatedMB = allocatedMB - res.getMemory();
    allocatedGB.set((int)Math.ceil(allocatedMB/1024d));
    availableMB = availableMB + res.getMemory();
    availableGB.set((int)Math.floor(availableMB/1024d));
    allocatedVCores.decr(res.getVirtualCores());
    availableVCores.incr(res.getVirtualCores());
  }

  public void changeContainer(Resource before, Resource now) {
    int deltaMB = now.getMemory() - before.getMemory();
    int deltaVCores = now.getVirtualCores() - before.getVirtualCores();
    allocatedMB = allocatedMB + deltaMB;
    allocatedGB.set((int)Math.ceil(allocatedMB/1024d));
    availableMB = availableMB - deltaMB;
    availableGB.set((int)Math.floor(availableMB/1024d));
    allocatedVCores.incr(deltaVCores);
    availableVCores.decr(deltaVCores);
  }

  public void addResource(Resource res) {
    availableMB = availableMB + res.getMemory();
    availableGB.incr((int)Math.floor(availableMB/1024d));
    availableVCores.incr(res.getVirtualCores());
  }

  public void addContainerLaunchDuration(long value) {
    containerLaunchDuration.add(value);
  }

  public void setBadLocalDirs(int badLocalDirs) {
    this.badLocalDirs.set(badLocalDirs);
  }

  public void setBadLogDirs(int badLogDirs) {
    this.badLogDirs.set(badLogDirs);
  }

  public void setGoodLocalDirsDiskUtilizationPerc(
      int goodLocalDirsDiskUtilizationPerc) {
    this.goodLocalDirsDiskUtilizationPerc.set(goodLocalDirsDiskUtilizationPerc);
  }

  public void setGoodLogDirsDiskUtilizationPerc(
      int goodLogDirsDiskUtilizationPerc) {
    this.goodLogDirsDiskUtilizationPerc.set(goodLogDirsDiskUtilizationPerc);
  }

  public int getRunningContainers() {
    return containersRunning.value();
  }

  @VisibleForTesting
  public int getKilledContainers() {
    return containersKilled.value();
  }

  @VisibleForTesting
  public int getFailedContainers() {
    return containersFailed.value();
  }

  @VisibleForTesting
  public int getCompletedContainers() {
    return containersCompleted.value();
  }

  @VisibleForTesting
  public int getBadLogDirs() {
    return badLogDirs.value();
  }

  @VisibleForTesting
  public int getBadLocalDirs() {
    return badLocalDirs.value();
  }

  @VisibleForTesting
  public int getGoodLogDirsDiskUtilizationPerc() {
    return goodLogDirsDiskUtilizationPerc.value();
  }

  @VisibleForTesting
  public int getGoodLocalDirsDiskUtilizationPerc() {
    return goodLocalDirsDiskUtilizationPerc.value();
  }

}
