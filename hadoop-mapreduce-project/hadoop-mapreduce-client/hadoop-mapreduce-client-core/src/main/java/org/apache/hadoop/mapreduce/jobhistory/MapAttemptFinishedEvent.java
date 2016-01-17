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

package org.apache.hadoop.mapreduce.jobhistory;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.ProgressSplitsBlock;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Event to record successful completion of a map attempt
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MapAttemptFinishedEvent  implements HistoryEvent {

  private MapAttemptFinished datum = null;

  private TaskAttemptID attemptId;
  private TaskType taskType;
  private String taskStatus;
  private long finishTime;
  private String hostname;
  private String rackName;
  private int port;
  private long mapFinishTime;
  private String state;
  private Counters counters;
  int[][] allSplits;
  int[] clockSplits;
  int[] cpuUsages;
  int[] vMemKbytes;
  int[] physMemKbytes;

  /** 
   * Create an event for successful completion of map attempts
   * @param id Task Attempt ID
   * @param taskType Type of the task
   * @param taskStatus Status of the task
   * @param mapFinishTime Finish time of the map phase
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the map executed
   * @param port RPC port for the tracker host.
   * @param rackName Name of the rack where the map executed
   * @param state State string for the attempt
   * @param counters Counters for the attempt
   * @param allSplits the "splits", or a pixelated graph of various
   *        measurable worker node state variables against progress.
   *        Currently there are four; wallclock time, CPU time,
   *        virtual memory and physical memory. 
   *
   *        If you have no splits data, code {@code null} for this
   *        parameter. 
   */
  public MapAttemptFinishedEvent
      (TaskAttemptID id, TaskType taskType, String taskStatus, 
       long mapFinishTime, long finishTime, String hostname, int port, 
       String rackName, String state, Counters counters, int[][] allSplits) {
    this.attemptId = id;
    this.taskType = taskType;
    this.taskStatus = taskStatus;
    this.mapFinishTime = mapFinishTime;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.rackName = rackName;
    this.port = port;
    this.state = state;
    this.counters = counters;
    this.allSplits = allSplits;
    this.clockSplits = ProgressSplitsBlock.arrayGetWallclockTime(allSplits);
    this.cpuUsages = ProgressSplitsBlock.arrayGetCPUTime(allSplits);
    this.vMemKbytes = ProgressSplitsBlock.arrayGetVMemKbytes(allSplits);
    this.physMemKbytes = ProgressSplitsBlock.arrayGetPhysMemKbytes(allSplits);
  }

  /** 
   * @deprecated please use the constructor with an additional
   *              argument, an array of splits arrays instead.  See
   *              {@link org.apache.hadoop.mapred.ProgressSplitsBlock}
   *              for an explanation of the meaning of that parameter.
   *
   * Create an event for successful completion of map attempts
   * @param id Task Attempt ID
   * @param taskType Type of the task
   * @param taskStatus Status of the task
   * @param mapFinishTime Finish time of the map phase
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the map executed
   * @param state State string for the attempt
   * @param counters Counters for the attempt
   */
  @Deprecated
  public MapAttemptFinishedEvent
      (TaskAttemptID id, TaskType taskType, String taskStatus, 
       long mapFinishTime, long finishTime, String hostname,
       String state, Counters counters) {
    this(id, taskType, taskStatus, mapFinishTime, finishTime, hostname, -1, "",
        state, counters, null);
  }
  
  
  MapAttemptFinishedEvent() {}

  public Object getDatum() {
    if (datum == null) {
      datum = new MapAttemptFinished();
      datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
      datum.setAttemptId(new Utf8(attemptId.toString()));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setTaskStatus(new Utf8(taskStatus));
      datum.setMapFinishTime(mapFinishTime);
      datum.setFinishTime(finishTime);
      datum.setHostname(new Utf8(hostname));
      datum.setPort(port);
      if (rackName != null) {
        datum.setRackname(new Utf8(rackName));
      }
      datum.setState(new Utf8(state));
      datum.setCounters(EventWriter.toAvro(counters));

      datum.setClockSplits(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetWallclockTime(allSplits)));
      datum.setCpuUsages(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetCPUTime(allSplits)));
      datum.setVMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetVMemKbytes(allSplits)));
      datum.setPhysMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetPhysMemKbytes(allSplits)));
    }
    return datum;
  }

  public void setDatum(Object oDatum) {
    this.datum = (MapAttemptFinished)oDatum;
    this.attemptId = TaskAttemptID.forName(datum.getAttemptId().toString());
    this.taskType = TaskType.valueOf(datum.getTaskType().toString());
    this.taskStatus = datum.getTaskStatus().toString();
    this.mapFinishTime = datum.getMapFinishTime();
    this.finishTime = datum.getFinishTime();
    this.hostname = datum.getHostname().toString();
    this.rackName = datum.getRackname().toString();
    this.port = datum.getPort();
    this.state = datum.getState().toString();
    this.counters = EventReader.fromAvro(datum.getCounters());
    this.clockSplits = AvroArrayUtils.fromAvro(datum.getClockSplits());
    this.cpuUsages = AvroArrayUtils.fromAvro(datum.getCpuUsages());
    this.vMemKbytes = AvroArrayUtils.fromAvro(datum.getVMemKbytes());
    this.physMemKbytes = AvroArrayUtils.fromAvro(datum.getPhysMemKbytes());
  }

  /** Get the task ID */
  public TaskID getTaskId() { return attemptId.getTaskID(); }
  /** Get the attempt id */
  public TaskAttemptID getAttemptId() {
    return attemptId;
  }

  /** Get the task type */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Get the task status */
  public String getTaskStatus() { return taskStatus.toString(); }
  /** Get the map phase finish time */
  public long getMapFinishTime() { return mapFinishTime; }
  /** Get the attempt finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the host name */
  public String getHostname() { return hostname.toString(); }
  /** Get the tracker rpc port */
  public int getPort() { return port; }
  
  /** Get the rack name */
  public String getRackName() {
    return rackName == null ? null : rackName.toString();
  }
  
  /** Get the state string */
  public String getState() { return state.toString(); }
  /** Get the counters */
  Counters getCounters() { return counters; }
  /** Get the event type */
   public EventType getEventType() {
    return EventType.MAP_ATTEMPT_FINISHED;
  }

  public int[] getClockSplits() {
    return clockSplits;
  }
  public int[] getCpuUsages() {
    return cpuUsages;
  }
  public int[] getVMemKbytes() {
    return vMemKbytes;
  }
  public int[] getPhysMemKbytes() {
    return physMemKbytes;
  }
  
}
