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
package org.apache.hadoop.mapreduce.tools;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doReturn;

public class TestCLI {
  private static String jobIdStr = "job_1015298225799_0015";

  @Test
  public void testListAttemptIdsWithValidInput() throws Exception {
    JobID jobId = JobID.forName(jobIdStr);
    Cluster mockCluster = mock(Cluster.class);
    Job job = mock(Job.class);
    CLI cli = spy(new CLI(new Configuration()));

    doReturn(mockCluster).when(cli).createCluster();
    when(job.getTaskReports(TaskType.MAP)).thenReturn(
        getTaskReports(jobId, TaskType.MAP));
    when(job.getTaskReports(TaskType.REDUCE)).thenReturn(
        getTaskReports(jobId, TaskType.REDUCE));
    when(mockCluster.getJob(jobId)).thenReturn(job);

    int retCode_MAP = cli.run(new String[] { "-list-attempt-ids", jobIdStr,
        "MAP", "running" });
    // testing case insensitive behavior
    int retCode_map = cli.run(new String[] { "-list-attempt-ids", jobIdStr,
        "map", "running" });

    int retCode_REDUCE = cli.run(new String[] { "-list-attempt-ids", jobIdStr,
        "REDUCE", "running" });

    int retCode_completed = cli.run(new String[] { "-list-attempt-ids",
        jobIdStr, "REDUCE", "completed" });

    assertEquals("MAP is a valid input,exit code should be 0", 0, retCode_MAP);
    assertEquals("map is a valid input,exit code should be 0", 0, retCode_map);
    assertEquals("REDUCE is a valid input,exit code should be 0", 0,
        retCode_REDUCE);
    assertEquals(
        "REDUCE and completed are a valid inputs to -list-attempt-ids,exit code should be 0",
        0, retCode_completed);

    verify(job, times(2)).getTaskReports(TaskType.MAP);
    verify(job, times(2)).getTaskReports(TaskType.REDUCE);
  }

  @Test
  public void testListAttemptIdsWithInvalidInputs() throws Exception {
    JobID jobId = JobID.forName(jobIdStr);
    Cluster mockCluster = mock(Cluster.class);
    Job job = mock(Job.class);
    CLI cli = spy(new CLI());

    doReturn(mockCluster).when(cli).createCluster();
    when(mockCluster.getJob(jobId)).thenReturn(job);

    int retCode_JOB_SETUP = cli.run(new String[] { "-list-attempt-ids",
        jobIdStr, "JOB_SETUP", "running" });

    int retCode_JOB_CLEANUP = cli.run(new String[] { "-list-attempt-ids",
        jobIdStr, "JOB_CLEANUP", "running" });

    int retCode_invalidTaskState = cli.run(new String[] { "-list-attempt-ids",
        jobIdStr, "REDUCE", "complete" });

    assertEquals("JOB_SETUP is an invalid input,exit code should be -1", -1,
        retCode_JOB_SETUP);
    assertEquals("JOB_CLEANUP is an invalid input,exit code should be -1", -1,
        retCode_JOB_CLEANUP);
    assertEquals("complete is an invalid input,exit code should be -1", -1,
        retCode_invalidTaskState);

  }

  private TaskReport[] getTaskReports(JobID jobId, TaskType type) {
    return new TaskReport[] { new TaskReport(), new TaskReport() };
  }

  @Test
  public void testJobKIll() throws Exception {
    Cluster mockCluster = mock(Cluster.class);
    CLI cli = spy(new CLI(new Configuration()));
    doReturn(mockCluster).when(cli).createCluster();
    String jobId1 = "job_1234654654_001";
    String jobId2 = "job_1234654654_002";
    String jobId3 = "job_1234654654_003";
    String jobId4 = "job_1234654654_004";
    Job mockJob1 = mockJob(mockCluster, jobId1, State.RUNNING);
    Job mockJob2 = mockJob(mockCluster, jobId2, State.KILLED);
    Job mockJob3 = mockJob(mockCluster, jobId3, State.FAILED);
    Job mockJob4 = mockJob(mockCluster, jobId4, State.PREP);

    int exitCode1 = cli.run(new String[] { "-kill", jobId1 });
    assertEquals(0, exitCode1);
    verify(mockJob1, times(1)).killJob();

    int exitCode2 = cli.run(new String[] { "-kill", jobId2 });
    assertEquals(-1, exitCode2);
    verify(mockJob2, times(0)).killJob();

    int exitCode3 = cli.run(new String[] { "-kill", jobId3 });
    assertEquals(-1, exitCode3);
    verify(mockJob3, times(0)).killJob();

    int exitCode4 = cli.run(new String[] { "-kill", jobId4 });
    assertEquals(0, exitCode4);
    verify(mockJob4, times(1)).killJob();
  }

  private Job mockJob(Cluster mockCluster, String jobId, State jobState)
      throws IOException, InterruptedException {
    Job mockJob = mock(Job.class);
    when(mockCluster.getJob(JobID.forName(jobId))).thenReturn(mockJob);
    JobStatus status = new JobStatus(null, 0, 0, 0, 0, jobState,
        JobPriority.HIGH, null, null, null, null);
    when(mockJob.getStatus()).thenReturn(status);
    return mockJob;
  }

  @Test
  public void testGetJob() throws Exception {
    Configuration conf = new Configuration();
    long sleepTime = 100;
    conf.setLong(MRJobConfig.MR_CLIENT_JOB_RETRY_INTERVAL, sleepTime);
    Cluster mockCluster = mock(Cluster.class);
    JobID jobId1 = JobID.forName("job_1234654654_001");
    when(mockCluster.getJob(jobId1)).thenReturn(null);

    for (int i = 0; i < 2; ++i) {
      conf.setInt(MRJobConfig.MR_CLIENT_JOB_MAX_RETRIES, i);
      CLI cli = spy(new CLI(conf));
      cli.cluster = mockCluster;
      doReturn(mockCluster).when(cli).createCluster();
      long start = Time.monotonicNow();
      cli.getJob(jobId1);
      long end = Time.monotonicNow();
      Assert.assertTrue(end - start > (i * sleepTime));
      Assert.assertTrue(end - start < ((i + 1) * sleepTime));
    }
  }
}
