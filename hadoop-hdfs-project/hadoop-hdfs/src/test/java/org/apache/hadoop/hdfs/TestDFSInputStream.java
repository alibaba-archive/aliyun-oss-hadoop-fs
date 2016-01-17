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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.hamcrest.CoreMatchers.equalTo;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.Assume;
import org.junit.Test;

public class TestDFSInputStream {
  private void testSkipInner(MiniDFSCluster cluster) throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSClient client = fs.dfs;
    Path file = new Path("/testfile");
    int fileLength = 1 << 22;
    byte[] fileContent = new byte[fileLength];
    for (int i = 0; i < fileLength; i++) {
      fileContent[i] = (byte) (i % 133);
    }
    FSDataOutputStream fout = fs.create(file);
    fout.write(fileContent);
    fout.close();
    Random random = new Random();
    for (int i = 3; i < 18; i++) {
      DFSInputStream fin = client.open("/testfile");
      for (long pos = 0; pos < fileLength;) {
        long skip = random.nextInt(1 << i) + 1;
        long skipped = fin.skip(skip);
        if (pos + skip >= fileLength) {
          assertEquals(fileLength, pos + skipped);
          break;
        } else {
          assertEquals(skip, skipped);
          pos += skipped;
          int data = fin.read();
          assertEquals(pos % 133, data);
          pos += 1;
        }
      }
      fin.close();
    }
  }

  @Test(timeout=60000)
  public void testSkipWithRemoteBlockReader() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      testSkipInner(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testSkipWithRemoteBlockReader2() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      testSkipInner(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testSkipWithLocalBlockReader() throws IOException {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(),
          "TestShortCircuitLocalRead._PORT.sock").getAbsolutePath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      DFSInputStream.tcpReadsDisabledForTesting = true;
      testSkipInner(cluster);
    } finally {
      DFSInputStream.tcpReadsDisabledForTesting = false;
      cluster.shutdown();
      sockDir.close();
    }
  }

  @Test(timeout=60000)
  public void testSeekToNewSource() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    Path path = new Path("/testfile");
    DFSTestUtil.createFile(fs, path, 1024, (short) 3, 0);
    DFSInputStream fin = fs.dfs.open("/testfile");
    try {
      fin.seekToNewSource(100);
      assertEquals(100, fin.getPos());
      DatanodeInfo firstNode = fin.getCurrentDatanode();
      assertNotNull(firstNode);
      fin.seekToNewSource(100);
      assertEquals(100, fin.getPos());
      assertFalse(firstNode.equals(fin.getCurrentDatanode()));
    } finally {
      fin.close();
      cluster.shutdown();
    }
  }
}
