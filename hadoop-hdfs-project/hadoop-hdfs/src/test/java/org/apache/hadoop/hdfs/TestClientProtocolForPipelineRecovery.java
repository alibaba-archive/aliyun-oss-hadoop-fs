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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * This tests pipeline recovery related client protocol works correct or not.
 */
public class TestClientProtocolForPipelineRecovery {
  
  @Test public void testGetNewStamp() throws IOException {
    int numDataNodes = 1;
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
    try {
      cluster.waitActive();
      FileSystem fileSys = cluster.getFileSystem();
      NamenodeProtocols namenode = cluster.getNameNodeRpc();

      /* Test writing to finalized replicas */
      Path file = new Path("dataprotocol.dat");    
      DFSTestUtil.createFile(fileSys, file, 1L, (short)numDataNodes, 0L);
      // get the first blockid for the file
      ExtendedBlock firstBlock = DFSTestUtil.getFirstBlock(fileSys, file);

      // test getNewStampAndToken on a finalized block
      try {
        namenode.updateBlockForPipeline(firstBlock, "");
        Assert.fail("Can not get a new GS from a finalized block");
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().contains(
            "not " + BlockUCState.UNDER_CONSTRUCTION));
      }
      
      // test getNewStampAndToken on a non-existent block
      try {
        long newBlockId = firstBlock.getBlockId() + 1;
        ExtendedBlock newBlock = new ExtendedBlock(firstBlock.getBlockPoolId(),
            newBlockId, 0, firstBlock.getGenerationStamp());
        namenode.updateBlockForPipeline(newBlock, "");
        Assert.fail("Cannot get a new GS from a non-existent block");
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().contains("does not exist"));
      }

      
      /* Test RBW replicas */
      // change first block to a RBW
      DFSOutputStream out = null;
      try {
        out = (DFSOutputStream)(fileSys.append(file).
            getWrappedStream()); 
        out.write(1);
        out.hflush();
        FSDataInputStream in = null;
        try {
          in = fileSys.open(file);
          firstBlock = DFSTestUtil.getAllBlocks(in).get(0).getBlock();
        } finally {
          IOUtils.closeStream(in);
        }

        // test non-lease holder
        DFSClient dfs = ((DistributedFileSystem)fileSys).dfs;
        try {
          namenode.updateBlockForPipeline(firstBlock, "test" + dfs.clientName);
          Assert.fail("Cannot get a new GS for a non lease holder");
        } catch (LeaseExpiredException e) {
          Assert.assertTrue(e.getMessage().startsWith("Lease mismatch"));
        }

        // test null lease holder
        try {
          namenode.updateBlockForPipeline(firstBlock, null);
          Assert.fail("Cannot get a new GS for a null lease holder");
        } catch (LeaseExpiredException e) {
          Assert.assertTrue(e.getMessage().startsWith("Lease mismatch"));
        }

        // test getNewStampAndToken on a rbw block
        namenode.updateBlockForPipeline(firstBlock, dfs.clientName);
      } finally {
        IOUtils.closeStream(out);
      }
    } finally {
      cluster.shutdown();
    }
  }

  /** Test whether corrupt replicas are detected correctly during pipeline
   * recoveries.
   */
  @Test
  public void testPipelineRecoveryForLastBlock() throws IOException {
    DFSClientFaultInjector faultInjector
        = Mockito.mock(DFSClientFaultInjector.class);
    DFSClientFaultInjector oldInjector = DFSClientFaultInjector.get();
    DFSClientFaultInjector.set(faultInjector);
    Configuration conf = new HdfsConfiguration();

    conf.setInt(HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY, 3);
    MiniDFSCluster cluster = null;

    try {
      int numDataNodes = 3;
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
      cluster.waitActive();
      FileSystem fileSys = cluster.getFileSystem();

      Path file = new Path("dataprotocol1.dat");
      Mockito.when(faultInjector.failPacket()).thenReturn(true);
      DFSTestUtil.createFile(fileSys, file, 68000000L, (short)numDataNodes, 0L);

      // At this point, NN should have accepted only valid replicas.
      // Read should succeed.
      FSDataInputStream in = fileSys.open(file);
      try {
        in.read();
        // Test will fail with BlockMissingException if NN does not update the
        // replica state based on the latest report.
      } catch (org.apache.hadoop.hdfs.BlockMissingException bme) {
        Assert.fail("Block is missing because the file was closed with"
            + " corrupt replicas.");
      }
    } finally {
      DFSClientFaultInjector.set(oldInjector);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testPacketTransmissionDelay() throws Exception {
    // Make the first datanode to not relay heartbeat packet.
    DataNodeFaultInjector dnFaultInjector = new DataNodeFaultInjector() {
      @Override
      public boolean dropHeartbeatPacket() {
        return true;
      }
    };
    DataNodeFaultInjector oldDnInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector.set(dnFaultInjector);

    // Setting the timeout to be 3 seconds. Normally heartbeat packet
    // would be sent every 1.5 seconds if there is no data traffic.
    Configuration conf = new HdfsConfiguration();
    conf.set(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, "3000");
    MiniDFSCluster cluster = null;

    try {
      int numDataNodes = 2;
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      FSDataOutputStream out = fs.create(new Path("noheartbeat.dat"), (short)2);
      out.write(0x31);
      out.hflush();

      DFSOutputStream dfsOut = (DFSOutputStream)out.getWrappedStream();

      // original pipeline
      DatanodeInfo[] orgNodes = dfsOut.getPipeline();

      // Cause the second datanode to timeout on reading packet
      Thread.sleep(3500);
      out.write(0x32);
      out.hflush();

      // new pipeline
      DatanodeInfo[] newNodes = dfsOut.getPipeline();
      out.close();

      boolean contains = false;
      for (int i = 0; i < newNodes.length; i++) {
        if (orgNodes[0].getXferAddr().equals(newNodes[i].getXferAddr())) {
          throw new IOException("The first datanode should have been replaced.");
        }
        if (orgNodes[1].getXferAddr().equals(newNodes[i].getXferAddr())) {
          contains = true;
        }
      }
      Assert.assertTrue(contains);
    } finally {
      DataNodeFaultInjector.set(oldDnInjector);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test recovery on restart OOB message. It also tests the delivery of 
   * OOB ack originating from the primary datanode. Since there is only
   * one node in the cluster, failure of restart-recovery will fail the
   * test.
   */
  @Test
  public void testPipelineRecoveryOnOOB() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(HdfsClientConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY, "15");
    MiniDFSCluster cluster = null;
    try {
      int numDataNodes = 1;
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
      cluster.waitActive();
      FileSystem fileSys = cluster.getFileSystem();

      Path file = new Path("dataprotocol2.dat");
      DFSTestUtil.createFile(fileSys, file, 10240L, (short)1, 0L);
      DFSOutputStream out = (DFSOutputStream)(fileSys.append(file).
          getWrappedStream());
      out.write(1);
      out.hflush();

      DFSAdmin dfsadmin = new DFSAdmin(conf);
      DataNode dn = cluster.getDataNodes().get(0);
      final String dnAddr = dn.getDatanodeId().getIpcAddr(false);
      // issue shutdown to the datanode.
      final String[] args1 = {"-shutdownDatanode", dnAddr, "upgrade" };
      Assert.assertEquals(0, dfsadmin.run(args1));
      // Wait long enough to receive an OOB ack before closing the file.
      Thread.sleep(4000);
      // Retart the datanode 
      cluster.restartDataNode(0, true);
      // The following forces a data packet and end of block packets to be sent. 
      out.close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /** Test restart timeout */
  @Test
  public void testPipelineRecoveryOnRestartFailure() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(HdfsClientConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY, "5");
    MiniDFSCluster cluster = null;
    try {
      int numDataNodes = 2;
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
      cluster.waitActive();
      FileSystem fileSys = cluster.getFileSystem();

      Path file = new Path("dataprotocol3.dat");
      DFSTestUtil.createFile(fileSys, file, 10240L, (short)2, 0L);
      DFSOutputStream out = (DFSOutputStream)(fileSys.append(file).
          getWrappedStream());
      out.write(1);
      out.hflush();

      DFSAdmin dfsadmin = new DFSAdmin(conf);
      DataNode dn = cluster.getDataNodes().get(0);
      final String dnAddr1 = dn.getDatanodeId().getIpcAddr(false);
      // issue shutdown to the datanode.
      final String[] args1 = {"-shutdownDatanode", dnAddr1, "upgrade" };
      Assert.assertEquals(0, dfsadmin.run(args1));
      Thread.sleep(4000);
      // This should succeed without restarting the node. The restart will
      // expire and regular pipeline recovery will kick in. 
      out.close();

      // At this point there is only one node in the cluster. 
      out = (DFSOutputStream)(fileSys.append(file).
          getWrappedStream());
      out.write(1);
      out.hflush();

      dn = cluster.getDataNodes().get(1);
      final String dnAddr2 = dn.getDatanodeId().getIpcAddr(false);
      // issue shutdown to the datanode.
      final String[] args2 = {"-shutdownDatanode", dnAddr2, "upgrade" };
      Assert.assertEquals(0, dfsadmin.run(args2));
      Thread.sleep(4000);
      try {
        // close should fail
        out.close();
        assert false;
      } catch (IOException ioe) { }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
