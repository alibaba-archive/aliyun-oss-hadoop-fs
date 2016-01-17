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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;

public class TestDistributedFileSystem {
  private static final Random RAN = new Random();

  static {
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
  }

  private boolean dualPortTesting = false;
  
  private boolean noXmlDefaults = false;
  
  private HdfsConfiguration getTestConfiguration() {
    HdfsConfiguration conf;
    if (noXmlDefaults) {
      conf = new HdfsConfiguration(false);
      String namenodeDir = new File(MiniDFSCluster.getBaseDirectory(), "name").
          getAbsolutePath();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, namenodeDir);
      conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, namenodeDir);
    } else {
      conf = new HdfsConfiguration();
    }
    if (dualPortTesting) {
      conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          "localhost:0");
    }
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);

    return conf;
  }

  @Test
  public void testEmptyDelegationToken() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fileSys = cluster.getFileSystem();
      fileSys.getDelegationToken("");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testFileSystemCloseAll() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).
        build();
    URI address = FileSystem.getDefaultUri(conf);

    try {
      FileSystem.closeAll();

      conf = getTestConfiguration();
      FileSystem.setDefaultUri(conf, address);
      FileSystem.get(conf);
      FileSystem.get(conf);
      FileSystem.closeAll();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /**
   * Tests DFSClient.close throws no ConcurrentModificationException if 
   * multiple files are open.
   * Also tests that any cached sockets are closed. (HDFS-3359)
   */
  @Test
  public void testDFSClose() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      DistributedFileSystem fileSys = cluster.getFileSystem();

      // create two files, leaving them open
      fileSys.create(new Path("/test/dfsclose/file-0"));
      fileSys.create(new Path("/test/dfsclose/file-1"));

      // create another file, close it, and read it, so
      // the client gets a socket in its SocketCache
      Path p = new Path("/non-empty-file");
      DFSTestUtil.createFile(fileSys, p, 1L, (short)1, 0L);
      DFSTestUtil.readFile(fileSys, p);

      fileSys.close();

      DFSClient dfsClient = fileSys.getClient();
      verifyOpsUsingClosedClient(dfsClient);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  private void verifyOpsUsingClosedClient(DFSClient dfsClient) {
    Path p = new Path("/non-empty-file");
    try {
      dfsClient.getBlockSize(p.getName());
      fail("getBlockSize using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getServerDefaults();
      fail("getServerDefaults using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.reportBadBlocks(new LocatedBlock[0]);
      fail("reportBadBlocks using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getBlockLocations(p.getName(), 0, 1);
      fail("getBlockLocations using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.createSymlink("target", "link", true);
      fail("createSymlink using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getLinkTarget(p.getName());
      fail("getLinkTarget using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setReplication(p.getName(), (short) 3);
      fail("setReplication using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setStoragePolicy(p.getName(),
          HdfsConstants.ONESSD_STORAGE_POLICY_NAME);
      fail("setStoragePolicy using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getStoragePolicies();
      fail("getStoragePolicies using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      fail("setSafeMode using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.refreshNodes();
      fail("refreshNodes using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.metaSave(p.getName());
      fail("metaSave using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setBalancerBandwidth(1000L);
      fail("setBalancerBandwidth using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.finalizeUpgrade();
      fail("finalizeUpgrade using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.rollingUpgrade(RollingUpgradeAction.QUERY);
      fail("rollingUpgrade using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getInotifyEventStream();
      fail("getInotifyEventStream using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getInotifyEventStream(100L);
      fail("getInotifyEventStream using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.saveNamespace(1000L, 200L);
      fail("saveNamespace using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.rollEdits();
      fail("rollEdits using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.restoreFailedStorage("");
      fail("restoreFailedStorage using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.getContentSummary(p.getName());
      fail("getContentSummary using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setQuota(p.getName(), 1000L, 500L);
      fail("setQuota using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfsClient.setQuotaByStorageType(p.getName(), StorageType.DISK, 500L);
      fail("setQuotaByStorageType using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
  }

  @Test
  public void testDFSCloseOrdering() throws Exception {
    DistributedFileSystem fs = new MyDistributedFileSystem();
    Path path = new Path("/a");
    fs.deleteOnExit(path);
    fs.close();

    InOrder inOrder = inOrder(fs.dfs);
    inOrder.verify(fs.dfs).closeOutputStreams(eq(false));
    inOrder.verify(fs.dfs).delete(eq(path.toString()), eq(true));
    inOrder.verify(fs.dfs).close();
  }
  
  private static class MyDistributedFileSystem extends DistributedFileSystem {
    MyDistributedFileSystem() {
      statistics = new FileSystem.Statistics("myhdfs"); // can't mock finals
      dfs = mock(DFSClient.class);
    }
    @Override
    public boolean exists(Path p) {
      return true; // trick out deleteOnExit
    }
    // Symlink resolution doesn't work with a mock, since it doesn't
    // have a valid Configuration to resolve paths to the right FileSystem.
    // Just call the DFSClient directly to register the delete
    @Override
    public boolean delete(Path f, final boolean recursive) throws IOException {
      return dfs.delete(f.toUri().getPath(), recursive);
    }
  }

  @Test
  public void testDFSSeekExceptions() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      FileSystem fileSys = cluster.getFileSystem();
      String file = "/test/fileclosethenseek/file-0";
      Path path = new Path(file);
      // create file
      FSDataOutputStream output = fileSys.create(path);
      output.writeBytes("Some test data to write longer than 10 bytes");
      output.close();
      FSDataInputStream input = fileSys.open(path);
      input.seek(10);
      boolean threw = false;
      try {
        input.seek(100);
      } catch (IOException e) {
        // success
        threw = true;
      }
      assertTrue("Failed to throw IOE when seeking past end", threw);
      input.close();
      threw = false;
      try {
        input.seek(1);
      } catch (IOException e) {
        //success
        threw = true;
      }
      assertTrue("Failed to throw IOE when seeking after close", threw);
      fileSys.close();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testDFSClient() throws Exception {
    Configuration conf = getTestConfiguration();
    final long grace = 1000L;
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      final String filepathstring = "/test/LeaseChecker/foo";
      final Path[] filepaths = new Path[4];
      for(int i = 0; i < filepaths.length; i++) {
        filepaths[i] = new Path(filepathstring + i);
      }
      final long millis = Time.now();

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Method setMethod = dfs.dfs.getLeaseRenewer().getClass()
            .getDeclaredMethod("setGraceSleepPeriod", long.class);
        setMethod.setAccessible(true);
        setMethod.invoke(dfs.dfs.getLeaseRenewer(), grace);
        Method checkMethod = dfs.dfs.getLeaseRenewer().getClass()
            .getDeclaredMethod("isRunning");
        checkMethod.setAccessible(true);
        assertFalse((boolean) checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
  
        {
          //create a file
          final FSDataOutputStream out = dfs.create(filepaths[0]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //write something
          out.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close
          out.close();
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          for(int i = 0; i < 3; i++) {
            if ((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer())) {
              Thread.sleep(grace/2);
            }
          }
          //passed grace period
          assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        {
          //create file1
          final FSDataOutputStream out1 = dfs.create(filepaths[1]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //create file2
          final FSDataOutputStream out2 = dfs.create(filepaths[2]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

          //write something to file1
          out1.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close file1
          out1.close();
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

          //write something to file2
          out2.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close file2
          out2.close();
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        {
          //create file3
          final FSDataOutputStream out3 = dfs.create(filepaths[3]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          Thread.sleep(grace/4*3);
          //passed previous grace period, should still running
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //write something to file3
          out3.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close file3
          out3.close();
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          for(int i = 0; i < 3; i++) {
            if ((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer())) {
              Thread.sleep(grace/2);
            }
          }
          //passed grace period
          assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        dfs.close();
      }

      {
        // Check to see if opening a non-existent file triggers a FNF
        FileSystem fs = cluster.getFileSystem();
        Path dir = new Path("/wrwelkj");
        assertFalse("File should not exist for test.", fs.exists(dir));

        try {
          FSDataInputStream in = fs.open(dir);
          try {
            in.close();
            fs.close();
          } finally {
            assertTrue("Did not get a FileNotFoundException for non-existing" +
                " file.", false);
          }
        } catch (FileNotFoundException fnf) {
          // This is the proper exception to catch; move on.
        }

      }

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Method checkMethod = dfs.dfs.getLeaseRenewer().getClass()
            .getDeclaredMethod("isRunning");
        checkMethod.setAccessible(true);
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

        //open and check the file
        FSDataInputStream in = dfs.open(filepaths[0]);
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        assertEquals(millis, in.readLong());
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        in.close();
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        dfs.close();
      }
      
      { // test accessing DFS with ip address. should work with any hostname
        // alias or ip address that points to the interface that NameNode
        // is listening on. In this case, it is localhost.
        String uri = "hdfs://127.0.0.1:" + cluster.getNameNodePort() + 
                      "/test/ipAddress/file";
        Path path = new Path(uri);
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FSDataOutputStream out = fs.create(path);
        byte[] buf = new byte[1024];
        out.write(buf);
        out.close();
        
        FSDataInputStream in = fs.open(path);
        in.readFully(buf);
        in.close();
        fs.close();
      }
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  @Test
  public void testStatistics() throws Exception {
    int lsLimit = 2;
    final Configuration conf = getTestConfiguration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, lsLimit);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      final FileSystem fs = cluster.getFileSystem();
      Path dir = new Path("/test");
      Path file = new Path(dir, "file");
      
      int readOps = DFSTestUtil.getStatistics(fs).getReadOps();
      int writeOps = DFSTestUtil.getStatistics(fs).getWriteOps();
      int largeReadOps = DFSTestUtil.getStatistics(fs).getLargeReadOps();
      fs.mkdirs(dir);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      FSDataOutputStream out = fs.create(file, (short)1);
      out.close();
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      FileStatus status = fs.getFileStatus(file);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      fs.getFileBlockLocations(file, 0, 0);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      fs.getFileBlockLocations(status, 0, 0);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      FSDataInputStream in = fs.open(file);
      in.close();
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      fs.setReplication(file, (short)2);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      Path file1 = new Path(dir, "file1");
      fs.rename(file, file1);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      fs.getContentSummary(file1);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      
      // Iterative ls test
      for (int i = 0; i < 10; i++) {
        Path p = new Path(dir, Integer.toString(i));
        fs.mkdirs(p);
        FileStatus[] list = fs.listStatus(dir);
        if (list.length > lsLimit) {
          // if large directory, then count readOps and largeReadOps by 
          // number times listStatus iterates
          int iterations = (int)Math.ceil((double)list.length/lsLimit);
          largeReadOps += iterations;
          readOps += iterations;
        } else {
          // Single iteration in listStatus - no large read operation done
          readOps++;
        }
        
        // writeOps incremented by 1 for mkdirs
        // readOps and largeReadOps incremented by 1 or more
        checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      }
      
      fs.getStatus(file1);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      fs.getFileChecksum(file1);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      fs.setPermission(file1, new FsPermission((short)0777));
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      fs.setTimes(file1, 0L, 0L);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      fs.setOwner(file1, ugi.getUserName(), ugi.getGroupNames()[0]);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      fs.delete(dir, true);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
    } finally {
      if (cluster != null) cluster.shutdown();
    }
    
  }
  
  /** Checks statistics. -1 indicates do not check for the operations */
  private void checkStatistics(FileSystem fs, int readOps, int writeOps,
      int largeReadOps) {
    assertEquals(readOps, DFSTestUtil.getStatistics(fs).getReadOps());
    assertEquals(writeOps, DFSTestUtil.getStatistics(fs).getWriteOps());
    assertEquals(largeReadOps, DFSTestUtil.getStatistics(fs).getLargeReadOps());
  }

  @Test
  public void testFileChecksum() throws Exception {
    final long seed = RAN.nextLong();
    System.out.println("seed=" + seed);
    RAN.setSeed(seed);

    final Configuration conf = getTestConfiguration();

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2).build();
    final FileSystem hdfs = cluster.getFileSystem();

    final String nnAddr = conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        current.getShortUserName() + "x", new String[]{"user"});
    
    try {
      hdfs.getFileChecksum(new Path(
          "/test/TestNonExistingFile"));
      fail("Expecting FileNotFoundException");
    } catch (FileNotFoundException e) {
      assertTrue("Not throwing the intended exception message", e.getMessage()
          .contains("File does not exist: /test/TestNonExistingFile"));
    }

    try {
      Path path = new Path("/test/TestExistingDir/");
      hdfs.mkdirs(path);
      hdfs.getFileChecksum(path);
      fail("Expecting FileNotFoundException");
    } catch (FileNotFoundException e) {
      assertTrue("Not throwing the intended exception message", e.getMessage()
          .contains("Path is not a file: /test/TestExistingDir"));
    }

    //webhdfs
    final String webhdfsuri = WebHdfsConstants.WEBHDFS_SCHEME + "://" + nnAddr;
    System.out.println("webhdfsuri=" + webhdfsuri);
    final FileSystem webhdfs = ugi.doAs(
        new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return new Path(webhdfsuri).getFileSystem(conf);
      }
    });

    final Path dir = new Path("/filechecksum");
    final int block_size = 1024;
    final int buffer_size = conf.getInt(
        CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096);
    conf.setInt(HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);

    //try different number of blocks
    for(int n = 0; n < 5; n++) {
      //generate random data
      final byte[] data = new byte[RAN.nextInt(block_size/2-1)+n*block_size+1];
      RAN.nextBytes(data);
      System.out.println("data.length=" + data.length);
  
      //write data to a file
      final Path foo = new Path(dir, "foo" + n);
      {
        final FSDataOutputStream out = hdfs.create(foo, false, buffer_size,
            (short)2, block_size);
        out.write(data);
        out.close();
      }
      
      //compute checksum
      final FileChecksum hdfsfoocs = hdfs.getFileChecksum(foo);
      System.out.println("hdfsfoocs=" + hdfsfoocs);

      //webhdfs
      final FileChecksum webhdfsfoocs = webhdfs.getFileChecksum(foo);
      System.out.println("webhdfsfoocs=" + webhdfsfoocs);

      final Path webhdfsqualified = new Path(webhdfsuri + dir, "foo" + n);
      final FileChecksum webhdfs_qfoocs =
          webhdfs.getFileChecksum(webhdfsqualified);
      System.out.println("webhdfs_qfoocs=" + webhdfs_qfoocs);

      //create a zero byte file
      final Path zeroByteFile = new Path(dir, "zeroByteFile" + n);
      {
        final FSDataOutputStream out = hdfs.create(zeroByteFile, false,
            buffer_size, (short)2, block_size);
        out.close();
      }

      // verify the magic val for zero byte files
      {
        final FileChecksum zeroChecksum = hdfs.getFileChecksum(zeroByteFile);
        assertEquals(zeroChecksum.toString(),
            "MD5-of-0MD5-of-0CRC32:70bc8f4b72a86921468bf8e8441dce51");
      }

      //write another file
      final Path bar = new Path(dir, "bar" + n);
      {
        final FSDataOutputStream out = hdfs.create(bar, false, buffer_size,
            (short)2, block_size);
        out.write(data);
        out.close();
      }
  
      { //verify checksum
        final FileChecksum barcs = hdfs.getFileChecksum(bar);
        final int barhashcode = barcs.hashCode();
        assertEquals(hdfsfoocs.hashCode(), barhashcode);
        assertEquals(hdfsfoocs, barcs);

        //webhdfs
        assertEquals(webhdfsfoocs.hashCode(), barhashcode);
        assertEquals(webhdfsfoocs, barcs);

        assertEquals(webhdfs_qfoocs.hashCode(), barhashcode);
        assertEquals(webhdfs_qfoocs, barcs);
      }

      hdfs.setPermission(dir, new FsPermission((short)0));

      { //test permission error on webhdfs 
        try {
          webhdfs.getFileChecksum(webhdfsqualified);
          fail();
        } catch(IOException ioe) {
          FileSystem.LOG.info("GOOD: getting an exception", ioe);
        }
      }
      hdfs.setPermission(dir, new FsPermission((short)0777));
    }
    cluster.shutdown();
  }
  
  @Test
  public void testAllWithDualPort() throws Exception {
    dualPortTesting = true;

    try {
      testFileSystemCloseAll();
      testDFSClose();
      testDFSClient();
      testFileChecksum();
    } finally {
      dualPortTesting = false;
    }
  }
  
  @Test
  public void testAllWithNoXmlDefaults() throws Exception {
    // Do all the tests with a configuration that ignores the defaults in
    // the XML files.
    noXmlDefaults = true;

    try {
      testFileSystemCloseAll();
      testDFSClose();
      testDFSClient();
      testFileChecksum();
    } finally {
     noXmlDefaults = false; 
    }
  }

  @Test(timeout=120000)
  public void testLocatedFileStatusStorageIdsTypes() throws Exception {
    final Configuration conf = getTestConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();
    try {
      final DistributedFileSystem fs = cluster.getFileSystem();
      final Path testFile = new Path("/testListLocatedStatus");
      final int blockSize = 4096;
      final int numBlocks = 10;
      // Create a test file
      final int repl = 2;
      DFSTestUtil.createFile(fs, testFile, blockSize, numBlocks * blockSize,
          blockSize, (short) repl, 0xADDED);
      DFSTestUtil.waitForReplication(fs, testFile, (short) repl, 30000);
      // Get the listing
      RemoteIterator<LocatedFileStatus> it = fs.listLocatedStatus(testFile);
      assertTrue("Expected file to be present", it.hasNext());
      LocatedFileStatus stat = it.next();
      BlockLocation[] locs = stat.getBlockLocations();
      assertEquals("Unexpected number of locations", numBlocks, locs.length);

      Set<String> dnStorageIds = new HashSet<>();
      for (DataNode d : cluster.getDataNodes()) {
        try (FsDatasetSpi.FsVolumeReferences volumes = d.getFSDataset()
            .getFsVolumeReferences()) {
          for (FsVolumeSpi vol : volumes) {
            dnStorageIds.add(vol.getStorageID());
          }
        }
      }

      for (BlockLocation loc : locs) {
        String[] ids = loc.getStorageIds();
        // Run it through a set to deduplicate, since there should be no dupes
        Set<String> storageIds = new HashSet<>();
        Collections.addAll(storageIds, ids);
        assertEquals("Unexpected num storage ids", repl, storageIds.size());
        // Make sure these are all valid storage IDs
        assertTrue("Unknown storage IDs found!", dnStorageIds.containsAll
            (storageIds));
        // Check storage types are the default, since we didn't set any
        StorageType[] types = loc.getStorageTypes();
        assertEquals("Unexpected num storage types", repl, types.length);
        for (StorageType t: types) {
          assertEquals("Unexpected storage type", StorageType.DEFAULT, t);
        }
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testCreateWithCustomChecksum() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    Path testBasePath = new Path("/test/csum");
    // create args 
    Path path1 = new Path(testBasePath, "file_wtih_crc1");
    Path path2 = new Path(testBasePath, "file_with_crc2");
    ChecksumOpt opt1 = new ChecksumOpt(DataChecksum.Type.CRC32C, 512);
    ChecksumOpt opt2 = new ChecksumOpt(DataChecksum.Type.CRC32, 512);

    // common args
    FsPermission perm = FsPermission.getDefault().applyUMask(
        FsPermission.getUMask(conf));
    EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.OVERWRITE,
        CreateFlag.CREATE);
    short repl = 1;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(testBasePath);

      // create two files with different checksum types
      FSDataOutputStream out1 = dfs.create(path1, perm, flags, 4096, repl,
          131072L, null, opt1);
      FSDataOutputStream out2 = dfs.create(path2, perm, flags, 4096, repl,
          131072L, null, opt2);

      for (int i = 0; i < 1024; i++) {
        out1.write(i);
        out2.write(i);
      }
      out1.close();
      out2.close();

      // the two checksums must be different.
      MD5MD5CRC32FileChecksum sum1 =
          (MD5MD5CRC32FileChecksum)dfs.getFileChecksum(path1);
      MD5MD5CRC32FileChecksum sum2 =
          (MD5MD5CRC32FileChecksum)dfs.getFileChecksum(path2);
      assertFalse(sum1.equals(sum2));

      // check the individual params
      assertEquals(DataChecksum.Type.CRC32C, sum1.getCrcType());
      assertEquals(DataChecksum.Type.CRC32,  sum2.getCrcType());

    } finally {
      if (cluster != null) {
        cluster.getFileSystem().delete(testBasePath, true);
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=60000)
  public void testFileCloseStatus() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {
      // create a new file.
      Path file = new Path("/simpleFlush.dat");
      FSDataOutputStream output = fs.create(file);
      // write to file
      output.writeBytes("Some test data");
      output.flush();
      assertFalse("File status should be open", fs.isFileClosed(file));
      output.close();
      assertTrue("File status should be closed", fs.isFileClosed(file));
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test(timeout=60000)
  public void testListFiles() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    
    try {
      DistributedFileSystem fs = cluster.getFileSystem();
  
      final Path relative = new Path("relative");
      fs.create(new Path(relative, "foo")).close();
  
      final List<LocatedFileStatus> retVal = new ArrayList<>();
      final RemoteIterator<LocatedFileStatus> iter =
          fs.listFiles(relative, true);
      while (iter.hasNext()) {
        retVal.add(iter.next());
      }
      System.out.println("retVal = " + retVal);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=10000)
  public void testDFSClientPeerReadTimeout() throws IOException {
    final int timeout = 1000;
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, timeout);

    // only need cluster to create a dfs client to get a peer
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();     
      DistributedFileSystem dfs = cluster.getFileSystem();
      // use a dummy socket to ensure the read timesout
      ServerSocket socket = new ServerSocket(0);
      Peer peer = dfs.getClient().newConnectedPeer(
          (InetSocketAddress) socket.getLocalSocketAddress(), null, null);
      long start = Time.now();
      try {
        peer.getInputStream().read();
        Assert.fail("read should timeout");
      } catch (SocketTimeoutException ste) {
        long delta = Time.now() - start;
        if (delta < timeout*0.9) {
          throw new IOException("read timedout too soon in " + delta + " ms.",
              ste);
        }
        if (delta > timeout*1.1) {
          throw new IOException("read timedout too late in " + delta + " ms.",
              ste);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testGetServerDefaults() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      FsServerDefaults fsServerDefaults = dfs.getServerDefaults();
      Assert.assertNotNull(fsServerDefaults);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=10000)
  public void testDFSClientPeerWriteTimeout() throws IOException {
    final int timeout = 1000;
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, timeout);

    // only need cluster to create a dfs client to get a peer
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      // Write 10 MB to a dummy socket to ensure the write times out
      ServerSocket socket = new ServerSocket(0);
      Peer peer = dfs.getClient().newConnectedPeer(
        (InetSocketAddress) socket.getLocalSocketAddress(), null, null);
      long start = Time.now();
      try {
        byte[] buf = new byte[10 * 1024 * 1024];
        peer.getOutputStream().write(buf);
        long delta = Time.now() - start;
        Assert.fail("write finish in " + delta + " ms" + "but should timedout");
      } catch (SocketTimeoutException ste) {
        long delta = Time.now() - start;

        if (delta < timeout * 0.9) {
          throw new IOException("write timedout too soon in " + delta + " ms.",
              ste);
        }
        if (delta > timeout * 1.2) {
          throw new IOException("write timedout too late in " + delta + " ms.",
              ste);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testTotalDfsUsed() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fs = cluster.getFileSystem();
      // create file under root
      FSDataOutputStream File1 = fs.create(new Path("/File1"));
      File1.write("hi".getBytes());
      File1.close();
      // create file under sub-folder
      FSDataOutputStream File2 = fs.create(new Path("/Folder1/File2"));
      File2.write("hi".getBytes());
      File2.close();
      // getUsed(Path) should return total len of all the files from a path
      assertEquals(2, fs.getUsed(new Path("/Folder1")));
      //getUsed() should return total length of all files in filesystem
      assertEquals(4, fs.getUsed());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

}
