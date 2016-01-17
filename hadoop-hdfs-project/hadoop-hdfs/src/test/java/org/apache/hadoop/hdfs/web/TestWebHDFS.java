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

package org.apache.hadoop.hdfs.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestDFSClientRetries;
import org.apache.hadoop.hdfs.TestFileCreation;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.WebHdfsInputStream;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test WebHDFS */
public class TestWebHDFS {
  static final Log LOG = LogFactory.getLog(TestWebHDFS.class);
  
  static final Random RANDOM = new Random();
  
  static final long systemStartTime = System.nanoTime();

  /** A timer for measuring performance. */
  static class Ticker {
    final String name;
    final long startTime = System.nanoTime();
    private long previousTick = startTime;

    Ticker(final String name, String format, Object... args) {
      this.name = name;
      LOG.info(String.format("\n\n%s START: %s\n",
          name, String.format(format, args)));
    }

    void tick(final long nBytes, String format, Object... args) {
      final long now = System.nanoTime();
      if (now - previousTick > 10000000000L) {
        previousTick = now;
        final double mintues = (now - systemStartTime)/60000000000.0;
        LOG.info(String.format("\n\n%s %.2f min) %s %s\n", name, mintues,
            String.format(format, args), toMpsString(nBytes, now)));
      }
    }
    
    void end(final long nBytes) {
      final long now = System.nanoTime();
      final double seconds = (now - startTime)/1000000000.0;
      LOG.info(String.format("\n\n%s END: duration=%.2fs %s\n",
          name, seconds, toMpsString(nBytes, now)));
    }
    
    String toMpsString(final long nBytes, final long now) {
      final double mb = nBytes/(double)(1<<20);
      final double mps = mb*1000000000.0/(now - startTime);
      return String.format("[nBytes=%.2fMB, speed=%.2fMB/s]", mb, mps);
    }
  }

  @Test(timeout=300000)
  public void testLargeFile() throws Exception {
    largeFileTest(200L << 20); //200MB file length
  }

  /** Test read and write large files. */
  static void largeFileTest(final long fileLength) throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .build();
    try {
      cluster.waitActive();

      final FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);
      final Path dir = new Path("/test/largeFile");
      Assert.assertTrue(fs.mkdirs(dir));

      final byte[] data = new byte[1 << 20];
      RANDOM.nextBytes(data);

      final byte[] expected = new byte[2 * data.length];
      System.arraycopy(data, 0, expected, 0, data.length);
      System.arraycopy(data, 0, expected, data.length, data.length);

      final Path p = new Path(dir, "file");
      final Ticker t = new Ticker("WRITE", "fileLength=" + fileLength);
      final FSDataOutputStream out = fs.create(p);
      try {
        long remaining = fileLength;
        for(; remaining > 0;) {
          t.tick(fileLength - remaining, "remaining=%d", remaining);
          
          final int n = (int)Math.min(remaining, data.length);
          out.write(data, 0, n);
          remaining -= n;
        }
      } finally {
        out.close();
      }
      t.end(fileLength);
  
      Assert.assertEquals(fileLength, fs.getFileStatus(p).getLen());

      final long smallOffset = RANDOM.nextInt(1 << 20) + (1 << 20);
      final long largeOffset = fileLength - smallOffset;
      final byte[] buf = new byte[data.length];

      verifySeek(fs, p, largeOffset, fileLength, buf, expected);
      verifySeek(fs, p, smallOffset, fileLength, buf, expected);
  
      verifyPread(fs, p, largeOffset, fileLength, buf, expected);
    } finally {
      cluster.shutdown();
    }
  }

  static void checkData(long offset, long remaining, int n,
      byte[] actual, byte[] expected) {
    if (RANDOM.nextInt(100) == 0) {
      int j = (int)(offset % actual.length);
      for(int i = 0; i < n; i++) {
        if (expected[j] != actual[i]) {
          Assert.fail("expected[" + j + "]=" + expected[j]
              + " != actual[" + i + "]=" + actual[i]
              + ", offset=" + offset + ", remaining=" + remaining + ", n=" + n);
        }
        j++;
      }
    }
  }

  /** test seek */
  static void verifySeek(FileSystem fs, Path p, long offset, long length,
      byte[] buf, byte[] expected) throws IOException { 
    long remaining = length - offset;
    long checked = 0;
    LOG.info("XXX SEEK: offset=" + offset + ", remaining=" + remaining);

    final Ticker t = new Ticker("SEEK", "offset=%d, remaining=%d",
        offset, remaining);
    final FSDataInputStream in = fs.open(p, 64 << 10);
    in.seek(offset);
    for(; remaining > 0; ) {
      t.tick(checked, "offset=%d, remaining=%d", offset, remaining);
      final int n = (int)Math.min(remaining, buf.length);
      in.readFully(buf, 0, n);
      checkData(offset, remaining, n, buf, expected);

      offset += n;
      remaining -= n;
      checked += n;
    }
    in.close();
    t.end(checked);
  }

  static void verifyPread(FileSystem fs, Path p, long offset, long length,
      byte[] buf, byte[] expected) throws IOException {
    long remaining = length - offset;
    long checked = 0;
    LOG.info("XXX PREAD: offset=" + offset + ", remaining=" + remaining);

    final Ticker t = new Ticker("PREAD", "offset=%d, remaining=%d",
        offset, remaining);
    final FSDataInputStream in = fs.open(p, 64 << 10);
    for(; remaining > 0; ) {
      t.tick(checked, "offset=%d, remaining=%d", offset, remaining);
      final int n = (int)Math.min(remaining, buf.length);
      in.readFully(offset, buf, 0, n);
      checkData(offset, remaining, n, buf, expected);

      offset += n;
      remaining -= n;
      checked += n;
    }
    in.close();
    t.end(checked);
  }

  /** Test client retry with namenode restarting. */
  @Test(timeout=300000)
  public void testNamenodeRestart() throws Exception {
    GenericTestUtils.setLogLevel(NamenodeWebHdfsMethods.LOG, Level.ALL);
    final Configuration conf = WebHdfsTestUtil.createConf();
    TestDFSClientRetries.namenodeRestartTest(conf, true);
  }
  
  @Test(timeout=300000)
  public void testLargeDirectory() throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();
    final int listLimit = 2;
    // force small chunking of directory listing
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, listLimit);
    // force paths to be only owner-accessible to ensure ugi isn't changing
    // during listStatus
    FsPermission.setUMask(conf, new FsPermission((short)0077));
    
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      cluster.waitActive();
      WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME)
          .setPermission(new Path("/"),
              new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

      // trick the NN into not believing it's not the superuser so we can
      // tell if the correct user is used by listStatus
      UserGroupInformation.setLoginUser(
          UserGroupInformation.createUserForTesting(
              "not-superuser", new String[]{"not-supergroup"}));

      UserGroupInformation.createUserForTesting("me", new String[]{"my-group"})
        .doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws IOException, URISyntaxException {
              FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
                  WebHdfsConstants.WEBHDFS_SCHEME);
              Path d = new Path("/my-dir");
            Assert.assertTrue(fs.mkdirs(d));
            for (int i=0; i < listLimit*3; i++) {
              Path p = new Path(d, "file-"+i);
              Assert.assertTrue(fs.createNewFile(p));
            }
            Assert.assertEquals(listLimit*3, fs.listStatus(d).length);
            return null;
          }
        });
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=300000)
  public void testNumericalUserName() throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();
    conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY, "^[A-Za-z0-9_][A-Za-z0-9" +
        "._-]*[$]?$");
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      cluster.waitActive();
      WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME)
          .setPermission(new Path("/"),
              new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

      UserGroupInformation.createUserForTesting("123", new String[]{"my-group"})
        .doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws IOException, URISyntaxException {
            FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
                WebHdfsConstants.WEBHDFS_SCHEME);
            Path d = new Path("/my-dir");
            Assert.assertTrue(fs.mkdirs(d));
            return null;
          }
        });
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test for catching "no datanode" IOException, when to create a file
   * but datanode is not running for some reason.
   */
  @Test(timeout=300000)
  public void testCreateWithNoDN() throws Exception {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
      cluster.waitActive();
      FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      fs.create(new Path("/testnodatanode"));
      Assert.fail("No exception was thrown");
    } catch (IOException ex) {
      GenericTestUtils.assertExceptionContains("Failed to find datanode", ex);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test allow and disallow snapshot through WebHdfs. Verifying webhdfs with
   * Distributed filesystem methods.
   */
  @Test
  public void testWebHdfsAllowandDisallowSnapshots() throws Exception {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final WebHdfsFileSystem webHdfs = WebHdfsTestUtil
          .getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);

      final Path bar = new Path("/bar");
      dfs.mkdirs(bar);

      // allow snapshots on /bar using webhdfs
      webHdfs.allowSnapshot(bar);
      webHdfs.createSnapshot(bar, "s1");
      final Path s1path = SnapshotTestHelper.getSnapshotRoot(bar, "s1");
      Assert.assertTrue(webHdfs.exists(s1path));
      SnapshottableDirectoryStatus[] snapshottableDirs =
          dfs.getSnapshottableDirListing();
      assertEquals(1, snapshottableDirs.length);
      assertEquals(bar, snapshottableDirs[0].getFullPath());
      dfs.deleteSnapshot(bar, "s1");
      dfs.disallowSnapshot(bar);
      snapshottableDirs = dfs.getSnapshottableDirListing();
      assertNull(snapshottableDirs);

      // disallow snapshots on /bar using webhdfs
      dfs.allowSnapshot(bar);
      snapshottableDirs = dfs.getSnapshottableDirListing();
      assertEquals(1, snapshottableDirs.length);
      assertEquals(bar, snapshottableDirs[0].getFullPath());
      webHdfs.disallowSnapshot(bar);
      snapshottableDirs = dfs.getSnapshottableDirListing();
      assertNull(snapshottableDirs);
      try {
        webHdfs.createSnapshot(bar);
        fail("Cannot create snapshot on a non-snapshottable directory");
      } catch (Exception e) {
        GenericTestUtils.assertExceptionContains(
            "Directory is not a snapshottable directory", e);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test snapshot creation through WebHdfs
   */
  @Test
  public void testWebHdfsCreateSnapshot() throws Exception {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);

      final Path foo = new Path("/foo");
      dfs.mkdirs(foo);

      try {
        webHdfs.createSnapshot(foo);
        fail("Cannot create snapshot on a non-snapshottable directory");
      } catch (Exception e) {
        GenericTestUtils.assertExceptionContains(
            "Directory is not a snapshottable directory", e);
      }

      // allow snapshots on /foo
      dfs.allowSnapshot(foo);
      // create snapshots on foo using WebHdfs
      webHdfs.createSnapshot(foo, "s1");
      // create snapshot without specifying name
      final Path spath = webHdfs.createSnapshot(foo, null);

      Assert.assertTrue(webHdfs.exists(spath));
      final Path s1path = SnapshotTestHelper.getSnapshotRoot(foo, "s1");
      Assert.assertTrue(webHdfs.exists(s1path));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test snapshot deletion through WebHdfs
   */
  @Test
  public void testWebHdfsDeleteSnapshot() throws Exception {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);

      final Path foo = new Path("/foo");
      dfs.mkdirs(foo);
      dfs.allowSnapshot(foo);

      webHdfs.createSnapshot(foo, "s1");
      final Path spath = webHdfs.createSnapshot(foo, null);
      Assert.assertTrue(webHdfs.exists(spath));
      final Path s1path = SnapshotTestHelper.getSnapshotRoot(foo, "s1");
      Assert.assertTrue(webHdfs.exists(s1path));

      // delete the two snapshots
      webHdfs.deleteSnapshot(foo, "s1");
      Assert.assertFalse(webHdfs.exists(s1path));
      webHdfs.deleteSnapshot(foo, spath.getName());
      Assert.assertFalse(webHdfs.exists(spath));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testWebHdfsCreateNonRecursive() throws IOException, URISyntaxException {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    WebHdfsFileSystem webHdfs = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);

      TestFileCreation.testFileCreationNonRecursive(webHdfs);

    } finally {
      if(webHdfs != null) {
       webHdfs.close();
      }

      if(cluster != null) {
        cluster.shutdown();
      }
    }
  }
  /**
   * Test snapshot rename through WebHdfs
   */
  @Test
  public void testWebHdfsRenameSnapshot() throws Exception {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);

      final Path foo = new Path("/foo");
      dfs.mkdirs(foo);
      dfs.allowSnapshot(foo);

      webHdfs.createSnapshot(foo, "s1");
      final Path s1path = SnapshotTestHelper.getSnapshotRoot(foo, "s1");
      Assert.assertTrue(webHdfs.exists(s1path));

      // rename s1 to s2
      webHdfs.renameSnapshot(foo, "s1", "s2");
      Assert.assertFalse(webHdfs.exists(s1path));
      final Path s2path = SnapshotTestHelper.getSnapshotRoot(foo, "s2");
      Assert.assertTrue(webHdfs.exists(s2path));

      webHdfs.deleteSnapshot(foo, "s2");
      Assert.assertFalse(webHdfs.exists(s2path));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Make sure a RetriableException is thrown when rpcServer is null in
   * NamenodeWebHdfsMethods.
   */
  @Test
  public void testRaceWhileNNStartup() throws Exception {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      final NameNode namenode = cluster.getNameNode();
      final NamenodeProtocols rpcServer = namenode.getRpcServer();
      Whitebox.setInternalState(namenode, "rpcServer", null);

      final Path foo = new Path("/foo");
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      try {
        webHdfs.mkdirs(foo);
        fail("Expected RetriableException");
      } catch (RetriableException e) {
        GenericTestUtils.assertExceptionContains("Namenode is in startup mode",
            e);
      }
      Whitebox.setInternalState(namenode, "rpcServer", rpcServer);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDTInInsecureClusterWithFallback()
      throws IOException, URISyntaxException {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    conf.setBoolean(CommonConfigurationKeys
        .IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, true);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      Assert.assertNull(webHdfs.getDelegationToken(null));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDTInInsecureCluster() throws Exception {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      webHdfs.getDelegationToken(null);
      fail("No exception is thrown.");
    } catch (AccessControlException ace) {
      Assert.assertTrue(ace.getMessage().startsWith(
          WebHdfsFileSystem.CANT_FALLBACK_TO_INSECURE_MSG));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testWebHdfsOffsetAndLength() throws Exception{
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    final int OFFSET = 42;
    final int LENGTH = 512;
    final String PATH = "/foo";
    byte[] CONTENTS = new byte[1024];
    RANDOM.nextBytes(CONTENTS);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      final WebHdfsFileSystem fs =
          WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);
      try (OutputStream os = fs.create(new Path(PATH))) {
        os.write(CONTENTS);
      }
      InetSocketAddress addr = cluster.getNameNode().getHttpAddress();
      URL url = new URL("http", addr.getHostString(), addr
          .getPort(), WebHdfsFileSystem.PATH_PREFIX + PATH + "?op=OPEN" +
          Param.toSortedString("&", new OffsetParam((long) OFFSET),
                               new LengthParam((long) LENGTH))
      );
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setInstanceFollowRedirects(true);
      Assert.assertEquals(LENGTH, conn.getContentLength());
      byte[] subContents = new byte[LENGTH];
      byte[] realContents = new byte[LENGTH];
      System.arraycopy(CONTENTS, OFFSET, subContents, 0, LENGTH);
      IOUtils.readFully(conn.getInputStream(), realContents);
      Assert.assertArrayEquals(subContents, realContents);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testContentSummary() throws Exception {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    final Path path = new Path("/QuotaDir");
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      final WebHdfsFileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(
          conf, WebHdfsConstants.WEBHDFS_SCHEME);
      final DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(path);
      dfs.setQuotaByStorageType(path, StorageType.DISK, 100000);
      ContentSummary contentSummary = webHdfs.getContentSummary(path);
      Assert.assertTrue((contentSummary.getTypeQuota(
          StorageType.DISK) == 100000));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testWebHdfsPread() throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .build();
    byte[] content = new byte[1024];
    RANDOM.nextBytes(content);
    final Path foo = new Path("/foo");
    FSDataInputStream in = null;
    try {
      final WebHdfsFileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      try (OutputStream os = fs.create(foo)) {
        os.write(content);
      }

      // pread
      in = fs.open(foo, 1024);
      byte[] buf = new byte[1024];
      try {
        in.readFully(1020, buf, 0, 5);
        Assert.fail("EOF expected");
      } catch (EOFException ignored) {}

      // mix pread with stateful read
      int length = in.read(buf, 0, 512);
      in.readFully(100, new byte[1024], 0, 100);
      int preadLen = in.read(200, new byte[1024], 0, 200);
      Assert.assertTrue(preadLen > 0);
      IOUtils.readFully(in, buf, length, 1024 - length);
      Assert.assertArrayEquals(content, buf);
    } finally {
      if (in != null) {
        in.close();
      }
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testGetHomeDirectory() throws Exception {

    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();

      final URI uri = new URI(WebHdfsConstants.WEBHDFS_SCHEME + "://"
          + cluster.getHttpUri(0).replace("http://", ""));
      final Configuration confTemp = new Configuration();

      {
        WebHdfsFileSystem webhdfs = (WebHdfsFileSystem) FileSystem.get(uri,
            confTemp);

        assertEquals(hdfs.getHomeDirectory().toUri().getPath(), webhdfs
            .getHomeDirectory().toUri().getPath());

        webhdfs.close();
      }

      {
        WebHdfsFileSystem webhdfs = createWebHDFSAsTestUser(confTemp, uri,
            "XXX");

        assertNotEquals(hdfs.getHomeDirectory().toUri().getPath(), webhdfs
            .getHomeDirectory().toUri().getPath());

        webhdfs.close();
      }

    } finally {
      if (cluster != null)
        cluster.shutdown();
    }
  }

  @Test
  public void testWebHdfsGetBlockLocationsWithStorageType() throws Exception{
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    final int OFFSET = 42;
    final int LENGTH = 512;
    final Path PATH = new Path("/foo");
    byte[] CONTENTS = new byte[1024];
    RANDOM.nextBytes(CONTENTS);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      final WebHdfsFileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsConstants.WEBHDFS_SCHEME);
      try (OutputStream os = fs.create(PATH)) {
        os.write(CONTENTS);
      }
      BlockLocation[] locations = fs.getFileBlockLocations(PATH, OFFSET,
          LENGTH);
      for (BlockLocation location: locations) {
        StorageType[] storageTypes = location.getStorageTypes();
        Assert.assertTrue(storageTypes != null && storageTypes.length > 0 &&
            storageTypes[0] == StorageType.DISK);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private WebHdfsFileSystem createWebHDFSAsTestUser(final Configuration conf,
      final URI uri, final String userName) throws Exception {

    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        userName, new String[] { "supergroup" });

    return ugi.doAs(new PrivilegedExceptionAction<WebHdfsFileSystem>() {
      @Override
      public WebHdfsFileSystem run() throws IOException {
        WebHdfsFileSystem webhdfs = (WebHdfsFileSystem) FileSystem.get(uri,
            conf);
        return webhdfs;
      }
    });
  }

  @Test(timeout=90000)
  public void testWebHdfsReadRetries() throws Exception {
    // ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    final Configuration conf = WebHdfsTestUtil.createConf();
    final Path dir = new Path("/testWebHdfsReadRetries");

    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024*512);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);

    final short numDatanodes = 1;
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDatanodes)
        .build();
    try {
      cluster.waitActive();
      final FileSystem fs = WebHdfsTestUtil
                .getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);

      //create a file
      final long length = 1L << 20;
      final Path file1 = new Path(dir, "testFile");

      DFSTestUtil.createFile(fs, file1, length, numDatanodes, 20120406L);

      //get file status and check that it was written properly.
      final FileStatus s1 = fs.getFileStatus(file1);
      assertEquals("Write failed for file " + file1, length, s1.getLen());

      // Ensure file can be read through WebHdfsInputStream
      FSDataInputStream in = fs.open(file1);
      assertTrue("Input stream is not an instance of class WebHdfsInputStream",
          in.getWrappedStream() instanceof WebHdfsInputStream);
      int count = 0;
      for(; in.read() != -1; count++);
      assertEquals("Read failed for file " + file1, s1.getLen(), count);
      assertEquals("Sghould not be able to read beyond end of file",
          in.read(), -1);
      in.close();
      try {
        in.read();
        fail("Read after close should have failed");
      } catch(IOException ioe) { }

      WebHdfsFileSystem wfs = (WebHdfsFileSystem)fs;
      // Read should not be retried if AccessControlException is encountered.
      String msg = "ReadRetries: Test Access Control Exception";
      testReadRetryExceptionHelper(wfs, file1,
                          new AccessControlException(msg), msg, false, 1);

      // Retry policy should be invoked if IOExceptions are thrown.
      msg = "ReadRetries: Test SocketTimeoutException";
      testReadRetryExceptionHelper(wfs, file1,
                          new SocketTimeoutException(msg), msg, true, 5);
      msg = "ReadRetries: Test SocketException";
      testReadRetryExceptionHelper(wfs, file1,
                          new SocketException(msg), msg, true, 5);
      msg = "ReadRetries: Test EOFException";
      testReadRetryExceptionHelper(wfs, file1,
                          new EOFException(msg), msg, true, 5);
      msg = "ReadRetries: Test Generic IO Exception";
      testReadRetryExceptionHelper(wfs, file1,
                          new IOException(msg), msg, true, 5);

      // If InvalidToken exception occurs, WebHdfs only retries if the
      // delegation token was replaced. Do that twice, then verify by checking
      // the number of times it tried.
      WebHdfsFileSystem spyfs = spy(wfs);
      when(spyfs.replaceExpiredDelegationToken()).thenReturn(true, true, false);
      msg = "ReadRetries: Test Invalid Token Exception";
      testReadRetryExceptionHelper(spyfs, file1,
                          new InvalidToken(msg), msg, false, 3);
    } finally {
      cluster.shutdown();
    }
  }

  public boolean attemptedRetry;
  private void testReadRetryExceptionHelper(WebHdfsFileSystem fs, Path fn,
      final IOException ex, String msg, boolean shouldAttemptRetry,
      int numTimesTried)
      throws Exception {
    // Ovverride WebHdfsInputStream#getInputStream so that it returns
    // an input stream that throws the specified exception when read
    // is called.
    FSDataInputStream in = fs.open(fn);
    in.read(); // Connection is made only when the first read() occurs.
    final WebHdfsInputStream webIn =
        (WebHdfsInputStream)(in.getWrappedStream());

    final InputStream spyInputStream =
        spy(webIn.getReadRunner().getInputStream());
    doThrow(ex).when(spyInputStream).read((byte[])any(), anyInt(), anyInt());
    final WebHdfsFileSystem.ReadRunner rr = spy(webIn.getReadRunner());
    doReturn(spyInputStream)
        .when(rr).initializeInputStream((HttpURLConnection) any());
    rr.setInputStream(spyInputStream);
    webIn.setReadRunner(rr);

    // Override filesystem's retry policy in order to verify that
    // WebHdfsInputStream is calling shouldRetry for the appropriate
    // exceptions.
    final RetryAction retryAction = new RetryAction(RetryDecision.RETRY);
    final RetryAction failAction = new RetryAction(RetryDecision.FAIL);
    RetryPolicy rp = new RetryPolicy() {
      @Override
      public RetryAction shouldRetry(Exception e, int retries, int failovers,
          boolean isIdempotentOrAtMostOnce) throws Exception {
        attemptedRetry = true;
       if (retries > 3) {
          return failAction;
        } else {
          return retryAction;
        }
      }
    };
    fs.setRetryPolicy(rp);

    // If the retry logic is exercised, attemptedRetry will be true. Some
    // exceptions should exercise the retry logic and others should not.
    // Either way, the value of attemptedRetry should match shouldAttemptRetry.
    attemptedRetry = false;
    try {
      webIn.read();
      fail(msg + ": Read should have thrown exception.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(msg));
    }
    assertEquals(msg + ": Read should " + (shouldAttemptRetry ? "" : "not ")
                + "have called shouldRetry. ",
        attemptedRetry, shouldAttemptRetry);

    verify(rr, times(numTimesTried)).getResponse((HttpURLConnection) any());
    webIn.close();
    in.close();
  }
}
