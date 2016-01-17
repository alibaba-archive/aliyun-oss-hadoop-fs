/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.oss;


import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectRequest;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;

import static org.apache.hadoop.fs.oss.SmartOSSClientConfig.*;


/**
 * Most of cases are covered by Hadoop File System contract test, here are some extra test cases mainly for high performance APIs and code coverage.
 */
public class ExtraOSSAPIsTest extends TestCase {

  private OSSClient client;
  private OSSFileSystem fileSystem;
  private String bucketName;
  public static final String TEST_FS_OSS_NAME = "test.fs.oss.name";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    Configuration conf = new Configuration();
    String accessKeyId = conf.get(SmartOSSClientConfig.HADOOPACCESS_KEY, null);
    String accessKeySecret = conf.get(SmartOSSClientConfig.HADOOP_SECRET_KEY, null);
    String endpoint = conf.get(SmartOSSClientConfig.HADOOP_ENDPOINT, null);
    bucketName = conf.get(TEST_FS_OSS_NAME);
    SmartOSSClientConfig ossConf = new SmartOSSClientConfig();
    ossConf.setMultipartUploadThreshold(50 * MB);
    ossConf.setMinimumUploadPartSize(10 * MB);
    ossConf.setMultipartCopyThreshold(50 * MB);
    ossConf.setMultipartCopyPartSize(10 * MB);
    client = new SmartOSSClient(endpoint, accessKeyId, accessKeySecret, ossConf);

    fileSystem = new OSSFileSystem();
    fileSystem.initialize(URI.create(bucketName), conf);

  }

  /**
   * Test high performance copy and upload.
   *
   * @throws Exception
   */
  public void testMultiPartCopyUpload() throws Exception {
    final File sampleFile = createSampleFile(1100000); //48.83 MB
    long fileLength = sampleFile.length();

    PutObjectRequest putObjectRequest = new PutObjectRequest("hadoop-intg", fileSystem.getWorkingDirectory() + "/test-multipart-upload", sampleFile);
    client.putObject(putObjectRequest);
    String originMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(sampleFile));
    System.out.println("[uploadMd5] " + originMD5);

    File uploadedFile = new File("/tmp/test-multipart-upload");
    uploadedFile.deleteOnExit();
    client.getObject(new GetObjectRequest("hadoop-intg", fileSystem.getWorkingDirectory() + "/test-multipart-upload"), uploadedFile);
    String uploadMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(uploadedFile));
    System.out.println("[downloadMd5] " + uploadMD5);
    assertEquals(originMD5, uploadMD5);


    CopyObjectRequest copyObjectRequest = new CopyObjectRequest("hadoop-intg", fileSystem.getWorkingDirectory() + "/test-multipart-upload",
            "hadoop-intg", "test-multipart-copy");
    client.copyObject(copyObjectRequest);

    File copiedFile = new File("/tmp/test-multipart-copy");
    copiedFile.deleteOnExit();
    client.getObject(new GetObjectRequest("hadoop-intg", "test-multipart-copy"), copiedFile);
    String copiedMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(copiedFile));
    System.out.println("[copiedMd5] " + copiedMD5);
    assertEquals(originMD5, copiedMD5);
  }


  /**
   * InputStream code coverage test.
   *
   * @throws Exception
   */
  public void testOSSInputStream() throws Exception {

    final InputStream nullStream = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };
    client.putObject("hadoop-intg", "test/test-oss-inputstream", nullStream);
    FSDataInputStream inputStream = fileSystem.open(path("/test/test-oss-inputstream"), 100);

    assertEquals(inputStream.available(), 0);
    assertEquals(inputStream.markSupported(), false);
  }

  /**
   * Test high performance copyFromLocal.
   *
   * @throws Exception
   */
  public void testCopyFromLocalFile() throws Exception {
    final File sampleFile = createSampleFile(1100); //48.83 MB
    fileSystem.copyFromLocalFile(false, true, new Path(sampleFile.getAbsolutePath()), path("test/test-copyFromLocalFile"));

    ObjectMetadata metaData = client.getObjectMetadata("hadoop-intg", "user/shawguo/test/test-copyFromLocalFile");
    assertEquals(sampleFile.length(), metaData.getContentLength());
  }


  /**
   * test list/delete objects more than 1000
   *
   * @throws Exception
   */
  public void testObjectPagination() throws Exception {

    final InputStream nullStream = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };
    client.putObject("hadoop-intg", "test/", nullStream);
    for (int i = 0; i < 1010; i++) {
      System.out.println("putObject:" + "test/" + i);
      client.putObject("hadoop-intg", "test/" + i, nullStream);
    }
    assertEquals(1010, fileSystem.listStatus(path("/test")).length);
    fileSystem.delete(path("/test"), true);

  }


  @Override
  protected void tearDown() throws Exception {

    if (fileSystem != null) {
      fileSystem.delete(path("test"), true);
    }
    super.tearDown();
    client.shutdown();
    fileSystem.close();
    super.tearDown();
  }

  private File createSampleFile(int size) throws IOException {
    File file = File.createTempFile("oss-java-sdk-", ".txt");
    file.deleteOnExit();

    Writer writer = new OutputStreamWriter(new FileOutputStream(file));
    //total 50 char
    for (int i = 0; i < size; i++) {
      writer.write("abcdefghijklmnopqrstuvwxyz\n");
      writer.write("0123456789011234567890\n");
    }
    writer.close();

    return file;
  }

  protected Path path(String pathString) {
    return (new Path(pathString)).makeQualified(this.fileSystem);
  }
}