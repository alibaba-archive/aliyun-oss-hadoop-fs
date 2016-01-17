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


import com.aliyun.oss.ClientConfiguration;

/**
 * SmartOSSClient Configuration, also include more multiple part upload/copy configuration items.
 * When initialize OSS FileSystem and SmartOSSClient, read these configuration items from Hadoop configuration.
 */
public class SmartOSSClientConfig extends ClientConfiguration {


  //The maximum allowed parts in a multipart upload.
  protected static final int MAXIMUM_UPLOAD_PARTS = 10000;
  // number of records to get while paging through a directory listing, from ListObjectsRequest
  protected static final int MAX_RETURNED_KEYS_LIMIT = 1000;
  // The maximum number of entries that can be deleted in any call to oss
  protected static final int DELETE_OBJECTS_ONETIME_LIMIT = 1000;
  /**
   * Constant values
   */
  protected static final String FS_OSS = "oss";
  protected static final int KB = 1024;
  protected static final int MB = 1048576;
  protected static final long GB = 1073741824L;


  /**
   * OSS File System Fundamental Configuration
   */
  //oss endpoint, use a custom endpoint
  protected static final String HADOOP_ENDPOINT = "fs.oss.endpoint";
  // oss secret key
  protected static final String HADOOP_SECRET_KEY = "fs.oss.secret.key";
  // oss access key
  protected static final String HADOOPACCESS_KEY = "fs.oss.access.key";

  /**
   * OSS Client Configuration, please refer to help.aliyun.com/document_detail/oss/sdk/java-sdk/init.html
   */
  protected static final String HADOOP_USER_AGENT = "fs.oss.clientconfig.useragent";
  protected static final String DEFAULT_USER_AGENT = "aliyun-sdk-java";
  //Proxy, connect to oss through a proxy server
  protected static final String HADOOP_PROXY_HOST = "fs.oss.clientconfig.proxy.host";
  protected static final String HADOOP_PROXY_PORT = "fs.oss.clientconfig.proxy.port";
  protected static final String HADOOP_PROXY_PASSWORD = "fs.oss.clientconfig.proxy.password";
  protected static final String HADOOP_PROXY_USERNAME = "fs.oss.clientconfig.proxy.username";
  protected static final String HADOOP_PROXY_WORKSTATION = "fs.oss.clientconfig.proxy.workstation";
  protected static final String HADOOP_PROXY_DOMAIN = "fs.oss.clientconfig.proxy.domain";
  //MaxConnections
  protected static final String HADOOP_MAXIMUM_CONNECTIONS = "fs.oss.clientconfig.connection.maximum";
  protected static final int DEFAULT_MAXIMUM_CONNECTIONS = 1024;
  //SocketTimeout
  protected static final String HADOOP_SOCKET_TIMEOUT = "fs.oss.clientconfig.connection.timeout";
  protected static final int DEFAULT_SOCKET_TIMEOUT = 50000;
  //ConnectionTimeout
  protected static final String HADOOP_ESTABLISH_TIMEOUT = "fs.oss.clientconfig.connection.establish.timeout";
  protected static final int DEFAULT_ESTABLISH_TIMEOUT = 50000;
  //MaxErrorRetry
  protected static final String HADOOP_MAX_ERROR_RETRIES = "fs.oss.clientconfig.attempts.maximum";
  protected static final int DEFAULT_MAX_ERROR_RETRIES = 3;
  //Protocol
  protected static final String HADOOP_SECURE_CONNECTIONS = "fs.oss.clientconfig.ssl.enabled";
  protected static final boolean DEFAULT_SECURE_CONNECTIONS = false;


  /**
   * extra configuration for multiple part copy/upload
   */
  // minimum size in bytes before we start a multipart uploads or copy
  protected static final String HADOOP_MULTIPART_UPLOAD_THRESHOLD = "fs.oss.extra.multipart.upload.threshold";
  //Default size threshold for when to use multipart uploads.
  protected static final long DEFAULT_MULTIPART_UPLOAD_THRESHOLD = 100 * MB;
  protected static final String HADOOP_MULTIPART_UPLOAD_PART_SIZE = "fs.oss.extra.multipart.upload.partsize";
  //Default minimum part size for upload parts.
  protected static final int DEFAULT_MINIMUM_UPLOAD_PART_SIZE = 10 * MB;
  // minimum size in bytes before we start a multipart uploads or copy
  protected static final String HADOOP_MULTIPART_COPY_THRESHOLD = "fs.oss.extra.multipart.copy.threshold";
  //Default size threshold for OSS object after which multi-part copy is initiated.
  protected static final long DEFAULT_MULTIPART_COPY_THRESHOLD = 1 * GB;
  protected static final String HADOOP_MULTIPART_COPY_PART_SIZE = "fs.oss.extra.multipart.copy.partsize";
  //Default minimum size of each part for multi-part copy.
  protected static final long DEFAULT_MINIMUM_COPY_PART_SIZE = 100 * MB;

  /**
   * extra configuration for multiple part copy/upload, Thread Pool,
   */
  // the maximum number of tasks cached if all threads are already uploading
  protected static final String HADOOP_CORE_POOL_SIZE = "fs.oss.threads.coresize";
  protected static final int DEFAULT_CORE_POOL_SIZE = 5;
  // the time an idle thread waits before terminating
  protected static final String HADOOP_KEEP_ALIVE_TIME = "fs.oss.threads.keepalivetime";
  protected static final int DEFAULT_KEEP_ALIVE_TIME = 60;
  // the maximum number of threads to allow in the pool used by SmartOSSClient
  protected static final String HADOOP_MAX_POOL_SIZE = "fs.oss.threads.maxsize";
  protected static final int DEFAULT_MAX_POOL_SIZE = 10;


  /**
   * Seeded configuration items
   */
  protected static final String BUFFER_DIR = "fs.oss.buffer.dir";


  /**
   * Default block size as used in block size and FS status queries.
   */
  public static final int OSS_DEFAULT_BLOCK_SIZE = 32 * MB;


  private long minimumUploadPartSize = DEFAULT_MINIMUM_UPLOAD_PART_SIZE;
  private long multipartUploadThreshold = DEFAULT_MULTIPART_UPLOAD_THRESHOLD;
  private long multipartCopyThreshold = DEFAULT_MULTIPART_COPY_THRESHOLD;
  private long multipartCopyPartSize = DEFAULT_MINIMUM_COPY_PART_SIZE;
  private int corePoolSize = DEFAULT_CORE_POOL_SIZE;
  private int keepAliveTime = DEFAULT_KEEP_ALIVE_TIME;
  private int maxPoolSize = DEFAULT_MAX_POOL_SIZE;


  public long getMinimumUploadPartSize() {
    return minimumUploadPartSize;
  }

  public void setMinimumUploadPartSize(long minimumUploadPartSize) {
    this.minimumUploadPartSize = minimumUploadPartSize;
  }

  public long getMultipartUploadThreshold() {
    return multipartUploadThreshold;
  }

  public void setMultipartUploadThreshold(long multipartUploadThreshold) {
    this.multipartUploadThreshold = multipartUploadThreshold;
  }

  public long getMultipartCopyPartSize() {

    return multipartCopyPartSize;
  }

  public void setMultipartCopyPartSize(long multipartCopyPartSize) {

    this.multipartCopyPartSize = multipartCopyPartSize;
  }

  public long getMultipartCopyThreshold() {
    return multipartCopyThreshold;
  }

  public int getCorePoolSize() {
    return corePoolSize;
  }

  public void setCorePoolSize(int corePoolSize) {
    this.corePoolSize = corePoolSize;
  }

  public int getKeepAliveTime() {
    return keepAliveTime;
  }

  public void setKeepAliveTime(int keepAliveTime) {
    this.keepAliveTime = keepAliveTime;
  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public void setMaxPoolSize(int maxPoolSize) {
    this.maxPoolSize = maxPoolSize;
  }

  public void setMultipartCopyThreshold(long multipartCopyThreshold) {

    this.multipartCopyThreshold = multipartCopyThreshold;
  }

}
