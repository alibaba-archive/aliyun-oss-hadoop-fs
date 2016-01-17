# Build and Install
```
git clone https://github.com/aliyun-beta/aliyun-oss-hadoop-fs.git
cd  hadoop-tools/hadoop-aliyun-oss
mvn clean package -Dmaven.test.skip=true
NOTE: if download maven dependencies failed, it may caused by GFW. Please make sure you have VPN access.
```

# How to run OSS file system contract test
Include the following properties in src/test/resources/oss-auth-key.xml
```
<property>
        <name>test.fs.oss.name</name>
        <value>oss://***bucket name***/</value>
    </property>
    <property>
        <name>fs.oss.endpoint</name>
        <value>***oss endpoint, for example oss-cn-shanghai.aliyuncs.com***</value>
    </property>
    <property>
        <name>fs.oss.access.key</name>
        <value>***your access key ***</value>
    </property>

    <property>
        <name>fs.oss.secret.key</name>
        <value>***your secret key***</value>
</property>
```
Include the following properties in src/test/resources/contract-test-options.xml.  
```
<property>
        <name>fs.contract.test.fs.oss</name>
        <value>oss://***your bucket name***/</value>
</property>
```
Run maven filesystem contract test
```
cd  hadoop-tools/hadoop-aliyun-oss
mvn test
The expected Results as below:
Tests run: 90, Failures: 0, Errors: 0, Skipped: 3
```


# How to access Aliyun OSS filesystem from HDFS Shell?    
1, Download latest hadoop(hadoop-2.6.3) from http://hadoop.apache.org/releases.html and extract tar file to ```hadoop-2.6.3``` directory;(tar zxvf hadoop-2.6.3.tar.gz)      

2, Download aliyun oss java SDK and extract to ```hadoop-2.6.3/aliyun-oss-lib``` directory;  

```
ls hadoop-2.6.3/aliyun-oss-lib
aliyun-sdk-oss-2.1.0.jar
commons-beanutils-1.8.0.jar
commons-codec-1.9.jar
commons-collections-3.2.1.jar
commons-lang-2.5.jar
commons-logging-1.2.jar
ezmorph-1.0.6.jar
hadoop-aliyun-oss-3.0.0-SNAPSHOT.jar
hamcrest-core-1.1.jar
httpclient-4.4.jar
httpcore-4.4.jar
jdom-1.1.jar
json-lib-2.4-jdk15.jar
junit-4.10.jar
```

3, In hadoop-aliyun-oss maven project directory, run "mvn package -DskipTests" and copy generated jar ```target/hadoop-aliyun-oss-3.0.0-SNAPSHOT.jar``` to ```hadoop-2.6.3/aliyun-oss-lib``` directory;     

4, Export hadoop environment variables:
   ```
export HADOOP_CLASSPATH=/your-local-path/hadoop-2.6.3/aliyun-oss-lib/*   
export HADOOP_USER_CLASSPATH_FIRST=true
```        
   In ```haddop-2.6.3``` directory, run "bin/hadoop classpath" and make sure aliyun-oss related jars in the beginning of the output classpath.  
for example:
```  
bin/hadoop classpath
/opt/Development/middleware/bigdata/hadoop-2.6.3/aliyun-oss-lib/*:/opt/Development/middleware/bigdata/hadoop-2.6.3/share/hadoop/common/lib/*:/opt/Development/middleware/bigdata/hadoop-2.6.3/share/hadoop/common/*:/opt/Development/middleware/bigdata/hadoop-2.6.3/share/hadoop/hdfs:/opt/Development/middleware/bigdata/hadoop-2.6.3/share/hadoop/hdfs/lib/*:/opt/Development/middleware/bigdata/hadoop-2.6.3/share/hadoop/hdfs/*:/opt/Development/middleware/bigdata/hadoop-2.6.3/share/hadoop/yarn/lib/*:/opt/Development/middleware/bigdata/hadoop-2.6.3/share/hadoop/yarn/*:/usr/lib/hadoop-mapreduce//share/hadoop/mapreduce/*
```
   
5, Add below properties to ```hadoop-2.6.3/etc/hadoop/core-site.xml```        
```
<property>     
      <name>fs.oss.endpoint</name>      
      <value>for example, oss-cn-shanghai.aliyuncs.com</value>        
</property>        
<property>       
       <name>fs.oss.access.key</name>        
       <value>*** replace with your access key ***</value>        
</property>        
<property>         
        <name>fs.oss.secret.key</name>        
        <value>***replace with your secret key ***</value>        
</property>        
	<property>        
        <name>fs.oss.buffer.dir</name>       
        <value>${hadoop.tmp.dir}/oss</value>        
        <description>Determines where on the local filesystem the OSS filesystem
            should store files before sending them to OSS
            (or after retrieving them from OSS).
        </description>        
</property> 
```   
6, **hadoop-2.6.3/bin/hadoop fs -ls** oss://***your bucket-name***/    
Will list your files in oss root directory, for example:
```
hadoop-2.6.3$ bin/hadoop fs -ls oss://hadoop-intg/
Found 1 items
-rw-rw-rw-   1          0 2016-01-08 18:23 oss://hadoop-intg/unity_support_test.1
 
```

# MapReduce program that reads input files from Aliyun OSS and writes output to Aliyun OSS  
Passed the test in cluster & production environment using **WordCount** MapReduce program.    
1, Add aliyun related jars to hadoop classpath;  
```
export HADOOP_CLASSPATH=/your-local-path/hadoop-2.6.3/aliyun-oss-lib/*
```
2, Add OSS Credentials into ```hadoop-2.6.3/etc/hadoop/core-site.xml``` (Just like above exmpale) or specifiy them in your MapReduce program(Below are my credentials for testing purpose):  
```
conf.set("fs.oss.endpoint","oss-cn-shanghai.aliyuncs.com");
conf.set("fs.oss.access.key","kqKNPIB4tfzF2eHS");
conf.set("fs.oss.secret.key","FolVVOzv2mZ1W0f2gYcWA4ija0OnJe");
conf.set("fs.oss.buffer.dir","fs.oss.buffer.dir");
conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
```
**Note: MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST must be true to avoid http client related jar conflicts**  
3, Specifiy OSS input and output in the program or pass it in the program argument;
For example:
```
FileInputFormat.addInputPath(job, new Path("oss://hadoop-intg/input/sales1.dat"));
FileOutputFormat.setOutputPath(job, new Path("oss://hadoop-intg/output/salesOutput_" +outputSuffix));
```
4, MapReduce Program end successfully.  
```
INFO mapreduce.Job: Job job_local1065339572_0001 completed successfully
INFO mapreduce.Job: Counters: 38
	File System Counters
		FILE: Number of bytes read=5476
		FILE: Number of bytes written=525094
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		OSS: Number of bytes read=9043814
		OSS: Number of bytes written=0
		OSS: Number of read operations=39
		OSS: Number of large read operations=0
		OSS: Number of write operations=6
	Map-Reduce Framework
		Map input records=100168
		Map output records=100168
		Map output bytes=977651
		Map output materialized bytes=2566
		Input split bytes=99
		Combine input records=100168
		Combine output records=217
		Reduce input groups=217
		Reduce shuffle bytes=2566
		Reduce input records=217
		Reduce output records=217
		Spilled Records=434
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=23
		CPU time spent (ms)=0
		Physical memory (bytes) snapshot=0
		Virtual memory (bytes) snapshot=0
		Total committed heap usage (bytes)=483393536
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=4521907
	File Output Format Counters 
		Bytes Written=0
```


# Advanced Usage

Please refer to SmartOSSClientConfig.java for a list of all hadoop configuration properties.  
```
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
```



# Hadoop File System Contract Test Status
**Hadoop File System Contract Test are all passed (Equivalent to AWS S3A File System)**  
```
NOTE:
testOverwriteNonEmptyDirectory    skipped   
testCreatedFileIsImmediatelyVisible  skipped    
testOverwriteEmptyDirectory   skipped   
Above three tests are skipped, both AWS S3A and Aliyun OSS does not support these tests. 
```

![alt text](https://raw.githubusercontent.com/aliyun-beta/aliyun-oss-hadoop-fs/master/hadoop-tools/hadoop-aliyun-oss/src/site/resources/TCs.JPG "Title")
```
TestOSSContractDelete
	testDeleteNonEmptyDirNonRecursive 	
	testDeleteDeepEmptyDir 	
	testDeleteSingleFile 	
	testDeleteNonEmptyDirRecursive 	
	testDeleteNonexistentPathRecursive 	
	testDeleteNonexistentPathNonRecursive 	
	testDeleteEmptyDirRecursive 	
	testDeleteEmptyDirNonRecursive 	
TestOSSContractMkdir
	testMkdirSlashHandling 	
	testMkdirOverParentFile 	
	testMkDirRmDir 	
	testNoMkdirOverFile 	
	testMkDirRmRfDir 	
TestOSSContractCreate
	testOverwriteNonEmptyDirectory    skipped 	
	testCreatedFileIsImmediatelyVisible  skipped 	
	testOverwriteEmptyDirectory   skipped 	
	testCreateFileOverExistingFileNoOverwrite 	
	testOverwriteExistingFile 	
	testCreateNewFile 	
TestOSSContractOpen
	testOpenReadZeroByteFile 	
	testFsIsEncrypted 	
	testOpenReadDir 	
	testOpenFileTwice 	
	testSequentialRead 	
	testOpenReadDirWithChild 	
TestOSSContractRootDir
	testRmEmptyRootDirNonRecursive 	
	testRmRootRecursive 	
	testListEmptyRootDirectory 	
	testCreateFileOverRoot 	
	testMkDirDepth1 	
	testRmNonEmptyRootDirNonRecursive 	
TestOSSFileSystemContract
	testMkdirsWithUmask 	
	testRenameFileAsExistingFile 	
	testRenameDirectoryAsExistingDirectory 	
	testMoveDirUnderParent 	
	testWorkingDirectory 	
	testMultiByteFilesAreFiles 	
	testDeleteEmptyDirectory 	
	testInputStreamClosedTwice 	
	testRenameFileMoveToNonExistentDirectory 	
	testWriteReadAndDeleteOneBlock 	
	testGetFileStatusThrowsExceptionForNonExistentFile 	
	testMkdirsFailsForSubdirectoryOfExistingFile 	
	testOverWriteAndRead 	
	testRenameRootDirForbidden 	
	testListStatusRootDir 	
	testRenameDirectoryAsExistingFile 	
	testMoveFileUnderParent 	
	testWriteReadAndDeleteEmptyFile 	
	testRenameDirectoryMoveToExistingDirectory 	
	testFsStatus 	
	testListStatus 	
	testFilesystemIsCaseSensitive 	
	testRenameDirectoryMoveToNonExistentDirectory 	
	testWriteReadAndDeleteHalfABlock 	
	testRenameToDirWithSamePrefixAllowed 	
	testRenameFileAsExistingDirectory 	
	testListStatusThrowsExceptionForNonExistentFile 	
	testWriteReadAndDeleteOneAndAHalfBlocks 	
	testOverwrite 	
	testRenameFileToSelf 	
	testWriteReadAndDeleteTwoBlocks 	
	testLSRootDir 	
	testRootDirAlwaysExists 	
	testRenameChildDirForbidden 	
	testWriteInNonExistentDirectory 	
	testRenameFileMoveToExistingDirectory 	
	testZeroByteFilesAreFiles 	
	testDeleteRecursively 	
	testMkdirs 	
	testDeleteNonExistentFile 	
	testRenameNonExistentPath 	
	testRenameDirToSelf 	
	testOutputStreamClosedTwice 	
TestOSSContractRename
	testRenameWithNonEmptySubDir 	
	testRenameFileOverExistingFile 	
	testRenameFileNonexistentDir 	
	testRenameNewFileSameDir 	
	testRenameNonexistentFile 	
	testRenameDirIntoExistingDir 	
TestOSSContractSeek
	testRandomSeeks 	
	testSeekAndReadPastEndOfFile 	
	testSeekReadClosedFile 	
	testBlockReadZeroByteFile 	
	testSeekPastEndOfFileThenReseekAndRead 	
	testNegativeSeek 	
	testPositionedBulkReadDoesntChangePosition 	
	testSeekBigFile 	
	testSeekFile 	
	testSeekZeroByteFile 	
```



# Code Coverage Status  
```Note: The real code coverage is higher that below diagram, below results are not accurate```    

![alt text](https://raw.githubusercontent.com/aliyun-beta/aliyun-oss-hadoop-fs/master/hadoop-tools/hadoop-aliyun-oss/src/site/resources/cc_20151229.JPG "Title")

# License
Licensed under the Apache License 2.0  
