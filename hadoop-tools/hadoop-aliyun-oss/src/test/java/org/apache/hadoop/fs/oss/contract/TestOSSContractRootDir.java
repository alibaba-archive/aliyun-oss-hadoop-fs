/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.oss.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * root dir operations against an oss bucket
 */

/**
 * TODO OSS have no proper representation for root dir
 * java.io.FileNotFoundException: No such file or directory: /
 * at org.apache.hadoop.fs.oss.OSSFileSystem.getFileStatus(OSSFileSystem.java:838)
 * at org.apache.hadoop.fs.oss.OSSFileSystem.listStatus(OSSFileSystem.java:591)
 * at org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest.testListEmptyRootDirectory(AbstractContractRootDirectoryTest.java:134)
 * at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
 * at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
 * at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
 * at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:47)
 * at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
 * at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:44)
 * at org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)
 * at org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:26)
 * at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:27)
 * at org.junit.internal.runners.statements.FailOnTimeout$StatementThread.run(FailOnTimeout.java:74)
 */
public class TestOSSContractRootDir extends
        AbstractContractRootDirectoryTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new OSSContract(conf);
  }
}
