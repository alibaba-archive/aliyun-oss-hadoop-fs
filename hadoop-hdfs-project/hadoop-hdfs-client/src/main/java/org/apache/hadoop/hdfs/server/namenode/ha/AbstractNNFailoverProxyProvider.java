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

package org.apache.hadoop.hdfs.server.namenode.ha;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.io.retry.FailoverProxyProvider;

public abstract class AbstractNNFailoverProxyProvider<T> implements
    FailoverProxyProvider <T> {

  private AtomicBoolean fallbackToSimpleAuth;

  /**
   * Inquire whether logical HA URI is used for the implementation. If it is
   * used, a special token handling may be needed to make sure a token acquired
   * from a node in the HA pair can be used against the other node.
   *
   * @return true if logical HA URI is used. false, if not used.
   */
  public abstract boolean useLogicalURI();

  /**
   * Set for tracking if a secure client falls back to simple auth.  This method
   * is synchronized only to stifle a Findbugs warning.
   *
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   */
  public synchronized void setFallbackToSimpleAuth(
      AtomicBoolean fallbackToSimpleAuth) {
    this.fallbackToSimpleAuth = fallbackToSimpleAuth;
  }

  public synchronized AtomicBoolean getFallbackToSimpleAuth() {
    return fallbackToSimpleAuth;
  }
}
