/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.retry.MultiException;

/**
 * A FailoverProxyProvider implementation that technically does not "failover"
 * per-se. It constructs a wrapper proxy that sends the request to ALL
 * underlying proxies simultaneously. It assumes the in an HA setup, there will
 * be only one Active, and the active should respond faster than any configured
 * standbys. Once it recieve a response from any one of the configred proxies,
 * outstanding requests to other proxies are immediately cancelled.
 */
public class RequestHedgingProxyProvider<T> extends
        ConfiguredFailoverProxyProvider<T> {

  private static final Log LOG =
          LogFactory.getLog(RequestHedgingProxyProvider.class);

  class RequestHedgingInvocationHandler implements InvocationHandler {

    final Map<String, ProxyInfo<T>> targetProxies;

    public RequestHedgingInvocationHandler(
            Map<String, ProxyInfo<T>> targetProxies) {
      this.targetProxies = new HashMap<>(targetProxies);
    }

    /**
     * Creates a Executor and invokes all proxies concurrently. This
     * implementation assumes that Clients have configured proper socket
     * timeouts, else the call can block forever.
     *
     * @param proxy
     * @param method
     * @param args
     * @return
     * @throws Throwable
     */
    @Override
    public Object
    invoke(Object proxy, final Method method, final Object[] args)
            throws Throwable {
      Map<Future<Object>, ProxyInfo<T>> proxyMap = new HashMap<>();
      int numAttempts = 0;

      ExecutorService executor = null;
      CompletionService<Object> completionService;
      try {
        // Optimization : if only 2 proxies are configured and one had failed
        // over, then we dont need to create a threadpool etc.
        targetProxies.remove(toIgnore);
        if (targetProxies.size() == 1) {
          ProxyInfo<T> proxyInfo = targetProxies.values().iterator().next();
          Object retVal = method.invoke(proxyInfo.proxy, args);
          successfulProxy = proxyInfo;
          return retVal;
        }
        executor = Executors.newFixedThreadPool(proxies.size());
        completionService = new ExecutorCompletionService<>(executor);
        for (final Map.Entry<String, ProxyInfo<T>> pEntry :
                targetProxies.entrySet()) {
          Callable<Object> c = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              return method.invoke(pEntry.getValue().proxy, args);
            }
          };
          proxyMap.put(completionService.submit(c), pEntry.getValue());
          numAttempts++;
        }

        Map<String, Exception> badResults = new HashMap<>();
        while (numAttempts > 0) {
          Future<Object> callResultFuture = completionService.take();
          Object retVal;
          try {
            retVal = callResultFuture.get();
            successfulProxy = proxyMap.get(callResultFuture);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Invocation successful on ["
                      + successfulProxy.proxyInfo + "]");
            }
            return retVal;
          } catch (Exception ex) {
            ProxyInfo<T> tProxyInfo = proxyMap.get(callResultFuture);
            LOG.warn("Invocation returned exception on "
                    + "[" + tProxyInfo.proxyInfo + "]");
            badResults.put(tProxyInfo.proxyInfo, ex);
            numAttempts--;
          }
        }

        // At this point we should have All bad results (Exceptions)
        // Or should have returned with successful result.
        if (badResults.size() == 1) {
          throw badResults.values().iterator().next();
        } else {
          throw new MultiException(badResults);
        }
      } finally {
        if (executor != null) {
          executor.shutdownNow();
        }
      }
    }
  }


  private volatile ProxyInfo<T> successfulProxy = null;
  private volatile String toIgnore = null;

  public RequestHedgingProxyProvider(
          Configuration conf, URI uri, Class<T> xface) {
    this(conf, uri, xface, new DefaultProxyFactory<T>());
  }

  @VisibleForTesting
  RequestHedgingProxyProvider(Configuration conf, URI uri,
                              Class<T> xface, ProxyFactory<T> factory) {
    super(conf, uri, xface, factory);
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    if (successfulProxy != null) {
      return successfulProxy;
    }
    Map<String, ProxyInfo<T>> targetProxyInfos = new HashMap<>();
    StringBuilder combinedInfo = new StringBuilder('[');
    for (int i = 0; i < proxies.size(); i++) {
      ProxyInfo<T> pInfo = super.getProxy();
      incrementProxyIndex();
      targetProxyInfos.put(pInfo.proxyInfo, pInfo);
      combinedInfo.append(pInfo.proxyInfo).append(',');
    }
    combinedInfo.append(']');
    T wrappedProxy = (T) Proxy.newProxyInstance(
            RequestHedgingInvocationHandler.class.getClassLoader(),
            new Class<?>[]{xface},
            new RequestHedgingInvocationHandler(targetProxyInfos));
    return new ProxyInfo<T>(wrappedProxy, combinedInfo.toString());
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    toIgnore = successfulProxy.proxyInfo;
    successfulProxy = null;
  }

}
