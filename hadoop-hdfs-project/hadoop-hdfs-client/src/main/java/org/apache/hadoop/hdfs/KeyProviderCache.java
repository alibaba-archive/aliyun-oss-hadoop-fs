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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

@InterfaceAudience.Private
public class KeyProviderCache {

  public static final Logger LOG = LoggerFactory.getLogger(
      KeyProviderCache.class);

  private final Cache<URI, KeyProvider> cache;

  public KeyProviderCache(long expiryMs) {
    cache = CacheBuilder.newBuilder()
        .expireAfterAccess(expiryMs, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<URI, KeyProvider>() {
          @Override
          public void onRemoval(
              @Nonnull RemovalNotification<URI, KeyProvider> notification) {
            try {
              assert notification.getValue() != null;
              notification.getValue().close();
            } catch (Throwable e) {
              LOG.error(
                  "Error closing KeyProvider with uri ["
                      + notification.getKey() + "]", e);
            }
          }
        })
        .build();
  }

  public KeyProvider get(final Configuration conf) {
    URI kpURI = createKeyProviderURI(conf);
    if (kpURI == null) {
      return null;
    }
    try {
      return cache.get(kpURI, new Callable<KeyProvider>() {
        @Override
        public KeyProvider call() throws Exception {
          return DFSUtilClient.createKeyProvider(conf);
        }
      });
    } catch (Exception e) {
      LOG.error("Could not create KeyProvider for DFSClient !!", e.getCause());
      return null;
    }
  }

  private URI createKeyProviderURI(Configuration conf) {
    final String providerUriStr = conf.getTrimmed(
        HdfsClientConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, "");
    // No provider set in conf
    if (providerUriStr.isEmpty()) {
      LOG.error("Could not find uri with key ["
          + HdfsClientConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI
          + "] to create a keyProvider !!");
      return null;
    }
    final URI providerUri;
    try {
      providerUri = new URI(providerUriStr);
    } catch (URISyntaxException e) {
      LOG.error("KeyProvider URI string is invalid [" + providerUriStr
          + "]!!", e.getCause());
      return null;
    }
    return providerUri;
  }

  @VisibleForTesting
  public void setKeyProvider(Configuration conf, KeyProvider keyProvider) {
    URI uri = createKeyProviderURI(conf);
    assert uri != null;
    cache.put(uri, keyProvider);
  }
}
