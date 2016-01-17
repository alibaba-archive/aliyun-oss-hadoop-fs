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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.*;

/** JSON Utilities */
public class JsonUtil {
  private static final Object[] EMPTY_OBJECT_ARRAY = {};

  /** Convert a token object to a Json string. */
  public static String toJsonString(final Token<? extends TokenIdentifier> token
      ) throws IOException {
    return toJsonString(Token.class, toJsonMap(token));
  }

  private static Map<String, Object> toJsonMap(
      final Token<? extends TokenIdentifier> token) throws IOException {
    if (token == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("urlString", token.encodeToUrlString());
    return m;
  }

  /** Convert an exception object to a Json string. */
  public static String toJsonString(final Exception e) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("exception", e.getClass().getSimpleName());
    m.put("message", e.getMessage());
    m.put("javaClassName", e.getClass().getName());
    return toJsonString(RemoteException.class, m);
  }

  private static String toJsonString(final Class<?> clazz, final Object value) {
    return toJsonString(clazz.getSimpleName(), value);
  }

  /** Convert a key-value pair to a Json string. */
  public static String toJsonString(final String key, final Object value) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put(key, value);
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(m);
    } catch (IOException ignored) {
    }
    return null;
  }

  /** Convert a FsPermission object to a string. */
  private static String toString(final FsPermission permission) {
    return String.format("%o", permission.toShort());
  }

  /** Convert a HdfsFileStatus object to a Json string. */
  public static String toJsonString(final HdfsFileStatus status,
      boolean includeType) {
    if (status == null) {
      return null;
    }
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("pathSuffix", status.getLocalName());
    m.put("type", WebHdfsConstants.PathType.valueOf(status));
    if (status.isSymlink()) {
      m.put("symlink", status.getSymlink());
    }

    m.put("length", status.getLen());
    m.put("owner", status.getOwner());
    m.put("group", status.getGroup());
    FsPermission perm = status.getPermission();
    m.put("permission", toString(perm));
    if (perm.getAclBit()) {
      m.put("aclBit", true);
    }
    if (perm.getEncryptedBit()) {
      m.put("encBit", true);
    }
    m.put("accessTime", status.getAccessTime());
    m.put("modificationTime", status.getModificationTime());
    m.put("blockSize", status.getBlockSize());
    m.put("replication", status.getReplication());
    m.put("fileId", status.getFileId());
    m.put("childrenNum", status.getChildrenNum());
    m.put("storagePolicy", status.getStoragePolicy());
    ObjectMapper mapper = new ObjectMapper();
    try {
      return includeType ?
          toJsonString(FileStatus.class, m) : mapper.writeValueAsString(m);
    } catch (IOException ignored) {
    }
    return null;
  }

  /** Convert an ExtendedBlock to a Json map. */
  private static Map<String, Object> toJsonMap(final ExtendedBlock extendedblock) {
    if (extendedblock == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("blockPoolId", extendedblock.getBlockPoolId());
    m.put("blockId", extendedblock.getBlockId());
    m.put("numBytes", extendedblock.getNumBytes());
    m.put("generationStamp", extendedblock.getGenerationStamp());
    return m;
  }

  /** Convert a DatanodeInfo to a Json map. */
  static Map<String, Object> toJsonMap(final DatanodeInfo datanodeinfo) {
    if (datanodeinfo == null) {
      return null;
    }

    // TODO: Fix storageID
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("ipAddr", datanodeinfo.getIpAddr());
    // 'name' is equivalent to ipAddr:xferPort. Older clients (1.x, 0.23.x) 
    // expects this instead of the two fields.
    m.put("name", datanodeinfo.getXferAddr());
    m.put("hostName", datanodeinfo.getHostName());
    m.put("storageID", datanodeinfo.getDatanodeUuid());
    m.put("xferPort", datanodeinfo.getXferPort());
    m.put("infoPort", datanodeinfo.getInfoPort());
    m.put("infoSecurePort", datanodeinfo.getInfoSecurePort());
    m.put("ipcPort", datanodeinfo.getIpcPort());

    m.put("capacity", datanodeinfo.getCapacity());
    m.put("dfsUsed", datanodeinfo.getDfsUsed());
    m.put("remaining", datanodeinfo.getRemaining());
    m.put("blockPoolUsed", datanodeinfo.getBlockPoolUsed());
    m.put("cacheCapacity", datanodeinfo.getCacheCapacity());
    m.put("cacheUsed", datanodeinfo.getCacheUsed());
    m.put("lastUpdate", datanodeinfo.getLastUpdate());
    m.put("lastUpdateMonotonic", datanodeinfo.getLastUpdateMonotonic());
    m.put("xceiverCount", datanodeinfo.getXceiverCount());
    m.put("networkLocation", datanodeinfo.getNetworkLocation());
    m.put("adminState", datanodeinfo.getAdminState().name());
    if (datanodeinfo.getUpgradeDomain() != null) {
      m.put("upgradeDomain", datanodeinfo.getUpgradeDomain());
    }
    return m;
  }

  /** Convert a DatanodeInfo[] to a Json array. */
  private static Object[] toJsonArray(final DatanodeInfo[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.length];
      for(int i = 0; i < array.length; i++) {
        a[i] = toJsonMap(array[i]);
      }
      return a;
    }
  }

  /** Convert a StorageType[] to a Json array. */
  private static Object[] toJsonArray(final StorageType[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.length];
      for(int i = 0; i < array.length; i++) {
        a[i] = array[i];
      }
      return a;
    }
  }

  /** Convert a LocatedBlock to a Json map. */
  private static Map<String, Object> toJsonMap(final LocatedBlock locatedblock
      ) throws IOException {
    if (locatedblock == null) {
      return null;
    }
 
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("blockToken", toJsonMap(locatedblock.getBlockToken()));
    m.put("isCorrupt", locatedblock.isCorrupt());
    m.put("startOffset", locatedblock.getStartOffset());
    m.put("block", toJsonMap(locatedblock.getBlock()));
    m.put("storageTypes", toJsonArray(locatedblock.getStorageTypes()));
    m.put("locations", toJsonArray(locatedblock.getLocations()));
    m.put("cachedLocations", toJsonArray(locatedblock.getCachedLocations()));
    return m;
  }

  /** Convert a LocatedBlock[] to a Json array. */
  private static Object[] toJsonArray(final List<LocatedBlock> array
      ) throws IOException {
    if (array == null) {
      return null;
    } else if (array.size() == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.size()];
      for(int i = 0; i < array.size(); i++) {
        a[i] = toJsonMap(array.get(i));
      }
      return a;
    }
  }

  /** Convert LocatedBlocks to a Json string. */
  public static String toJsonString(final LocatedBlocks locatedblocks
      ) throws IOException {
    if (locatedblocks == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("fileLength", locatedblocks.getFileLength());
    m.put("isUnderConstruction", locatedblocks.isUnderConstruction());

    m.put("locatedBlocks", toJsonArray(locatedblocks.getLocatedBlocks()));
    m.put("lastLocatedBlock", toJsonMap(locatedblocks.getLastLocatedBlock()));
    m.put("isLastBlockComplete", locatedblocks.isLastBlockComplete());
    return toJsonString(LocatedBlocks.class, m);
  }

  /** Convert a ContentSummary to a Json string. */
  public static String toJsonString(final ContentSummary contentsummary) {
    if (contentsummary == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("length", contentsummary.getLength());
    m.put("fileCount", contentsummary.getFileCount());
    m.put("directoryCount", contentsummary.getDirectoryCount());
    m.put("quota", contentsummary.getQuota());
    m.put("spaceConsumed", contentsummary.getSpaceConsumed());
    m.put("spaceQuota", contentsummary.getSpaceQuota());
    final Map<String, Map<String, Long>> typeQuota =
        new TreeMap<String, Map<String, Long>>();
    for (StorageType t : StorageType.getTypesSupportingQuota()) {
      long tQuota = contentsummary.getTypeQuota(t);
      if (tQuota != HdfsConstants.QUOTA_RESET) {
        Map<String, Long> type = typeQuota.get(t.toString());
        if (type == null) {
          type = new TreeMap<String, Long>();
          typeQuota.put(t.toString(), type);
        }
        type.put("quota", contentsummary.getTypeQuota(t));
        type.put("consumed", contentsummary.getTypeConsumed(t));
      }
    }
    m.put("typeQuota", typeQuota);
    return toJsonString(ContentSummary.class, m);
  }

  /** Convert a MD5MD5CRC32FileChecksum to a Json string. */
  public static String toJsonString(final MD5MD5CRC32FileChecksum checksum) {
    if (checksum == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("algorithm", checksum.getAlgorithmName());
    m.put("length", checksum.getLength());
    m.put("bytes", StringUtils.byteToHexString(checksum.getBytes()));
    return toJsonString(FileChecksum.class, m);
  }

  /** Convert a AclStatus object to a Json string. */
  public static String toJsonString(final AclStatus status) {
    if (status == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("owner", status.getOwner());
    m.put("group", status.getGroup());
    m.put("stickyBit", status.isStickyBit());

    final List<String> stringEntries = new ArrayList<>();
    for (AclEntry entry : status.getEntries()) {
      stringEntries.add(entry.toString());
    }
    m.put("entries", stringEntries);

    FsPermission perm = status.getPermission();
    if (perm != null) {
      m.put("permission", toString(perm));
      if (perm.getAclBit()) {
        m.put("aclBit", true);
      }
      if (perm.getEncryptedBit()) {
        m.put("encBit", true);
      }
    }
    final Map<String, Map<String, Object>> finalMap =
        new TreeMap<String, Map<String, Object>>();
    finalMap.put(AclStatus.class.getSimpleName(), m);

    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(finalMap);
    } catch (IOException ignored) {
    }
    return null;
  }

  private static Map<String, Object> toJsonMap(final XAttr xAttr,
      final XAttrCodec encoding) throws IOException {
    if (xAttr == null) {
      return null;
    }
 
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("name", XAttrHelper.getPrefixedName(xAttr));
    m.put("value", xAttr.getValue() != null ?
        XAttrCodec.encodeValue(xAttr.getValue(), encoding) : null);
    return m;
  }
  
  private static Object[] toJsonArray(final List<XAttr> array,
      final XAttrCodec encoding) throws IOException {
    if (array == null) {
      return null;
    } else if (array.size() == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.size()];
      for(int i = 0; i < array.size(); i++) {
        a[i] = toJsonMap(array.get(i), encoding);
      }
      return a;
    }
  }
  
  public static String toJsonString(final List<XAttr> xAttrs, 
      final XAttrCodec encoding) throws IOException {
    final Map<String, Object> finalMap = new TreeMap<String, Object>();
    finalMap.put("XAttrs", toJsonArray(xAttrs, encoding));
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(finalMap);
  }
  
  public static String toJsonString(final List<XAttr> xAttrs)
    throws IOException {
    final List<String> names = Lists.newArrayListWithCapacity(xAttrs.size());
    for (XAttr xAttr : xAttrs) {
      names.add(XAttrHelper.getPrefixedName(xAttr));
    }
    ObjectMapper mapper = new ObjectMapper();
    String ret = mapper.writeValueAsString(names);
    final Map<String, Object> finalMap = new TreeMap<String, Object>();
    finalMap.put("XAttrNames", ret);
    return mapper.writeValueAsString(finalMap);
  }

}
