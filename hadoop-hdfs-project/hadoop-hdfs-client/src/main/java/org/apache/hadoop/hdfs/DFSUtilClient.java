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

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.primitives.SignedBytes;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.BasicInetPeer;
import org.apache.hadoop.hdfs.net.NioInetPeer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.util.IOUtilsClient;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMESERVICES;

public class DFSUtilClient {
  public static final byte[] EMPTY_BYTES = {};
  private static final Logger LOG = LoggerFactory.getLogger(
      DFSUtilClient.class);
  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  public static byte[] string2Bytes(String str) {
    return str.getBytes(Charsets.UTF_8);
  }

  /**
   * Converts a byte array to a string using UTF8 encoding.
   */
  public static String bytes2String(byte[] bytes) {
    return bytes2String(bytes, 0, bytes.length);
  }

  /** Return used as percentage of capacity */
  public static float getPercentUsed(long used, long capacity) {
    return capacity <= 0 ? 100 : (used * 100.0f)/capacity;
  }

  /** Return remaining as percentage of capacity */
  public static float getPercentRemaining(long remaining, long capacity) {
    return capacity <= 0 ? 0 : (remaining * 100.0f)/capacity;
  }

  /** Convert percentage to a string. */
  public static String percent2String(double percentage) {
    return StringUtils.format("%.2f%%", percentage);
  }

  /**
   * Returns collection of nameservice Ids from the configuration.
   * @param conf configuration
   * @return collection of nameservice Ids, or null if not specified
   */
  public static Collection<String> getNameServiceIds(Configuration conf) {
    return conf.getTrimmedStringCollection(DFS_NAMESERVICES);
  }

  /**
   * Namenode HighAvailability related configuration.
   * Returns collection of namenode Ids from the configuration. One logical id
   * for each namenode in the in the HA setup.
   *
   * @param conf configuration
   * @param nsId the nameservice ID to look at, or null for non-federated
   * @return collection of namenode Ids
   */
  public static Collection<String> getNameNodeIds(Configuration conf, String nsId) {
    String key = addSuffix(DFS_HA_NAMENODES_KEY_PREFIX, nsId);
    return conf.getTrimmedStringCollection(key);
  }

  /** Add non empty and non null suffix to a key */
  static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    assert !suffix.startsWith(".") :
      "suffix '" + suffix + "' should not already have '.' prepended.";
    return key + "." + suffix;
  }

  /**
   * Returns list of InetSocketAddress corresponding to HA NN HTTP addresses from
   * the configuration.
   *
   * @return list of InetSocketAddresses
   */
  public static Map<String, Map<String, InetSocketAddress>> getHaNnWebHdfsAddresses(
      Configuration conf, String scheme) {
    if (WebHdfsConstants.WEBHDFS_SCHEME.equals(scheme)) {
      return getAddresses(conf, null,
          HdfsClientConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    } else if (WebHdfsConstants.SWEBHDFS_SCHEME.equals(scheme)) {
      return getAddresses(conf, null,
          HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    } else {
      throw new IllegalArgumentException("Unsupported scheme: " + scheme);
    }
  }

  /**
   * Convert a LocatedBlocks to BlockLocations[]
   * @param blocks a LocatedBlocks
   * @return an array of BlockLocations
   */
  public static BlockLocation[] locatedBlocks2Locations(LocatedBlocks blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    return locatedBlocks2Locations(blocks.getLocatedBlocks());
  }

  /**
   * Convert a List<LocatedBlock> to BlockLocation[]
   * @param blocks A List<LocatedBlock> to be converted
   * @return converted array of BlockLocation
   */
  public static BlockLocation[] locatedBlocks2Locations(
      List<LocatedBlock> blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    int nrBlocks = blocks.size();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    if (nrBlocks == 0) {
      return blkLocations;
    }
    int idx = 0;
    for (LocatedBlock blk : blocks) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] xferAddrs = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        xferAddrs[hCnt] = locations[hCnt].getXferAddr();
        NodeBase node = new NodeBase(xferAddrs[hCnt],
                                     locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      DatanodeInfo[] cachedLocations = blk.getCachedLocations();
      String[] cachedHosts = new String[cachedLocations.length];
      for (int i=0; i<cachedLocations.length; i++) {
        cachedHosts[i] = cachedLocations[i].getHostName();
      }
      blkLocations[idx] = new BlockLocation(xferAddrs, hosts, cachedHosts,
                                            racks,
                                            blk.getStorageIDs(),
                                            blk.getStorageTypes(),
                                            blk.getStartOffset(),
                                            blk.getBlockSize(),
                                            blk.isCorrupt());
      idx++;
    }
    return blkLocations;
  }

  /** Compare two byte arrays by lexicographical order. */
  public static int compareBytes(byte[] left, byte[] right) {
    if (left == null) {
      left = EMPTY_BYTES;
    }
    if (right == null) {
      right = EMPTY_BYTES;
    }
    return SignedBytes.lexicographicalComparator().compare(left, right);
  }

  /**
   * Given a list of path components returns a byte array
   */
  public static byte[] byteArray2bytes(byte[][] pathComponents) {
    if (pathComponents.length == 0) {
      return EMPTY_BYTES;
    } else if (pathComponents.length == 1
        && (pathComponents[0] == null || pathComponents[0].length == 0)) {
      return new byte[]{(byte) Path.SEPARATOR_CHAR};
    }
    int length = 0;
    for (int i = 0; i < pathComponents.length; i++) {
      length += pathComponents[i].length;
      if (i < pathComponents.length - 1) {
        length++; // for SEPARATOR
      }
    }
    byte[] path = new byte[length];
    int index = 0;
    for (int i = 0; i < pathComponents.length; i++) {
      System.arraycopy(pathComponents[i], 0, path, index,
          pathComponents[i].length);
      index += pathComponents[i].length;
      if (i < pathComponents.length - 1) {
        path[index] = (byte) Path.SEPARATOR_CHAR;
        index++;
      }
    }
    return path;
  }

  /**
   * Decode a specific range of bytes of the given byte array to a string
   * using UTF8.
   *
   * @param bytes The bytes to be decoded into characters
   * @param offset The index of the first byte to decode
   * @param length The number of bytes to decode
   * @return The decoded string
   */
  private static String bytes2String(byte[] bytes, int offset, int length) {
    try {
      return new String(bytes, offset, length, "UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * @return <code>coll</code> if it is non-null and non-empty. Otherwise,
   * returns a list with a single null value.
   */
  static Collection<String> emptyAsSingletonNull(Collection<String> coll) {
    if (coll == null || coll.isEmpty()) {
      return Collections.singletonList(null);
    } else {
      return coll;
    }
  }

  /** Concatenate list of suffix strings '.' separated */
  static String concatSuffixes(String... suffixes) {
    if (suffixes == null) {
      return null;
    }
    return Joiner.on(".").skipNulls().join(suffixes);
  }

  /**
   * Returns the configured address for all NameNodes in the cluster.
   * @param conf configuration
   * @param defaultAddress default address to return in case key is not found.
   * @param keys Set of keys to look for in the order of preference
   * @return a map(nameserviceId to map(namenodeId to InetSocketAddress))
   */
  static Map<String, Map<String, InetSocketAddress>> getAddresses(
      Configuration conf, String defaultAddress, String... keys) {
    Collection<String> nameserviceIds = getNameServiceIds(conf);
    return getAddressesForNsIds(conf, nameserviceIds, defaultAddress, keys);
  }

  /**
   * Returns the configured address for all NameNodes in the cluster.
   * @param conf configuration
   * @param defaultAddress default address to return in case key is not found.
   * @param keys Set of keys to look for in the order of preference
   *
   * @return a map(nameserviceId to map(namenodeId to InetSocketAddress))
   */
  static Map<String, Map<String, InetSocketAddress>> getAddressesForNsIds(
      Configuration conf, Collection<String> nsIds, String defaultAddress,
      String... keys) {
    // Look for configurations of the form <key>[.<nameserviceId>][.<namenodeId>]
    // across all of the configured nameservices and namenodes.
    Map<String, Map<String, InetSocketAddress>> ret = Maps.newLinkedHashMap();
    for (String nsId : emptyAsSingletonNull(nsIds)) {
      Map<String, InetSocketAddress> isas =
          getAddressesForNameserviceId(conf, nsId, defaultAddress, keys);
      if (!isas.isEmpty()) {
        ret.put(nsId, isas);
      }
    }
    return ret;
  }

  static Map<String, InetSocketAddress> getAddressesForNameserviceId(
      Configuration conf, String nsId, String defaultValue, String... keys) {
    Collection<String> nnIds = getNameNodeIds(conf, nsId);
    Map<String, InetSocketAddress> ret = Maps.newHashMap();
    for (String nnId : emptyAsSingletonNull(nnIds)) {
      String suffix = concatSuffixes(nsId, nnId);
      String address = getConfValue(defaultValue, suffix, conf, keys);
      if (address != null) {
        InetSocketAddress isa = NetUtils.createSocketAddr(address);
        if (isa.isUnresolved()) {
          LOG.warn("Namenode for {} remains unresolved for ID {}. Check your "
              + "hdfs-site.xml file to ensure namenodes are configured "
              + "properly.", nsId, nnId);
        }
        ret.put(nnId, isa);
      }
    }
    return ret;
  }

  /**
   * Given a list of keys in the order of preference, returns a value
   * for the key in the given order from the configuration.
   * @param defaultValue default value to return, when key was not found
   * @param keySuffix suffix to add to the key, if it is not null
   * @param conf Configuration
   * @param keys list of keys in the order of preference
   * @return value of the key or default if a key was not found in configuration
   */
  private static String getConfValue(String defaultValue, String keySuffix,
      Configuration conf, String... keys) {
    String value = null;
    for (String key : keys) {
      key = addSuffix(key, keySuffix);
      value = conf.get(key);
      if (value != null) {
        break;
      }
    }
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }

  /**
   * Whether the pathname is valid.  Currently prohibits relative paths,
   * names which contain a ":" or "//", or other non-canonical paths.
   */
  public static boolean isValidName(String src) {
    // Path must be absolute.
    if (!src.startsWith(Path.SEPARATOR)) {
      return false;
    }

    // Check for ".." "." ":" "/"
    String[] components = StringUtils.split(src, '/');
    for (int i = 0; i < components.length; i++) {
      String element = components[i];
      if (element.equals(".")  ||
          (element.contains(":"))  ||
          (element.contains("/"))) {
        return false;
      }
      // ".." is allowed in path starting with /.reserved/.inodes
      if (element.equals("..")) {
        if (components.length > 4
            && components[1].equals(".reserved")
            && components[2].equals(".inodes")) {
          continue;
        }
        return false;
      }
      // The string may start or end with a /, but not have
      // "//" in the middle.
      if (element.isEmpty() && i != components.length - 1 &&
          i != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Converts a time duration in milliseconds into DDD:HH:MM:SS format.
   */
  public static String durationToString(long durationMs) {
    boolean negative = false;
    if (durationMs < 0) {
      negative = true;
      durationMs = -durationMs;
    }
    // Chop off the milliseconds
    long durationSec = durationMs / 1000;
    final int secondsPerMinute = 60;
    final int secondsPerHour = 60*60;
    final int secondsPerDay = 60*60*24;
    final long days = durationSec / secondsPerDay;
    durationSec -= days * secondsPerDay;
    final long hours = durationSec / secondsPerHour;
    durationSec -= hours * secondsPerHour;
    final long minutes = durationSec / secondsPerMinute;
    durationSec -= minutes * secondsPerMinute;
    final long seconds = durationSec;
    final long milliseconds = durationMs % 1000;
    String format = "%03d:%02d:%02d:%02d.%03d";
    if (negative)  {
      format = "-" + format;
    }
    return String.format(format, days, hours, minutes, seconds, milliseconds);
  }

  /**
   * Converts a Date into an ISO-8601 formatted datetime string.
   */
  public static String dateToIso8601String(Date date) {
    SimpleDateFormat df =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.ENGLISH);
    return df.format(date);
  }

  private static final Map<String, Boolean> localAddrMap = Collections
      .synchronizedMap(new HashMap<String, Boolean>());

  public static boolean isLocalAddress(InetSocketAddress targetAddr) {
    InetAddress addr = targetAddr.getAddress();
    Boolean cached = localAddrMap.get(addr.getHostAddress());
    if (cached != null) {
      LOG.trace("Address {} is {} local", targetAddr, (cached ? "" : "not"));
      return cached;
    }

    boolean local = NetUtils.isLocalAddress(addr);

    LOG.trace("Address {} is {} local", targetAddr, (local ? "" : "not"));
    localAddrMap.put(addr.getHostAddress(), local);
    return local;
  }

  /** Create a {@link ClientDatanodeProtocol} proxy */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      boolean connectToDnViaHostname, LocatedBlock locatedBlock) throws IOException {
    return new ClientDatanodeProtocolTranslatorPB(datanodeid, conf, socketTimeout,
        connectToDnViaHostname, locatedBlock);
  }

  /** Create {@link ClientDatanodeProtocol} proxy using kerberos ticket */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      boolean connectToDnViaHostname) throws IOException {
    return new ClientDatanodeProtocolTranslatorPB(
        datanodeid, conf, socketTimeout, connectToDnViaHostname);
  }

  /** Create a {@link ClientDatanodeProtocol} proxy */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory) throws IOException {
    return new ClientDatanodeProtocolTranslatorPB(addr, ticket, conf, factory);
  }

  /**
   * Creates a new KeyProvider from the given Configuration.
   *
   * @param conf Configuration
   * @return new KeyProvider, or null if no provider was found.
   * @throws IOException if the KeyProvider is improperly specified in
   *                             the Configuration
   */
  public static KeyProvider createKeyProvider(
      final Configuration conf) throws IOException {
    final String providerUriStr =
        conf.getTrimmed(HdfsClientConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, "");
    // No provider set in conf
    if (providerUriStr.isEmpty()) {
      return null;
    }
    final URI providerUri;
    try {
      providerUri = new URI(providerUriStr);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    KeyProvider keyProvider = KeyProviderFactory.get(providerUri, conf);
    if (keyProvider == null) {
      throw new IOException("Could not instantiate KeyProvider from " +
          HdfsClientConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI + " setting of '"
          + providerUriStr + "'");
    }
    if (keyProvider.isTransient()) {
      throw new IOException("KeyProvider " + keyProvider.toString()
          + " was found but it is a transient provider.");
    }
    return keyProvider;
  }

  public static Peer peerFromSocket(Socket socket)
      throws IOException {
    Peer peer;
    boolean success = false;
    try {
      // TCP_NODELAY is crucial here because of bad interactions between
      // Nagle's Algorithm and Delayed ACKs. With connection keepalive
      // between the client and DN, the conversation looks like:
      //   1. Client -> DN: Read block X
      //   2. DN -> Client: data for block X
      //   3. Client -> DN: Status OK (successful read)
      //   4. Client -> DN: Read block Y
      // The fact that step #3 and #4 are both in the client->DN direction
      // triggers Nagling. If the DN is using delayed ACKs, this results
      // in a delay of 40ms or more.
      //
      // TCP_NODELAY disables nagling and thus avoids this performance
      // disaster.
      socket.setTcpNoDelay(true);
      SocketChannel channel = socket.getChannel();
      if (channel == null) {
        peer = new BasicInetPeer(socket);
      } else {
        peer = new NioInetPeer(socket);
      }
      success = true;
      return peer;
    } finally {
      if (!success) {
        // peer is always null so no need to call peer.close().
        socket.close();
      }
    }
  }

  public static Peer peerFromSocketAndKey(
        SaslDataTransferClient saslClient, Socket s,
        DataEncryptionKeyFactory keyFactory,
        Token<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
        throws IOException {
    Peer peer = null;
    boolean success = false;
    try {
      peer = peerFromSocket(s);
      peer = saslClient.peerSend(peer, keyFactory, blockToken, datanodeId);
      success = true;
      return peer;
    } finally {
      if (!success) {
        IOUtilsClient.cleanup(null, peer);
      }
    }
  }

  public static int getIoFileBufferSize(Configuration conf) {
    return conf.getInt(
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
  }

  public static int getSmallBufferSize(Configuration conf) {
    return Math.min(getIoFileBufferSize(conf) / 2, 512);
  }

  /**
   * Probe for HDFS Encryption being enabled; this uses the value of
   * the option {@link HdfsClientConfigKeys#DFS_ENCRYPTION_KEY_PROVIDER_URI},
   * returning true if that property contains a non-empty, non-whitespace
   * string.
   * @param conf configuration to probe
   * @return true if encryption is considered enabled.
   */
  public static boolean isHDFSEncryptionEnabled(Configuration conf) {
    return !conf.getTrimmed(
        HdfsClientConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, "").isEmpty();
  }

  public static InetSocketAddress getNNAddress(String address) {
    return NetUtils.createSocketAddr(address,
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT);
  }

  public static InetSocketAddress getNNAddress(Configuration conf) {
    URI filesystemURI = FileSystem.getDefaultUri(conf);
    return getNNAddressCheckLogical(conf, filesystemURI);
  }

  /**
   * @return address of file system
   */
  public static InetSocketAddress getNNAddress(URI filesystemURI) {
    String authority = filesystemURI.getAuthority();
    if (authority == null) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s has no authority.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString()));
    }
    if (!HdfsConstants.HDFS_URI_SCHEME.equalsIgnoreCase(
        filesystemURI.getScheme())) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): " +
          "%s is not of scheme '%s'.", FileSystem.FS_DEFAULT_NAME_KEY,
          filesystemURI.toString(), HdfsConstants.HDFS_URI_SCHEME));
    }
    return getNNAddress(authority);
  }

  /**
   * Get the NN address from the URI. If the uri is logical, default address is
   * returned. Otherwise return the DNS-resolved address of the URI.
   *
   * @param conf configuration
   * @param filesystemURI URI of the file system
   * @return address of file system
   */
  public static InetSocketAddress getNNAddressCheckLogical(Configuration conf,
      URI filesystemURI) {
    InetSocketAddress retAddr;
    if (HAUtilClient.isLogicalUri(conf, filesystemURI)) {
      retAddr = InetSocketAddress.createUnresolved(filesystemURI.getAuthority(),
          HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT);
    } else {
      retAddr = getNNAddress(filesystemURI);
    }
    return retAddr;
  }

  public static URI getNNUri(InetSocketAddress namenode) {
    int port = namenode.getPort();
    String portString =
        (port == HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT) ?
        "" : (":" + port);
    return URI.create(HdfsConstants.HDFS_URI_SCHEME + "://"
        + namenode.getHostName() + portString);
  }

  public static InterruptedIOException toInterruptedIOException(String message,
      InterruptedException e) {
    final InterruptedIOException iioe = new InterruptedIOException(message);
    iioe.initCause(e);
    return iioe;
  }
}
