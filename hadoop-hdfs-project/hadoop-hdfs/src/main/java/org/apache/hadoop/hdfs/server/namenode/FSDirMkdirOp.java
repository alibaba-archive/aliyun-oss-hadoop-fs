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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.util.Time.now;

class FSDirMkdirOp {

  static HdfsFileStatus mkdirs(FSNamesystem fsn, String src,
      PermissionStatus permissions, boolean createParent) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
    }
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }
    FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    fsd.writeLock();
    try {
      src = fsd.resolvePath(pc, src, pathComponents);
      INodesInPath iip = fsd.getINodesInPath4Write(src);
      if (fsd.isPermissionEnabled()) {
        fsd.checkTraverse(pc, iip);
      }

      final INode lastINode = iip.getLastINode();
      if (lastINode != null && lastINode.isFile()) {
        throw new FileAlreadyExistsException("Path is not a directory: " + src);
      }

      INodesInPath existing = lastINode != null ? iip : iip.getExistingINodes();
      if (lastINode == null) {
        if (fsd.isPermissionEnabled()) {
          fsd.checkAncestorAccess(pc, iip, FsAction.WRITE);
        }

        if (!createParent) {
          fsd.verifyParentDir(iip, src);
        }

        // validate that we have enough inodes. This is, at best, a
        // heuristic because the mkdirs() operation might need to
        // create multiple inodes.
        fsn.checkFsObjectLimit();

        List<String> nonExisting = iip.getPath(existing.length(),
            iip.length() - existing.length());
        int length = nonExisting.size();
        if (length > 1) {
          List<String> ancestors = nonExisting.subList(0, length - 1);
          // Ensure that the user can traversal the path by adding implicit
          // u+wx permission to all ancestor directories
          existing = createChildrenDirectories(fsd, existing, ancestors,
              addImplicitUwx(permissions, permissions));
          if (existing == null) {
            throw new IOException("Failed to create directory: " + src);
          }
        }

        if ((existing = createChildrenDirectories(fsd, existing,
            nonExisting.subList(length - 1, length), permissions)) == null) {
          throw new IOException("Failed to create directory: " + src);
        }
      }
      return fsd.getAuditFileInfo(existing);
    } finally {
      fsd.writeUnlock();
    }
  }

  /**
   * For a given absolute path, create all ancestors as directories along the
   * path. All ancestors inherit their parent's permission plus an implicit
   * u+wx permission. This is used by create() and addSymlink() for
   * implicitly creating all directories along the path.
   *
   * For example, path="/foo/bar/spam", "/foo" is an existing directory,
   * "/foo/bar" is not existing yet, the function will create directory bar.
   *
   * @return a tuple which contains both the new INodesInPath (with all the
   * existing and newly created directories) and the last component in the
   * relative path. Or return null if there are errors.
   */
  static Map.Entry<INodesInPath, String> createAncestorDirectories(
      FSDirectory fsd, INodesInPath iip, PermissionStatus permission)
      throws IOException {
    final String last = new String(iip.getLastLocalName(), Charsets.UTF_8);
    INodesInPath existing = iip.getExistingINodes();
    List<String> children = iip.getPath(existing.length(),
        iip.length() - existing.length());
    int size = children.size();
    if (size > 1) { // otherwise all ancestors have been created
      List<String> directories = children.subList(0, size - 1);
      INode parentINode = existing.getLastINode();
      // Ensure that the user can traversal the path by adding implicit
      // u+wx permission to all ancestor directories
      existing = createChildrenDirectories(fsd, existing, directories,
          addImplicitUwx(parentINode.getPermissionStatus(), permission));
      if (existing == null) {
        return null;
      }
    }
    return new AbstractMap.SimpleImmutableEntry<>(existing, last);
  }

  /**
   * Create the directory {@code parent} / {@code children} and all ancestors
   * along the path.
   *
   * @param fsd FSDirectory
   * @param existing The INodesInPath instance containing all the existing
   *                 ancestral INodes
   * @param children The relative path from the parent towards children,
   *                 starting with "/"
   * @param perm the permission of the directory. Note that all ancestors
   *             created along the path has implicit {@code u+wx} permissions.
   *
   * @return {@link INodesInPath} which contains all inodes to the
   * target directory, After the execution parentPath points to the path of
   * the returned INodesInPath. The function return null if the operation has
   * failed.
   */
  private static INodesInPath createChildrenDirectories(FSDirectory fsd,
      INodesInPath existing, List<String> children, PermissionStatus perm)
      throws IOException {
    assert fsd.hasWriteLock();

    for (String component : children) {
      existing = createSingleDirectory(fsd, existing, component, perm);
      if (existing == null) {
        return null;
      }
    }
    return existing;
  }

  static void mkdirForEditLog(FSDirectory fsd, long inodeId, String src,
      PermissionStatus permissions, List<AclEntry> aclEntries, long timestamp)
      throws QuotaExceededException, UnresolvedLinkException, AclException,
      FileAlreadyExistsException {
    assert fsd.hasWriteLock();
    INodesInPath iip = fsd.getINodesInPath(src, false);
    final byte[] localName = iip.getLastLocalName();
    final INodesInPath existing = iip.getParentINodesInPath();
    Preconditions.checkState(existing.getLastINode() != null);
    unprotectedMkdir(fsd, inodeId, existing, localName, permissions, aclEntries,
        timestamp);
  }

  private static INodesInPath createSingleDirectory(FSDirectory fsd,
      INodesInPath existing, String localName, PermissionStatus perm)
      throws IOException {
    assert fsd.hasWriteLock();
    existing = unprotectedMkdir(fsd, fsd.allocateNewInodeId(), existing,
        localName.getBytes(Charsets.UTF_8), perm, null, now());
    if (existing == null) {
      return null;
    }

    final INode newNode = existing.getLastINode();
    // Directory creation also count towards FilesCreated
    // to match count of FilesDeleted metric.
    NameNode.getNameNodeMetrics().incrFilesCreated();

    String cur = existing.getPath();
    fsd.getEditLog().logMkDir(cur, newNode);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("mkdirs: created directory " + cur);
    }
    return existing;
  }

  private static PermissionStatus addImplicitUwx(PermissionStatus parentPerm,
      PermissionStatus perm) {
    FsPermission p = parentPerm.getPermission();
    FsPermission ancestorPerm = new FsPermission(
        p.getUserAction().or(FsAction.WRITE_EXECUTE),
        p.getGroupAction(),
        p.getOtherAction());
    return new PermissionStatus(perm.getUserName(), perm.getGroupName(),
        ancestorPerm);
  }

  /**
   * create a directory at path specified by parent
   */
  private static INodesInPath unprotectedMkdir(FSDirectory fsd, long inodeId,
      INodesInPath parent, byte[] name, PermissionStatus permission,
      List<AclEntry> aclEntries, long timestamp)
      throws QuotaExceededException, AclException, FileAlreadyExistsException {
    assert fsd.hasWriteLock();
    assert parent.getLastINode() != null;
    if (!parent.getLastINode().isDirectory()) {
      throw new FileAlreadyExistsException("Parent path is not a directory: " +
          parent.getPath() + " " + DFSUtil.bytes2String(name));
    }
    final INodeDirectory dir = new INodeDirectory(inodeId, name, permission,
        timestamp);

    INodesInPath iip = fsd.addLastINode(parent, dir, true);
    if (iip != null && aclEntries != null) {
      AclStorage.updateINodeAcl(dir, aclEntries, Snapshot.CURRENT_STATE_ID);
    }
    return iip;
  }
}

