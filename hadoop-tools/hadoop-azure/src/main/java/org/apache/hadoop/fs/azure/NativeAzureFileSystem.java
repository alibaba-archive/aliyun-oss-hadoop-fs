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

package org.apache.hadoop.fs.azure;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemMetricsSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlobWrapper;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageErrorCode;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import org.apache.hadoop.io.IOUtils;

/**
 * A {@link FileSystem} for reading and writing files stored on <a
 * href="http://store.azure.com/">Windows Azure</a>. This implementation is
 * blob-based and stores files on Azure in their native form so they can be read
 * by other Azure tools.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NativeAzureFileSystem extends FileSystem {
  private static final int USER_WX_PERMISION = 0300;
  /**
   * A description of a folder rename operation, including the source and
   * destination keys, and descriptions of the files in the source folder.
   */
  public static class FolderRenamePending {
    private SelfRenewingLease folderLease;
    private String srcKey;
    private String dstKey;
    private FileMetadata[] fileMetadata = null;    // descriptions of source files
    private ArrayList<String> fileStrings = null;
    private NativeAzureFileSystem fs;
    private static final int MAX_RENAME_PENDING_FILE_SIZE = 10000000;
    private static final int FORMATTING_BUFFER = 10000;
    private boolean committed;
    public static final String SUFFIX = "-RenamePending.json";

    // Prepare in-memory information needed to do or redo a folder rename.
    public FolderRenamePending(String srcKey, String dstKey, SelfRenewingLease lease,
        NativeAzureFileSystem fs) throws IOException {
      this.srcKey = srcKey;
      this.dstKey = dstKey;
      this.folderLease = lease;
      this.fs = fs;
      ArrayList<FileMetadata> fileMetadataList = new ArrayList<FileMetadata>();

      // List all the files in the folder.
      String priorLastKey = null;
      do {
        PartialListing listing = fs.getStoreInterface().listAll(srcKey, AZURE_LIST_ALL,
          AZURE_UNBOUNDED_DEPTH, priorLastKey);
        for(FileMetadata file : listing.getFiles()) {
          fileMetadataList.add(file);
        }
        priorLastKey = listing.getPriorLastKey();
      } while (priorLastKey != null);
      fileMetadata = fileMetadataList.toArray(new FileMetadata[fileMetadataList.size()]);
      this.committed = true;
    }

    // Prepare in-memory information needed to do or redo folder rename from
    // a -RenamePending.json file read from storage. This constructor is to use during
    // redo processing.
    public FolderRenamePending(Path redoFile, NativeAzureFileSystem fs)
        throws IllegalArgumentException, IOException {

      this.fs = fs;

      // open redo file
      Path f = redoFile;
      FSDataInputStream input = fs.open(f);
      byte[] bytes = new byte[MAX_RENAME_PENDING_FILE_SIZE];
      int l = input.read(bytes);
      if (l < 0) {
        throw new IOException(
            "Error reading pending rename file contents -- no data available");
      }
      if (l == MAX_RENAME_PENDING_FILE_SIZE) {
        throw new IOException(
            "Error reading pending rename file contents -- "
                + "maximum file size exceeded");
      }
      String contents = new String(bytes, 0, l, Charset.forName("UTF-8"));

      // parse the JSON
      ObjectMapper objMapper = new ObjectMapper();
      objMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      JsonNode json = null;
      try {
        json = objMapper.readValue(contents, JsonNode.class);
        this.committed = true;
      } catch (JsonMappingException e) {

        // The -RedoPending.json file is corrupted, so we assume it was
        // not completely written
        // and the redo operation did not commit.
        this.committed = false;
      } catch (JsonParseException e) {
        this.committed = false;
      } catch (IOException e) {
        this.committed = false;  
      }
      
      if (!this.committed) {
        LOG.error("Deleting corruped rename pending file {} \n {}",
            redoFile, contents);

        // delete the -RenamePending.json file
        fs.delete(redoFile, false);
        return;
      }

      // initialize this object's fields
      ArrayList<String> fileStrList = new ArrayList<String>();
      JsonNode oldFolderName = json.get("OldFolderName");
      JsonNode newFolderName = json.get("NewFolderName");
      if (oldFolderName == null || newFolderName == null) {
    	  this.committed = false;
      } else {
        this.srcKey = oldFolderName.getTextValue();
        this.dstKey = newFolderName.getTextValue();
        if (this.srcKey == null || this.dstKey == null) {
          this.committed = false;    	  
        } else {
          JsonNode fileList = json.get("FileList");
          if (fileList == null) {
            this.committed = false;	
          } else {
            for (int i = 0; i < fileList.size(); i++) {
              fileStrList.add(fileList.get(i).getTextValue());
            }
          }
        }
      }
      this.fileStrings = fileStrList;
    }

    public FileMetadata[] getFiles() {
      return fileMetadata;
    }

    public SelfRenewingLease getFolderLease() {
      return folderLease;
    }

    /**
     * Write to disk the information needed to redo folder rename,
     * in JSON format. The file name will be
     * {@code wasb://<sourceFolderPrefix>/folderName-RenamePending.json}
     * The file format will be:
     * <pre>{@code
     * {
     *   FormatVersion: "1.0",
     *   OperationTime: "<YYYY-MM-DD HH:MM:SS.MMM>",
     *   OldFolderName: "<key>",
     *   NewFolderName: "<key>",
     *   FileList: [ <string> , <string> , ... ]
     * }
     *
     * Here's a sample:
     * {
     *  FormatVersion: "1.0",
     *  OperationUTCTime: "2014-07-01 23:50:35.572",
     *  OldFolderName: "user/ehans/folderToRename",
     *  NewFolderName: "user/ehans/renamedFolder",
     *  FileList: [
     *    "innerFile",
     *    "innerFile2"
     *  ]
     * } }</pre>
     * @throws IOException
     */
    public void writeFile(FileSystem fs) throws IOException {
      Path path = getRenamePendingFilePath();
      LOG.debug("Preparing to write atomic rename state to {}", path.toString());
      OutputStream output = null;

      String contents = makeRenamePendingFileContents();

      // Write file.
      try {
        output = fs.create(path);
        output.write(contents.getBytes(Charset.forName("UTF-8")));
      } catch (IOException e) {
        throw new IOException("Unable to write RenamePending file for folder rename from "
            + srcKey + " to " + dstKey, e);
      } finally {
        NativeAzureFileSystem.cleanup(LOG, output);
      }
    }

    /**
     * Return the contents of the JSON file to represent the operations
     * to be performed for a folder rename.
     */
    public String makeRenamePendingFileContents() {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      String time = sdf.format(new Date());

      // Make file list string
      StringBuilder builder = new StringBuilder();
      builder.append("[\n");
      for (int i = 0; i != fileMetadata.length; i++) {
        if (i > 0) {
          builder.append(",\n");
        }
        builder.append("    ");
        String noPrefix = StringUtils.removeStart(fileMetadata[i].getKey(), srcKey + "/");

        // Quote string file names, escaping any possible " characters or other
        // necessary characters in the name.
        builder.append(quote(noPrefix));
        if (builder.length() >=
            MAX_RENAME_PENDING_FILE_SIZE - FORMATTING_BUFFER) {

          // Give up now to avoid using too much memory.
          LOG.error("Internal error: Exceeded maximum rename pending file size of {} bytes.",
              MAX_RENAME_PENDING_FILE_SIZE);

          // return some bad JSON with an error message to make it human readable
          return "exceeded maximum rename pending file size";
        }
      }
      builder.append("\n  ]");
      String fileList = builder.toString();

      // Make file contents as a string. Again, quote file names, escaping
      // characters as appropriate.
      String contents = "{\n"
          + "  FormatVersion: \"1.0\",\n"
          + "  OperationUTCTime: \"" + time + "\",\n"
          + "  OldFolderName: " + quote(srcKey) + ",\n"
          + "  NewFolderName: " + quote(dstKey) + ",\n"
          + "  FileList: " + fileList + "\n"
          + "}\n";

      return contents;
    }
    
    /**
     * This is an exact copy of org.codehaus.jettison.json.JSONObject.quote 
     * method.
     * 
     * Produce a string in double quotes with backslash sequences in all the
     * right places. A backslash will be inserted within </, allowing JSON
     * text to be delivered in HTML. In JSON text, a string cannot contain a
     * control character or an unescaped quote or backslash.
     * @param string A String
     * @return  A String correctly formatted for insertion in a JSON text.
     */
    private String quote(String string) {
        if (string == null || string.length() == 0) {
            return "\"\"";
        }

        char c = 0;
        int  i;
        int  len = string.length();
        StringBuilder sb = new StringBuilder(len + 4);
        String t;

        sb.append('"');
        for (i = 0; i < len; i += 1) {
            c = string.charAt(i);
            switch (c) {
            case '\\':
            case '"':
                sb.append('\\');
                sb.append(c);
                break;
            case '/':
                sb.append('\\');
                sb.append(c);
                break;
            case '\b':
                sb.append("\\b");
                break;
            case '\t':
                sb.append("\\t");
                break;
            case '\n':
                sb.append("\\n");
                break;
            case '\f':
                sb.append("\\f");
                break;
            case '\r':
                sb.append("\\r");
                break;
            default:
                if (c < ' ') {
                    t = "000" + Integer.toHexString(c);
                    sb.append("\\u" + t.substring(t.length() - 4));
                } else {
                    sb.append(c);
                }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    public String getSrcKey() {
      return srcKey;
    }

    public String getDstKey() {
      return dstKey;
    }

    public FileMetadata getSourceMetadata() throws IOException {
      return fs.getStoreInterface().retrieveMetadata(srcKey);
    }

    /**
     * Execute a folder rename. This is the execution path followed
     * when everything is working normally. See redo() for the alternate
     * execution path for the case where we're recovering from a folder rename
     * failure.
     * @throws IOException
     */
    public void execute() throws IOException {

      for (FileMetadata file : this.getFiles()) {

        // Rename all materialized entries under the folder to point to the
        // final destination.
        if (file.getBlobMaterialization() == BlobMaterialization.Explicit) {
          String srcName = file.getKey();
          String suffix  = srcName.substring((this.getSrcKey()).length());
          String dstName = this.getDstKey() + suffix;

          // Rename gets exclusive access (via a lease) for files
          // designated for atomic rename.
          // The main use case is for HBase write-ahead log (WAL) and data
          // folder processing correctness.  See the rename code for details.
          boolean acquireLease = fs.getStoreInterface().isAtomicRenameKey(srcName);
          fs.getStoreInterface().rename(srcName, dstName, acquireLease, null);
        }
      }

      // Rename the source folder 0-byte root file itself.
      FileMetadata srcMetadata2 = this.getSourceMetadata();
      if (srcMetadata2.getBlobMaterialization() ==
          BlobMaterialization.Explicit) {

        // It already has a lease on it from the "prepare" phase so there's no
        // need to get one now. Pass in existing lease to allow file delete.
        fs.getStoreInterface().rename(this.getSrcKey(), this.getDstKey(),
            false, folderLease);
      }

      // Update the last-modified time of the parent folders of both source and
      // destination.
      fs.updateParentFolderLastModifiedTime(srcKey);
      fs.updateParentFolderLastModifiedTime(dstKey);
    }

    /** Clean up after execution of rename.
     * @throws IOException */
    public void cleanup() throws IOException {

      if (fs.getStoreInterface().isAtomicRenameKey(srcKey)) {

        // Remove RenamePending file
        fs.delete(getRenamePendingFilePath(), false);

        // Freeing source folder lease is not necessary since the source
        // folder file was deleted.
      }
    }

    private Path getRenamePendingFilePath() {
      String fileName = srcKey + SUFFIX;
      Path fileNamePath = keyToPath(fileName);
      Path path = fs.makeAbsolute(fileNamePath);
      return path;
    }

    /**
     * Recover from a folder rename failure by redoing the intended work,
     * as recorded in the -RenamePending.json file.
     * 
     * @throws IOException
     */
    public void redo() throws IOException {

      if (!committed) {

        // Nothing to do. The -RedoPending.json file should have already been
        // deleted.
        return;
      }

      // Try to get a lease on source folder to block concurrent access to it.
      // It may fail if the folder is already gone. We don't check if the
      // source exists explicitly because that could recursively trigger redo
      // and give an infinite recursion.
      SelfRenewingLease lease = null;
      boolean sourceFolderGone = false;
      try {
        lease = fs.leaseSourceFolder(srcKey);
      } catch (AzureException e) {

        // If the source folder was not found then somebody probably
        // raced with us and finished the rename first, or the
        // first rename failed right before deleting the rename pending
        // file.
        String errorCode = "";
        try {
          StorageException se = (StorageException) e.getCause();
          errorCode = se.getErrorCode();
        } catch (Exception e2) {
          ; // do nothing -- could not get errorCode
        }
        if (errorCode.equals("BlobNotFound")) {
          sourceFolderGone = true;
        } else {
          throw new IOException(
              "Unexpected error when trying to lease source folder name during "
              + "folder rename redo",
              e);
        }
      }

      if (!sourceFolderGone) {
        // Make sure the target folder exists.
        Path dst = fullPath(dstKey);
        if (!fs.exists(dst)) {
          fs.mkdirs(dst);
        }

        // For each file inside the folder to be renamed,
        // make sure it has been renamed.
        for(String fileName : fileStrings) {
          finishSingleFileRename(fileName);
        }

        // Remove the source folder. Don't check explicitly if it exists,
        // to avoid triggering redo recursively.
        try {
          fs.getStoreInterface().delete(srcKey, lease);
        } catch (Exception e) {
          LOG.info("Unable to delete source folder during folder rename redo. "
              + "If the source folder is already gone, this is not an error "
              + "condition. Continuing with redo.", e);
        }

        // Update the last-modified time of the parent folders of both source
        // and destination.
        fs.updateParentFolderLastModifiedTime(srcKey);
        fs.updateParentFolderLastModifiedTime(dstKey);
      }

      // Remove the -RenamePending.json file.
      fs.delete(getRenamePendingFilePath(), false);
    }

    // See if the source file is still there, and if it is, rename it.
    private void finishSingleFileRename(String fileName)
        throws IOException {
      Path srcFile = fullPath(srcKey, fileName);
      Path dstFile = fullPath(dstKey, fileName);
      boolean srcExists = fs.exists(srcFile);
      boolean dstExists = fs.exists(dstFile);
      if (srcExists && !dstExists) {

        // Rename gets exclusive access (via a lease) for HBase write-ahead log
        // (WAL) file processing correctness.  See the rename code for details.
        String srcName = fs.pathToKey(srcFile);
        String dstName = fs.pathToKey(dstFile);
        fs.getStoreInterface().rename(srcName, dstName, true, null);
      } else if (srcExists && dstExists) {

        // Get a lease on source to block write access.
        String srcName = fs.pathToKey(srcFile);
        SelfRenewingLease lease = null;
        try {
          lease = fs.acquireLease(srcFile);
          // Delete the file. This will free the lease too.
          fs.getStoreInterface().delete(srcName, lease);
        } catch(AzureException e) {
            String errorCode = "";
            try {
              StorageException e2 = (StorageException) e.getCause();
              errorCode = e2.getErrorCode();
            } catch(Exception e3) {
              // do nothing if cast fails
            }
            // If the rename already finished do nothing
            if(!errorCode.equals("BlobNotFound")){
              throw e;
            }
        } finally {
          try {
            if(lease != null){
              lease.free();
            }
          } catch(StorageException e) {
            LOG.warn("Unable to free lease because: " + e.getMessage());
          }
        }
      } else if (!srcExists && dstExists) {

        // The rename already finished, so do nothing.
        ;
      } else {
        throw new IOException(
            "Attempting to complete rename of file " + srcKey + "/" + fileName
            + " during folder rename redo, and file was not found in source "
            + "or destination.");
      }
    }

    // Return an absolute path for the specific fileName within the folder
    // specified by folderKey.
    private Path fullPath(String folderKey, String fileName) {
      return new Path(new Path(fs.getUri()), "/" + folderKey + "/" + fileName);
    }

    private Path fullPath(String fileKey) {
      return new Path(new Path(fs.getUri()), "/" + fileKey);
    }
  }

  private static final String TRAILING_PERIOD_PLACEHOLDER = "[[.]]";
  private static final Pattern TRAILING_PERIOD_PLACEHOLDER_PATTERN =
      Pattern.compile("\\[\\[\\.\\]\\](?=$|/)");
  private static final Pattern TRAILING_PERIOD_PATTERN = Pattern.compile("\\.(?=$|/)");

  @Override
  public String getScheme() {
    return "wasb";
  }

  
  /**
   * <p>
   * A {@link FileSystem} for reading and writing files stored on <a
   * href="http://store.azure.com/">Windows Azure</a>. This implementation is
   * blob-based and stores files on Azure in their native form so they can be read
   * by other Azure tools. This implementation uses HTTPS for secure network communication.
   * </p>
   */
  public static class Secure extends NativeAzureFileSystem {
    @Override
    public String getScheme() {
      return "wasbs";
    }
  }

  public static final Logger LOG = LoggerFactory.getLogger(NativeAzureFileSystem.class);

  static final String AZURE_BLOCK_SIZE_PROPERTY_NAME = "fs.azure.block.size";
  /**
   * The time span in seconds before which we consider a temp blob to be
   * dangling (not being actively uploaded to) and up for reclamation.
   * 
   * So e.g. if this is 60, then any temporary blobs more than a minute old
   * would be considered dangling.
   */
  static final String AZURE_TEMP_EXPIRY_PROPERTY_NAME = "fs.azure.fsck.temp.expiry.seconds";
  private static final int AZURE_TEMP_EXPIRY_DEFAULT = 3600;
  static final String PATH_DELIMITER = Path.SEPARATOR;
  static final String AZURE_TEMP_FOLDER = "_$azuretmpfolder$";

  private static final int AZURE_LIST_ALL = -1;
  private static final int AZURE_UNBOUNDED_DEPTH = -1;

  private static final long MAX_AZURE_BLOCK_SIZE = 512 * 1024 * 1024L;

  /**
   * The configuration property that determines which group owns files created
   * in WASB.
   */
  private static final String AZURE_DEFAULT_GROUP_PROPERTY_NAME = "fs.azure.permissions.supergroup";
  /**
   * The default value for fs.azure.permissions.supergroup. Chosen as the same
   * default as DFS.
   */
  static final String AZURE_DEFAULT_GROUP_DEFAULT = "supergroup";

  static final String AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME =
      "fs.azure.block.location.impersonatedhost";
  private static final String AZURE_BLOCK_LOCATION_HOST_DEFAULT =
      "localhost";
  static final String AZURE_RINGBUFFER_CAPACITY_PROPERTY_NAME =
      "fs.azure.ring.buffer.capacity";
  static final String AZURE_OUTPUT_STREAM_BUFFER_SIZE_PROPERTY_NAME =
      "fs.azure.output.stream.buffer.size";

  public static final String SKIP_AZURE_METRICS_PROPERTY_NAME = "fs.azure.skip.metrics";

  private class NativeAzureFsInputStream extends FSInputStream {
    private InputStream in;
    private final String key;
    private long pos = 0;
    private boolean closed = false;
    private boolean isPageBlob;

    // File length, valid only for streams over block blobs.
    private long fileLength;

    public NativeAzureFsInputStream(DataInputStream in, String key, long fileLength) {
      this.in = in;
      this.key = key;
      this.isPageBlob = store.isPageBlobKey(key);
      this.fileLength = fileLength;
    }

    /**
     * Return the size of the remaining available bytes
     * if the size is less than or equal to {@link Integer#MAX_VALUE},
     * otherwise, return {@link Integer#MAX_VALUE}.
     *
     * This is to match the behavior of DFSInputStream.available(),
     * which some clients may rely on (HBase write-ahead log reading in
     * particular).
     */
    @Override
    public synchronized int available() throws IOException {
      if (isPageBlob) {
        return in.available();
      } else {
        if (closed) {
          throw new IOException("Stream closed");
        }
        final long remaining = this.fileLength - pos;
        return remaining <= Integer.MAX_VALUE ?
            (int) remaining : Integer.MAX_VALUE;
      }
    }

    /*
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an integer in the range 0 to 255. If no byte is available
     * because the end of the stream has been reached, the value -1 is returned.
     * This method blocks until input data is available, the end of the stream
     * is detected, or an exception is thrown.
     *
     * @returns int An integer corresponding to the byte read.
     */
    @Override
    public synchronized int read() throws FileNotFoundException, IOException {
      try {
        int result = 0;
        result = in.read();
        if (result != -1) {
          pos++;
          if (statistics != null) {
            statistics.incrementBytesRead(1);
          }
        }
      // Return to the caller with the result.
      //
        return result;
      } catch(IOException e) {

        Throwable innerException = checkForAzureStorageException(e);

        if (innerException instanceof StorageException) {

          LOG.error("Encountered Storage Exception for read on Blob : {}"
              + " Exception details: {} Error Code : {}",
              key, e, ((StorageException) innerException).getErrorCode());

          if (isFileNotFoundException((StorageException) innerException)) {
            throw new FileNotFoundException(String.format("%s is not found", key));
          }
        }

       throw e;
      }
    }

    /*
     * Reads up to len bytes of data from the input stream into an array of
     * bytes. An attempt is made to read as many as len bytes, but a smaller
     * number may be read. The number of bytes actually read is returned as an
     * integer. This method blocks until input data is available, end of file is
     * detected, or an exception is thrown. If len is zero, then no bytes are
     * read and 0 is returned; otherwise, there is an attempt to read at least
     * one byte. If no byte is available because the stream is at end of file,
     * the value -1 is returned; otherwise, at least one byte is read and stored
     * into b.
     *
     * @param b -- the buffer into which data is read
     *
     * @param off -- the start offset in the array b at which data is written
     *
     * @param len -- the maximum number of bytes read
     *
     * @ returns int The total number of byes read into the buffer, or -1 if
     * there is no more data because the end of stream is reached.
     */
    @Override
    public synchronized int read(byte[] b, int off, int len) throws FileNotFoundException, IOException {
      try {
        int result = 0;
        result = in.read(b, off, len);
        if (result > 0) {
          pos += result;
        }

        if (null != statistics) {
          statistics.incrementBytesRead(result);
        }

        // Return to the caller with the result.
        return result;
      } catch(IOException e) {

        Throwable innerException = checkForAzureStorageException(e);

        if (innerException instanceof StorageException) {

          LOG.error("Encountered Storage Exception for read on Blob : {}"
              + " Exception details: {} Error Code : {}",
              key, e, ((StorageException) innerException).getErrorCode());

          if (isFileNotFoundException((StorageException) innerException)) {
            throw new FileNotFoundException(String.format("%s is not found", key));
          }
        }

       throw e;
      }
    }

    @Override
    public synchronized void close() throws IOException {
      if (!closed) {
        closed = true;
        IOUtils.closeStream(in);
        in = null;
      }
    }

    @Override
    public synchronized void seek(long pos) throws FileNotFoundException, EOFException, IOException {
      try {
        checkNotClosed();
        if (pos < 0) {
          throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        IOUtils.closeStream(in);
        in = store.retrieve(key);
        this.pos = in.skip(pos);
        LOG.debug("Seek to position {}. Bytes skipped {}", pos,
          this.pos);
      } catch(IOException e) {

        Throwable innerException = checkForAzureStorageException(e);

        if (innerException instanceof StorageException
             && isFileNotFoundException((StorageException) innerException)) {
          throw new FileNotFoundException(String.format("%s is not found", key));
        }

        throw e;
      }
    }

    @Override
    public synchronized long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    /*
     * Helper method to recursively check if the cause of the exception is
     * a Azure storage exception.
     */
    private Throwable checkForAzureStorageException(IOException e) {

      Throwable innerException = e.getCause();

      while (innerException != null
              && !(innerException instanceof StorageException)) {
        innerException = innerException.getCause();
      }

      return innerException;
    }

    /*
     * Helper method to check if the AzureStorageException is
     * because backing blob was not found.
     */
    private boolean isFileNotFoundException(StorageException e) {

      String errorCode = ((StorageException) e).getErrorCode();
      if (errorCode != null
          && (errorCode.equals(StorageErrorCodeStrings.BLOB_NOT_FOUND)
              || errorCode.equals(StorageErrorCodeStrings.RESOURCE_NOT_FOUND)
              || errorCode.equals(StorageErrorCode.BLOB_NOT_FOUND.toString())
              || errorCode.equals(StorageErrorCode.RESOURCE_NOT_FOUND.toString()))) {

        return true;
      }

      return false;
    }

    /*
     * Helper method to check if a stream is closed.
     */
    private void checkNotClosed() throws IOException {
      if (closed) {
        throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
      }
    }
  }

  private class NativeAzureFsOutputStream extends OutputStream {
    // We should not override flush() to actually close current block and flush
    // to DFS, this will break applications that assume flush() is a no-op.
    // Applications are advised to use Syncable.hflush() for that purpose.
    // NativeAzureFsOutputStream needs to implement Syncable if needed.
    private String key;
    private String keyEncoded;
    private OutputStream out;

    public NativeAzureFsOutputStream(OutputStream out, String aKey,
        String anEncodedKey) throws IOException {
      // Check input arguments. The output stream should be non-null and the
      // keys
      // should be valid strings.
      if (null == out) {
        throw new IllegalArgumentException(
            "Illegal argument: the output stream is null.");
      }

      if (null == aKey || 0 == aKey.length()) {
        throw new IllegalArgumentException(
            "Illegal argument the key string is null or empty");
      }

      if (null == anEncodedKey || 0 == anEncodedKey.length()) {
        throw new IllegalArgumentException(
            "Illegal argument the encoded key string is null or empty");
      }

      // Initialize the member variables with the incoming parameters.
      this.out = out;

      setKey(aKey);
      setEncodedKey(anEncodedKey);
    }

    @Override
    public synchronized void close() throws IOException {
      if (out != null) {
        // Close the output stream and decode the key for the output stream
        // before returning to the caller.
        //
        out.close();
        restoreKey();
        out = null;
      }
    }

    /**
     * Writes the specified byte to this output stream. The general contract for
     * write is that one byte is written to the output stream. The byte to be
     * written is the eight low-order bits of the argument b. The 24 high-order
     * bits of b are ignored.
     * 
     * @param b
     *          32-bit integer of block of 4 bytes
     */
    @Override
    public void write(int b) throws IOException {
      try {
        out.write(b);
      } catch(IOException e) {
        if (e.getCause() instanceof StorageException) {
          StorageException storageExcp  = (StorageException) e.getCause();
          LOG.error("Encountered Storage Exception for write on Blob : {}"
              + " Exception details: {} Error Code : {}",
              key, e.getMessage(), storageExcp.getErrorCode());
        }
        throw e;
      }
    }

    /**
     * Writes b.length bytes from the specified byte array to this output
     * stream. The general contract for write(b) is that it should have exactly
     * the same effect as the call write(b, 0, b.length).
     * 
     * @param b
     *          Block of bytes to be written to the output stream.
     */
    @Override
    public void write(byte[] b) throws IOException {
      try {
        out.write(b);
      } catch(IOException e) {
        if (e.getCause() instanceof StorageException) {
          StorageException storageExcp  = (StorageException) e.getCause();
          LOG.error("Encountered Storage Exception for write on Blob : {}"
              + " Exception details: {} Error Code : {}",
              key, e.getMessage(), storageExcp.getErrorCode());
        }
        throw e;
      }
    }

    /**
     * Writes <code>len</code> from the specified byte array starting at offset
     * <code>off</code> to the output stream. The general contract for write(b,
     * off, len) is that some of the bytes in the array <code>
     * b</code b> are written to the output stream in order; element
     * <code>b[off]</code> is the first byte written and
     * <code>b[off+len-1]</code> is the last byte written by this operation.
     * 
     * @param b
     *          Byte array to be written.
     * @param off
     *          Write this offset in stream.
     * @param len
     *          Number of bytes to be written.
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      try {
        out.write(b, off, len);
      } catch(IOException e) {
        if (e.getCause() instanceof StorageException) {
          StorageException storageExcp  = (StorageException) e.getCause();
          LOG.error("Encountered Storage Exception for write on Blob : {}"
              + " Exception details: {} Error Code : {}",
              key, e.getMessage(), storageExcp.getErrorCode());
        }
        throw e;
      }
    }

    /**
     * Get the blob name.
     * 
     * @return String Blob name.
     */
    public String getKey() {
      return key;
    }

    /**
     * Set the blob name.
     * 
     * @param key
     *          Blob name.
     */
    public void setKey(String key) {
      this.key = key;
    }

    /**
     * Get the blob name.
     * 
     * @return String Blob name.
     */
    public String getEncodedKey() {
      return keyEncoded;
    }

    /**
     * Set the blob name.
     * 
     * @param anEncodedKey
     *          Blob name.
     */
    public void setEncodedKey(String anEncodedKey) {
      this.keyEncoded = anEncodedKey;
    }

    /**
     * Restore the original key name from the m_key member variable. Note: The
     * output file stream is created with an encoded blob store key to guarantee
     * load balancing on the front end of the Azure storage partition servers.
     * The create also includes the name of the original key value which is
     * stored in the m_key member variable. This method should only be called
     * when the stream is closed.
     */
    private void restoreKey() throws IOException {
      store.rename(getEncodedKey(), getKey());
    }
  }

  private URI uri;
  private NativeFileSystemStore store;
  private AzureNativeFileSystemStore actualStore;
  private Path workingDir;
  private long blockSize = MAX_AZURE_BLOCK_SIZE;
  private AzureFileSystemInstrumentation instrumentation;
  private String metricsSourceName;
  private boolean isClosed = false;
  private static boolean suppressRetryPolicy = false;
  // A counter to create unique (within-process) names for my metrics sources.
  private static AtomicInteger metricsSourceNameCounter = new AtomicInteger();

  
  public NativeAzureFileSystem() {
    // set store in initialize()
  }

  public NativeAzureFileSystem(NativeFileSystemStore store) {
    this.store = store;
  }

  /**
   * Suppress the default retry policy for the Storage, useful in unit tests to
   * test negative cases without waiting forever.
   */
  @VisibleForTesting
  static void suppressRetryPolicy() {
    suppressRetryPolicy = true;
  }

  /**
   * Undo the effect of suppressRetryPolicy.
   */
  @VisibleForTesting
  static void resumeRetryPolicy() {
    suppressRetryPolicy = false;
  }

  /**
   * Creates a new metrics source name that's unique within this process.
   */
  @VisibleForTesting
  public static String newMetricsSourceName() {
    int number = metricsSourceNameCounter.incrementAndGet();
    final String baseName = "AzureFileSystemMetrics";
    if (number == 1) { // No need for a suffix for the first one
      return baseName;
    } else {
      return baseName + number;
    }
  }
  
  /**
   * Checks if the given URI scheme is a scheme that's affiliated with the Azure
   * File System.
   * 
   * @param scheme
   *          The URI scheme.
   * @return true iff it's an Azure File System URI scheme.
   */
  private static boolean isWasbScheme(String scheme) {
    // The valid schemes are: asv (old name), asvs (old name over HTTPS),
    // wasb (new name), wasbs (new name over HTTPS).
    return scheme != null
        && (scheme.equalsIgnoreCase("asv") || scheme.equalsIgnoreCase("asvs")
            || scheme.equalsIgnoreCase("wasb") || scheme
              .equalsIgnoreCase("wasbs"));
  }

  /**
   * Puts in the authority of the default file system if it is a WASB file
   * system and the given URI's authority is null.
   * 
   * @return The URI with reconstructed authority if necessary and possible.
   */
  private static URI reconstructAuthorityIfNeeded(URI uri, Configuration conf) {
    if (null == uri.getAuthority()) {
      // If WASB is the default file system, get the authority from there
      URI defaultUri = FileSystem.getDefaultUri(conf);
      if (defaultUri != null && isWasbScheme(defaultUri.getScheme())) {
        try {
          // Reconstruct the URI with the authority from the default URI.
          return new URI(uri.getScheme(), defaultUri.getAuthority(),
              uri.getPath(), uri.getQuery(), uri.getFragment());
        } catch (URISyntaxException e) {
          // This should never happen.
          throw new Error("Bad URI construction", e);
        }
      }
    }
    return uri;
  }

  @Override
  protected void checkPath(Path path) {
    // Make sure to reconstruct the path's authority if needed
    super.checkPath(new Path(reconstructAuthorityIfNeeded(path.toUri(),
        getConf())));
  }

  @Override
  public void initialize(URI uri, Configuration conf)
      throws IOException, IllegalArgumentException {
    // Check authority for the URI to guarantee that it is non-null.
    uri = reconstructAuthorityIfNeeded(uri, conf);
    if (null == uri.getAuthority()) {
      final String errMsg = String
          .format("Cannot initialize WASB file system, URI authority not recognized.");
      throw new IllegalArgumentException(errMsg);
    }
    super.initialize(uri, conf);

    if (store == null) {
      store = createDefaultStore(conf);
    }

    instrumentation = new AzureFileSystemInstrumentation(conf);
    if(!conf.getBoolean(SKIP_AZURE_METRICS_PROPERTY_NAME, false)) {
      // Make sure the metrics system is available before interacting with Azure
      AzureFileSystemMetricsSystem.fileSystemStarted();
      metricsSourceName = newMetricsSourceName();
      String sourceDesc = "Azure Storage Volume File System metrics";
      AzureFileSystemMetricsSystem.registerSource(metricsSourceName, sourceDesc,
        instrumentation);
    }

    store.initialize(uri, conf, instrumentation);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = new Path("/user", UserGroupInformation.getCurrentUser()
        .getShortUserName()).makeQualified(getUri(), getWorkingDirectory());
    this.blockSize = conf.getLong(AZURE_BLOCK_SIZE_PROPERTY_NAME,
        MAX_AZURE_BLOCK_SIZE);


    LOG.debug("NativeAzureFileSystem. Initializing.");
    LOG.debug("  blockSize  = {}",
        conf.getLong(AZURE_BLOCK_SIZE_PROPERTY_NAME, MAX_AZURE_BLOCK_SIZE));

  }

  private NativeFileSystemStore createDefaultStore(Configuration conf) {
    actualStore = new AzureNativeFileSystemStore();

    if (suppressRetryPolicy) {
      actualStore.suppressRetryPolicy();
    }
    return actualStore;
  }

  /**
   * Azure Storage doesn't allow the blob names to end in a period,
   * so encode this here to work around that limitation.
   */
  private static String encodeTrailingPeriod(String toEncode) {
    Matcher matcher = TRAILING_PERIOD_PATTERN.matcher(toEncode);
    return matcher.replaceAll(TRAILING_PERIOD_PLACEHOLDER);
  }

  /**
   * Reverse the encoding done by encodeTrailingPeriod().
   */
  private static String decodeTrailingPeriod(String toDecode) {
    Matcher matcher = TRAILING_PERIOD_PLACEHOLDER_PATTERN.matcher(toDecode);
    return matcher.replaceAll(".");
  }

  /**
   * Convert the path to a key. By convention, any leading or trailing slash is
   * removed, except for the special case of a single slash.
   */
  @VisibleForTesting
  public String pathToKey(Path path) {
    // Convert the path to a URI to parse the scheme, the authority, and the
    // path from the path object.
    URI tmpUri = path.toUri();
    String pathUri = tmpUri.getPath();

    // The scheme and authority is valid. If the path does not exist add a "/"
    // separator to list the root of the container.
    Path newPath = path;
    if ("".equals(pathUri)) {
      newPath = new Path(tmpUri.toString() + Path.SEPARATOR);
    }

    // Verify path is absolute if the path refers to a windows drive scheme.
    if (!newPath.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }

    String key = null;
    key = newPath.toUri().getPath();
    key = removeTrailingSlash(key);
    key = encodeTrailingPeriod(key);
    if (key.length() == 1) {
      return key;
    } else {
      return key.substring(1); // remove initial slash
    }
  }

  // Remove any trailing slash except for the case of a single slash.
  private static String removeTrailingSlash(String key) {
    if (key.length() == 0 || key.length() == 1) {
      return key;
    }
    if (key.charAt(key.length() - 1) == '/') {
      return key.substring(0, key.length() - 1);
    } else {
      return key;
    }
  }

  private static Path keyToPath(String key) {
    if (key.equals("/")) {
      return new Path("/"); // container
    }
    return new Path("/" + decodeTrailingPeriod(key));
  }

  /**
   * Get the absolute version of the path (fully qualified).
   * This is public for testing purposes.
   *
   * @param path
   * @return fully qualified path
   */
  @VisibleForTesting
  public Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  /**
   * For unit test purposes, retrieves the AzureNativeFileSystemStore store
   * backing this file system.
   * 
   * @return The store object.
   */
  @VisibleForTesting
  public AzureNativeFileSystemStore getStore() {
    return actualStore;
  }
  
  NativeFileSystemStore getStoreInterface() {
    return store;
  }

  /**
   * Gets the metrics source for this file system.
   * This is mainly here for unit testing purposes.
   *
   * @return the metrics source.
   */
  public AzureFileSystemInstrumentation getInstrumentation() {
    return instrumentation;
  }

  /** This optional operation is not yet supported. */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return create(f, permission, overwrite, true,
        bufferSize, replication, blockSize, progress,
        (SelfRenewingLease) null);
  }

  /**
   * Get a self-renewing lease on the specified file.
   */
  public SelfRenewingLease acquireLease(Path path) throws AzureException {
    String fullKey = pathToKey(makeAbsolute(path));
    return getStore().acquireLease(fullKey);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {

    Path parent = f.getParent();

    // Get exclusive access to folder if this is a directory designated
    // for atomic rename. The primary use case of for HBase write-ahead
    // log file management.
    SelfRenewingLease lease = null;
    if (store.isAtomicRenameKey(pathToKey(f))) {
      try {
        lease = acquireLease(parent);
      } catch (AzureException e) {

        String errorCode = "";
        try {
          StorageException e2 = (StorageException) e.getCause();
          errorCode = e2.getErrorCode();
        } catch (Exception e3) {
          // do nothing if cast fails
        }
        if (errorCode.equals("BlobNotFound")) {
          throw new FileNotFoundException("Cannot create file " +
              f.getName() + " because parent folder does not exist.");
        }

        LOG.warn("Got unexpected exception trying to get lease on {} . {}",
          pathToKey(parent), e.getMessage());
        throw e;
      }
    }

    // See if the parent folder exists. If not, throw error.
    // The exists() check will push any pending rename operation forward,
    // if there is one, and return false.
    //
    // At this point, we have exclusive access to the source folder
    // via the lease, so we will not conflict with an active folder
    // rename operation.
    if (!exists(parent)) {
      try {

        // This'll let the keep-alive thread exit as soon as it wakes up.
        lease.free();
      } catch (Exception e) {
        LOG.warn("Unable to free lease because: {}", e.getMessage());
      }
      throw new FileNotFoundException("Cannot create file " +
          f.getName() + " because parent folder does not exist.");
    }

    // Create file inside folder.
    FSDataOutputStream out = null;
    try {
      out = create(f, permission, overwrite, false,
          bufferSize, replication, blockSize, progress, lease);
    } finally {
      // Release exclusive access to folder.
      try {
        if (lease != null) {
          lease.free();
        }
      } catch (Exception e) {
        NativeAzureFileSystem.cleanup(LOG, out);
        String msg = "Unable to free lease on " + parent.toUri();
        LOG.error(msg);
        throw new IOException(msg, e);
      }
    }
    return out;
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {

    // Check if file should be appended or overwritten. Assume that the file
    // is overwritten on if the CREATE and OVERWRITE create flags are set. Note
    // that any other combinations of create flags will result in an open new or
    // open with append.
    final EnumSet<CreateFlag> createflags =
        EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
    boolean overwrite = flags.containsAll(createflags);

    // Delegate the create non-recursive call.
    return this.createNonRecursive(f, permission, overwrite,
        bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return this.createNonRecursive(f, FsPermission.getFileDefault(),
        overwrite, bufferSize, replication, blockSize, progress);
  }


  /**
   * Create an Azure blob and return an output stream to use
   * to write data to it.
   *
   * @param f
   * @param permission
   * @param overwrite
   * @param createParent
   * @param bufferSize
   * @param replication
   * @param blockSize
   * @param progress
   * @param parentFolderLease Lease on parent folder (or null if
   * no lease).
   * @return
   * @throws IOException
   */
  private FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, boolean createParent, int bufferSize,
      short replication, long blockSize, Progressable progress,
      SelfRenewingLease parentFolderLease)
          throws IOException {

    LOG.debug("Creating file: {}", f.toString());

    if (containsColon(f)) {
      throw new IOException("Cannot create file " + f
          + " through WASB that has colons in the name");
    }

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    FileMetadata existingMetadata = store.retrieveMetadata(key);
    if (existingMetadata != null) {
      if (existingMetadata.isDir()) {
        throw new IOException("Cannot create file " + f
            + "; already exists as a directory.");
      }
      if (!overwrite) {
        throw new IOException("File already exists:" + f);
      }
    }

    Path parentFolder = absolutePath.getParent();
    if (parentFolder != null && parentFolder.getParent() != null) { // skip root
      // Update the parent folder last modified time if the parent folder
      // already exists.
      String parentKey = pathToKey(parentFolder);
      FileMetadata parentMetadata = store.retrieveMetadata(parentKey);
      if (parentMetadata != null && parentMetadata.isDir() &&
        parentMetadata.getBlobMaterialization() == BlobMaterialization.Explicit) {
        if (parentFolderLease != null) {
          store.updateFolderLastModifiedTime(parentKey, parentFolderLease);
        } else {
          updateParentFolderLastModifiedTime(key);
        }
      } else {
        // Make sure that the parent folder exists.
        // Create it using inherited permissions from the first existing directory going up the path
        Path firstExisting = parentFolder.getParent();
        FileMetadata metadata = store.retrieveMetadata(pathToKey(firstExisting));
        while(metadata == null) {
          // Guaranteed to terminate properly because we will eventually hit root, which will return non-null metadata
          firstExisting = firstExisting.getParent();
          metadata = store.retrieveMetadata(pathToKey(firstExisting));
        }
        mkdirs(parentFolder, metadata.getPermissionStatus().getPermission(), true);
      }
    }

    // Mask the permission first (with the default permission mask as well).
    FsPermission masked = applyUMask(permission, UMaskApplyMode.NewFile);
    PermissionStatus permissionStatus = createPermissionStatus(masked);

    OutputStream bufOutStream;
    if (store.isPageBlobKey(key)) {
      // Store page blobs directly in-place without renames.
      bufOutStream = store.storefile(key, permissionStatus);
    } else {
      // This is a block blob, so open the output blob stream based on the
      // encoded key.
      //
      String keyEncoded = encodeKey(key);


      // First create a blob at the real key, pointing back to the temporary file
      // This accomplishes a few things:
      // 1. Makes sure we can create a file there.
      // 2. Makes it visible to other concurrent threads/processes/nodes what
      // we're
      // doing.
      // 3. Makes it easier to restore/cleanup data in the event of us crashing.
      store.storeEmptyLinkFile(key, keyEncoded, permissionStatus);

      // The key is encoded to point to a common container at the storage server.
      // This reduces the number of splits on the server side when load balancing.
      // Ingress to Azure storage can take advantage of earlier splits. We remove
      // the root path to the key and prefix a random GUID to the tail (or leaf
      // filename) of the key. Keys are thus broadly and randomly distributed over
      // a single container to ease load balancing on the storage server. When the
      // blob is committed it is renamed to its earlier key. Uncommitted blocks
      // are not cleaned up and we leave it to Azure storage to garbage collect
      // these
      // blocks.
      bufOutStream = new NativeAzureFsOutputStream(store.storefile(
          keyEncoded, permissionStatus), key, keyEncoded);
    }
    // Construct the data output stream from the buffered output stream.
    FSDataOutputStream fsOut = new FSDataOutputStream(bufOutStream, statistics);

    
    // Increment the counter
    instrumentation.fileCreated();
    
    // Return data output stream to caller.
    return fsOut;
  }

  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return delete(f, recursive, false);
  }

  /**
   * Delete the specified file or folder. The parameter
   * skipParentFolderLastModifidedTimeUpdate
   * is used in the case of atomic folder rename redo. In that case, there is
   * a lease on the parent folder, so (without reworking the code) modifying
   * the parent folder update time will fail because of a conflict with the
   * lease. Since we are going to delete the folder soon anyway so accurate
   * modified time is not necessary, it's easier to just skip
   * the modified time update.
   *
   * @param f
   * @param recursive
   * @param skipParentFolderLastModifidedTimeUpdate If true, don't update the folder last
   * modified time.
   * @return true if and only if the file is deleted
   * @throws IOException
   */
  public boolean delete(Path f, boolean recursive,
      boolean skipParentFolderLastModifidedTimeUpdate) throws IOException {

    LOG.debug("Deleting file: {}", f.toString());

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    // Capture the metadata for the path.
    //
    FileMetadata metaFile = store.retrieveMetadata(key);

    if (null == metaFile) {
      // The path to be deleted does not exist.
      return false;
    }

    // The path exists, determine if it is a folder containing objects,
    // an empty folder, or a simple file and take the appropriate actions.
    if (!metaFile.isDir()) {
      // The path specifies a file. We need to check the parent path
      // to make sure it's a proper materialized directory before we
      // delete the file. Otherwise we may get into a situation where
      // the file we were deleting was the last one in an implicit directory
      // (e.g. the blob store only contains the blob a/b and there's no
      // corresponding directory blob a) and that would implicitly delete
      // the directory as well, which is not correct.
      Path parentPath = absolutePath.getParent();
      if (parentPath.getParent() != null) {// Not root
        String parentKey = pathToKey(parentPath);
        FileMetadata parentMetadata = store.retrieveMetadata(parentKey);
        if (!parentMetadata.isDir()) {
          // Invalid state: the parent path is actually a file. Throw.
          throw new AzureException("File " + f + " has a parent directory "
              + parentPath + " which is also a file. Can't resolve.");
        }
        if (parentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
          LOG.debug("Found an implicit parent directory while trying to"
              + " delete the file {}. Creating the directory blob for"
              + " it in {}.", f, parentKey);

          store.storeEmptyFolder(parentKey,
              createPermissionStatus(FsPermission.getDefault()));
        } else {
          if (!skipParentFolderLastModifidedTimeUpdate) {
            updateParentFolderLastModifiedTime(key);
          }
        }
      }
      store.delete(key);
      instrumentation.fileDeleted();
    } else {
      // The path specifies a folder. Recursively delete all entries under the
      // folder.
      LOG.debug("Directory Delete encountered: {}", f.toString());
      Path parentPath = absolutePath.getParent();
      if (parentPath.getParent() != null) {
        String parentKey = pathToKey(parentPath);
        FileMetadata parentMetadata = store.retrieveMetadata(parentKey);

        if (parentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
          LOG.debug("Found an implicit parent directory while trying to"
              + " delete the directory {}. Creating the directory blob for"
              + " it in {}. ", f, parentKey);

          store.storeEmptyFolder(parentKey,
              createPermissionStatus(FsPermission.getDefault()));
        }
      }

      // List all the blobs in the current folder.
      String priorLastKey = null;
      PartialListing listing = store.listAll(key, AZURE_LIST_ALL, 1,
          priorLastKey);
      FileMetadata[] contents = listing.getFiles();
      if (!recursive && contents.length > 0) {
        // The folder is non-empty and recursive delete was not specified.
        // Throw an exception indicating that a non-recursive delete was
        // specified for a non-empty folder.
        throw new IOException("Non-recursive delete of non-empty directory "
            + f.toString());
      }

      // Delete all the files in the folder.
      for (FileMetadata p : contents) {
        // Tag on the directory name found as the suffix of the suffix of the
        // parent directory to get the new absolute path.
        String suffix = p.getKey().substring(
            p.getKey().lastIndexOf(PATH_DELIMITER));
        if (!p.isDir()) {
          store.delete(key + suffix);
          instrumentation.fileDeleted();
        } else {
          // Recursively delete contents of the sub-folders. Notice this also
          // deletes the blob for the directory.
          if (!delete(new Path(f.toString() + suffix), true)) {
            return false;
          }
        }
      }
      store.delete(key);

      // Update parent directory last modified time
      Path parent = absolutePath.getParent();
      if (parent != null && parent.getParent() != null) { // not root
        if (!skipParentFolderLastModifidedTimeUpdate) {
          updateParentFolderLastModifiedTime(key);
        }
      }
      instrumentation.directoryDeleted();
    }

    // File or directory was successfully deleted.
    LOG.debug("Delete Successful for : {}", f.toString());
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {

    LOG.debug("Getting the file status for {}", f.toString());

    // Capture the absolute path and the path to key.
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    if (key.length() == 0) { // root always exists
      return newDirectory(null, absolutePath);
    }

    // The path is either a folder or a file. Retrieve metadata to
    // determine if it is a directory or file.
    FileMetadata meta = store.retrieveMetadata(key);
    if (meta != null) {
      if (meta.isDir()) {
        // The path is a folder with files in it.
        //

        LOG.debug("Path {} is a folder.", f.toString());

        // If a rename operation for the folder was pending, redo it.
        // Then the file does not exist, so signal that.
        if (conditionalRedoFolderRename(f)) {
          throw new FileNotFoundException(
              absolutePath + ": No such file or directory.");
        }

        // Return reference to the directory object.
        return newDirectory(meta, absolutePath);
      }

      // The path is a file.
      LOG.debug("Found the path: {} as a file.", f.toString());

      // Return with reference to a file object.
      return newFile(meta, absolutePath);
    }

    // File not found. Throw exception no such file or directory.
    //
    throw new FileNotFoundException(
        absolutePath + ": No such file or directory.");
  }

  // Return true if there is a rename pending and we redo it, otherwise false.
  private boolean conditionalRedoFolderRename(Path f) throws IOException {

    // Can't rename /, so return immediately in that case.
    if (f.getName().equals("")) {
      return false;
    }

    // Check if there is a -RenamePending.json file for this folder, and if so,
    // redo the rename.
    Path absoluteRenamePendingFile = renamePendingFilePath(f);
    if (exists(absoluteRenamePendingFile)) {
      FolderRenamePending pending =
          new FolderRenamePending(absoluteRenamePendingFile, this);
      pending.redo();
      return true;
    } else {
      return false;
    }
  }

  // Return the path name that would be used for rename of folder with path f.
  private Path renamePendingFilePath(Path f) {
    Path absPath = makeAbsolute(f);
    String key = pathToKey(absPath);
    key += "-RenamePending.json";
    return keyToPath(key);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * Retrieve the status of a given path if it is a file, or of all the
   * contained files if it is a directory.
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {

    LOG.debug("Listing status for {}", f.toString());

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    Set<FileStatus> status = new TreeSet<FileStatus>();
    FileMetadata meta = store.retrieveMetadata(key);

    if (meta != null) {
      if (!meta.isDir()) {

        LOG.debug("Found path as a file");

        return new FileStatus[] { newFile(meta, absolutePath) };
      }
      String partialKey = null;
      PartialListing listing = store.list(key, AZURE_LIST_ALL, 1, partialKey);

      // For any -RenamePending.json files in the listing,
      // push the rename forward.
      boolean renamed = conditionalRedoFolderRenames(listing);

      // If any renames were redone, get another listing,
      // since the current one may have changed due to the redo.
      if (renamed) {
        listing = store.list(key, AZURE_LIST_ALL, 1, partialKey);
      }

      for (FileMetadata fileMetadata : listing.getFiles()) {
        Path subpath = keyToPath(fileMetadata.getKey());

        // Test whether the metadata represents a file or directory and
        // add the appropriate metadata object.
        //
        // Note: There was a very old bug here where directories were added
        // to the status set as files flattening out recursive listings
        // using "-lsr" down the file system hierarchy.
        if (fileMetadata.isDir()) {
          // Make sure we hide the temp upload folder
          if (fileMetadata.getKey().equals(AZURE_TEMP_FOLDER)) {
            // Don't expose that.
            continue;
          }
          status.add(newDirectory(fileMetadata, subpath));
        } else {
          status.add(newFile(fileMetadata, subpath));
        }
      }

      LOG.debug("Found path as a directory with {}"
          + " files in it.", status.size());

    } else {
      // There is no metadata found for the path.
      LOG.debug("Did not find any metadata for path: {}", key);

      throw new FileNotFoundException("File" + f + " does not exist.");
    }

    return status.toArray(new FileStatus[0]);
  }

  // Redo any folder renames needed if there are rename pending files in the
  // directory listing. Return true if one or more redo operations were done.
  private boolean conditionalRedoFolderRenames(PartialListing listing)
      throws IllegalArgumentException, IOException {
    boolean renamed = false;
    for (FileMetadata fileMetadata : listing.getFiles()) {
      Path subpath = keyToPath(fileMetadata.getKey());
      if (isRenamePendingFile(subpath)) {
        FolderRenamePending pending =
            new FolderRenamePending(subpath, this);
        pending.redo();
        renamed = true;
      }
    }
    return renamed;
  }

  // True if this is a folder rename pending file, else false.
  private boolean isRenamePendingFile(Path path) {
    return path.toString().endsWith(FolderRenamePending.SUFFIX);
  }

  private FileStatus newFile(FileMetadata meta, Path path) {
    return new FileStatus (
        meta.getLength(),
        false,
        1,
        blockSize,
        meta.getLastModified(),
        0,
        meta.getPermissionStatus().getPermission(),
        meta.getPermissionStatus().getUserName(),
        meta.getPermissionStatus().getGroupName(),
        path.makeQualified(getUri(), getWorkingDirectory()));
  }

  private FileStatus newDirectory(FileMetadata meta, Path path) {
    return new FileStatus (
        0,
        true,
        1,
        blockSize,
        meta == null ? 0 : meta.getLastModified(),
        0,
        meta == null ? FsPermission.getDefault() : meta.getPermissionStatus().getPermission(),
        meta == null ? "" : meta.getPermissionStatus().getUserName(),
        meta == null ? "" : meta.getPermissionStatus().getGroupName(),
        path.makeQualified(getUri(), getWorkingDirectory()));
  }

  private static enum UMaskApplyMode {
    NewFile,
    NewDirectory,
    NewDirectoryNoUmask,
    ChangeExistingFile,
    ChangeExistingDirectory,
  }

  /**
   * Applies the applicable UMASK's on the given permission.
   * 
   * @param permission
   *          The permission to mask.
   * @param applyMode
   *          Whether to also apply the default umask.
   * @return The masked persmission.
   */
  private FsPermission applyUMask(final FsPermission permission,
      final UMaskApplyMode applyMode) {
    FsPermission newPermission = new FsPermission(permission);
    // Apply the default umask - this applies for new files or directories.
    if (applyMode == UMaskApplyMode.NewFile
        || applyMode == UMaskApplyMode.NewDirectory) {
      newPermission = newPermission
          .applyUMask(FsPermission.getUMask(getConf()));
    }
    return newPermission;
  }

  /**
   * Creates the PermissionStatus object to use for the given permission, based
   * on the current user in context.
   * 
   * @param permission
   *          The permission for the file.
   * @return The permission status object to use.
   * @throws IOException
   *           If login fails in getCurrentUser
   */
  @VisibleForTesting
  PermissionStatus createPermissionStatus(FsPermission permission)
    throws IOException {
    // Create the permission status for this file based on current user
    return new PermissionStatus(
        UserGroupInformation.getCurrentUser().getShortUserName(),
        getConf().get(AZURE_DEFAULT_GROUP_PROPERTY_NAME,
            AZURE_DEFAULT_GROUP_DEFAULT),
        permission);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return mkdirs(f, permission, false);
  }

  public boolean mkdirs(Path f, FsPermission permission, boolean noUmask) throws IOException {


    LOG.debug("Creating directory: {}", f.toString());

    if (containsColon(f)) {
      throw new IOException("Cannot create directory " + f
          + " through WASB that has colons in the name");
    }

    Path absolutePath = makeAbsolute(f);
    PermissionStatus permissionStatus = null;
    if(noUmask) {
      // ensure owner still has wx permissions at the minimum
      permissionStatus = createPermissionStatus(
          applyUMask(FsPermission.createImmutable((short) (permission.toShort() | USER_WX_PERMISION)),
              UMaskApplyMode.NewDirectoryNoUmask));
    } else {
      permissionStatus = createPermissionStatus(
          applyUMask(permission, UMaskApplyMode.NewDirectory));
    }


    ArrayList<String> keysToCreateAsFolder = new ArrayList<String>();
    ArrayList<String> keysToUpdateAsFolder = new ArrayList<String>();
    boolean childCreated = false;
    // Check that there is no file in the parent chain of the given path.
    for (Path current = absolutePath, parent = current.getParent();
        parent != null; // Stop when you get to the root
        current = parent, parent = current.getParent()) {
      String currentKey = pathToKey(current);
      FileMetadata currentMetadata = store.retrieveMetadata(currentKey);
      if (currentMetadata != null && !currentMetadata.isDir()) {
        throw new IOException("Cannot create directory " + f + " because " +
            current + " is an existing file.");
      } else if (currentMetadata == null) {
        keysToCreateAsFolder.add(currentKey);
        childCreated = true;
      } else {
        // The directory already exists. Its last modified time need to be
        // updated if there is a child directory created under it.
        if (childCreated) {
          keysToUpdateAsFolder.add(currentKey);
        }
        childCreated = false;
      }
    }

    for (String currentKey : keysToCreateAsFolder) {
      store.storeEmptyFolder(currentKey, permissionStatus);
    }

    instrumentation.directoryCreated();

    // otherwise throws exception
    return true;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {

    LOG.debug("Opening file: {}", f.toString());

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    FileMetadata meta = store.retrieveMetadata(key);
    if (meta == null) {
      throw new FileNotFoundException(f.toString());
    }
    if (meta.isDir()) {
      throw new FileNotFoundException(f.toString()
          + " is a directory not a file.");
    }

    return new FSDataInputStream(new BufferedFSInputStream(
        new NativeAzureFsInputStream(store.retrieve(key), key, meta.getLength()), bufferSize));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {

    FolderRenamePending renamePending = null;

    LOG.debug("Moving {} to {}", src, dst);

    if (containsColon(dst)) {
      throw new IOException("Cannot rename to file " + dst
          + " through WASB that has colons in the name");
    }

    String srcKey = pathToKey(makeAbsolute(src));

    if (srcKey.length() == 0) {
      // Cannot rename root of file system
      return false;
    }

    // Figure out the final destination
    Path absoluteDst = makeAbsolute(dst);
    String dstKey = pathToKey(absoluteDst);
    FileMetadata dstMetadata = store.retrieveMetadata(dstKey);
    if (dstMetadata != null && dstMetadata.isDir()) {
      // It's an existing directory.
      dstKey = pathToKey(makeAbsolute(new Path(dst, src.getName())));
      LOG.debug("Destination {} "
          + " is a directory, adjusted the destination to be {}", dst, dstKey);
    } else if (dstMetadata != null) {
      // Attempting to overwrite a file using rename()
      LOG.debug("Destination {}"
          + " is an already existing file, failing the rename.", dst);
      return false;
    } else {
      // Check that the parent directory exists.
      FileMetadata parentOfDestMetadata =
          store.retrieveMetadata(pathToKey(absoluteDst.getParent()));
      if (parentOfDestMetadata == null) {
        LOG.debug("Parent of the destination {}"
            + " doesn't exist, failing the rename.", dst);
        return false;
      } else if (!parentOfDestMetadata.isDir()) {
        LOG.debug("Parent of the destination {}"
            + " is a file, failing the rename.", dst);
        return false;
      }
    }
    FileMetadata srcMetadata = store.retrieveMetadata(srcKey);
    if (srcMetadata == null) {
      // Source doesn't exist
      LOG.debug("Source {} doesn't exist, failing the rename.", src);
      return false;
    } else if (!srcMetadata.isDir()) {
      LOG.debug("Source {} found as a file, renaming.", src);
      store.rename(srcKey, dstKey);
    } else {

      // Prepare for, execute and clean up after of all files in folder, and
      // the root file, and update the last modified time of the source and
      // target parent folders. The operation can be redone if it fails part
      // way through, by applying the "Rename Pending" file.

      // The following code (internally) only does atomic rename preparation
      // and lease management for page blob folders, limiting the scope of the
      // operation to HBase log file folders, where atomic rename is required.
      // In the future, we could generalize it easily to all folders.
      renamePending = prepareAtomicFolderRename(srcKey, dstKey);
      renamePending.execute();

      LOG.debug("Renamed {} to {} successfully.", src, dst);
      renamePending.cleanup();
      return true;
    }

    // Update the last-modified time of the parent folders of both source
    // and destination.
    updateParentFolderLastModifiedTime(srcKey);
    updateParentFolderLastModifiedTime(dstKey);

    LOG.debug("Renamed {} to {} successfully.", src, dst);
    return true;
  }

  /**
   * Update the last-modified time of the parent folder of the file
   * identified by key.
   * @param key
   * @throws IOException
   */
  private void updateParentFolderLastModifiedTime(String key)
      throws IOException {
    Path parent = makeAbsolute(keyToPath(key)).getParent();
    if (parent != null && parent.getParent() != null) { // not root
      String parentKey = pathToKey(parent);

      // ensure the parent is a materialized folder
      FileMetadata parentMetadata = store.retrieveMetadata(parentKey);
      // The metadata could be null if the implicit folder only contains a
      // single file. In this case, the parent folder no longer exists if the
      // file is renamed; so we can safely ignore the null pointer case.
      if (parentMetadata != null) {
        if (parentMetadata.isDir()
            && parentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
          store.storeEmptyFolder(parentKey,
              createPermissionStatus(FsPermission.getDefault()));
        }

        if (store.isAtomicRenameKey(parentKey)) {
          SelfRenewingLease lease = null;
          try {
            lease = leaseSourceFolder(parentKey);
            store.updateFolderLastModifiedTime(parentKey, lease);
          } catch (AzureException e) {
            String errorCode = "";
            try {
              StorageException e2 = (StorageException) e.getCause();
              errorCode = e2.getErrorCode();
            } catch (Exception e3) {
              // do nothing if cast fails
            }
            if (errorCode.equals("BlobNotFound")) {
              throw new FileNotFoundException("Folder does not exist: " + parentKey);
            }
            LOG.warn("Got unexpected exception trying to get lease on {}. {}",
                parentKey, e.getMessage());
            throw e;
          } finally {
            try {
              if (lease != null) {
                lease.free();
              }
            } catch (Exception e) {
              LOG.error("Unable to free lease on {}", parentKey, e);
            }
          }
        } else {
          store.updateFolderLastModifiedTime(parentKey, null);
        }
      }
    }
  }

  /**
   * If the source is a page blob folder,
   * prepare to rename this folder atomically. This means to get exclusive
   * access to the source folder, and record the actions to be performed for
   * this rename in a "Rename Pending" file. This code was designed to
   * meet the needs of HBase, which requires atomic rename of write-ahead log
   * (WAL) folders for correctness.
   *
   * Before calling this method, the caller must ensure that the source is a
   * folder.
   *
   * For non-page-blob directories, prepare the in-memory information needed,
   * but don't take the lease or write the redo file. This is done to limit the
   * scope of atomic folder rename to HBase, at least at the time of writing
   * this code.
   *
   * @param srcKey Source folder name.
   * @param dstKey Destination folder name.
   * @throws IOException
   */
  private FolderRenamePending prepareAtomicFolderRename(
      String srcKey, String dstKey) throws IOException {

    if (store.isAtomicRenameKey(srcKey)) {

      // Block unwanted concurrent access to source folder.
      SelfRenewingLease lease = leaseSourceFolder(srcKey);

      // Prepare in-memory information needed to do or redo a folder rename.
      FolderRenamePending renamePending =
          new FolderRenamePending(srcKey, dstKey, lease, this);

      // Save it to persistent storage to help recover if the operation fails.
      renamePending.writeFile(this);
      return renamePending;
    } else {
      FolderRenamePending renamePending =
          new FolderRenamePending(srcKey, dstKey, null, this);
      return renamePending;
    }
  }

  /**
   * Get a self-renewing Azure blob lease on the source folder zero-byte file.
   */
  private SelfRenewingLease leaseSourceFolder(String srcKey)
      throws AzureException {
    return store.acquireLease(srcKey);
  }

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file. For WASB we'll just lie and give
   * fake hosts to make sure we get many splits in MR jobs.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file,
      long start, long len) throws IOException {
    if (file == null) {
      return null;
    }

    if ((start < 0) || (len < 0)) {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }

    if (file.getLen() < start) {
      return new BlockLocation[0];
    }
    final String blobLocationHost = getConf().get(
        AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME,
        AZURE_BLOCK_LOCATION_HOST_DEFAULT);
    final String[] name = { blobLocationHost };
    final String[] host = { blobLocationHost };
    long blockSize = file.getBlockSize();
    if (blockSize <= 0) {
      throw new IllegalArgumentException(
          "The block size for the given file is not a positive number: "
              + blockSize);
    }
    int numberOfLocations = (int) (len / blockSize)
        + ((len % blockSize == 0) ? 0 : 1);
    BlockLocation[] locations = new BlockLocation[numberOfLocations];
    for (int i = 0; i < locations.length; i++) {
      long currentOffset = start + (i * blockSize);
      long currentLength = Math.min(blockSize, start + len - currentOffset);
      locations[i] = new BlockLocation(name, host, currentOffset, currentLength);
    }
    return locations;
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = makeAbsolute(newDir);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    Path absolutePath = makeAbsolute(p);
    String key = pathToKey(absolutePath);
    FileMetadata metadata = store.retrieveMetadata(key);
    if (metadata == null) {
      throw new FileNotFoundException("File doesn't exist: " + p);
    }
    permission = applyUMask(permission,
        metadata.isDir() ? UMaskApplyMode.ChangeExistingDirectory
            : UMaskApplyMode.ChangeExistingFile);
    if (metadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
      // It's an implicit folder, need to materialize it.
      store.storeEmptyFolder(key, createPermissionStatus(permission));
    } else if (!metadata.getPermissionStatus().getPermission().
        equals(permission)) {
      store.changePermissionStatus(key, new PermissionStatus(
          metadata.getPermissionStatus().getUserName(),
          metadata.getPermissionStatus().getGroupName(),
          permission));
    }
  }

  @Override
  public void setOwner(Path p, String username, String groupname)
      throws IOException {
    Path absolutePath = makeAbsolute(p);
    String key = pathToKey(absolutePath);
    FileMetadata metadata = store.retrieveMetadata(key);
    if (metadata == null) {
      throw new FileNotFoundException("File doesn't exist: " + p);
    }
    PermissionStatus newPermissionStatus = new PermissionStatus(
        username == null ?
            metadata.getPermissionStatus().getUserName() : username,
        groupname == null ?
            metadata.getPermissionStatus().getGroupName() : groupname,
        metadata.getPermissionStatus().getPermission());
    if (metadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
      // It's an implicit folder, need to materialize it.
      store.storeEmptyFolder(key, newPermissionStatus);
    } else {
      store.changePermissionStatus(key, newPermissionStatus);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (isClosed) {
      return;
    }

    // Call the base close() to close any resources there.
    super.close();
    // Close the store to close any resources there - e.g. the bandwidth
    // updater thread would be stopped at this time.
    store.close();
    // Notify the metrics system that this file system is closed, which may
    // trigger one final metrics push to get the accurate final file system
    // metrics out.

    long startTime = System.currentTimeMillis();

    if(!getConf().getBoolean(SKIP_AZURE_METRICS_PROPERTY_NAME, false)) {
      AzureFileSystemMetricsSystem.unregisterSource(metricsSourceName);
      AzureFileSystemMetricsSystem.fileSystemClosed();
    }

    LOG.debug("Submitting metrics when file system closed took {} ms.",
        (System.currentTimeMillis() - startTime));
    isClosed = true;
  }

  /**
   * A handler that defines what to do with blobs whose upload was
   * interrupted.
   */
  private abstract class DanglingFileHandler {
    abstract void handleFile(FileMetadata file, FileMetadata tempFile)
      throws IOException;
  }

  /**
   * Handler implementation for just deleting dangling files and cleaning
   * them up.
   */
  private class DanglingFileDeleter extends DanglingFileHandler {
    @Override
    void handleFile(FileMetadata file, FileMetadata tempFile)
        throws IOException {

      LOG.debug("Deleting dangling file {}", file.getKey());
      store.delete(file.getKey());
      store.delete(tempFile.getKey());
    }
  }

  /**
   * Handler implementation for just moving dangling files to recovery
   * location (/lost+found).
   */
  private class DanglingFileRecoverer extends DanglingFileHandler {
    private final Path destination;

    DanglingFileRecoverer(Path destination) {
      this.destination = destination;
    }

    @Override
    void handleFile(FileMetadata file, FileMetadata tempFile)
        throws IOException {

      LOG.debug("Recovering {}", file.getKey());
      // Move to the final destination
      String finalDestinationKey =
          pathToKey(new Path(destination, file.getKey()));
      store.rename(tempFile.getKey(), finalDestinationKey);
      if (!finalDestinationKey.equals(file.getKey())) {
        // Delete the empty link file now that we've restored it.
        store.delete(file.getKey());
      }
    }
  }

  /**
   * Check if a path has colons in its name
   */
  private boolean containsColon(Path p) {
    return p.toUri().getPath().toString().contains(":");
  }

  /**
   * Implements recover and delete (-move and -delete) behaviors for handling
   * dangling files (blobs whose upload was interrupted).
   * 
   * @param root
   *          The root path to check from.
   * @param handler
   *          The handler that deals with dangling files.
   */
  private void handleFilesWithDanglingTempData(Path root,
      DanglingFileHandler handler) throws IOException {
    // Calculate the cut-off for when to consider a blob to be dangling.
    long cutoffForDangling = new Date().getTime()
        - getConf().getInt(AZURE_TEMP_EXPIRY_PROPERTY_NAME,
            AZURE_TEMP_EXPIRY_DEFAULT) * 1000;
    // Go over all the blobs under the given root and look for blobs to
    // recover.
    String priorLastKey = null;
    do {
      PartialListing listing = store.listAll(pathToKey(root), AZURE_LIST_ALL,
          AZURE_UNBOUNDED_DEPTH, priorLastKey);

      for (FileMetadata file : listing.getFiles()) {
        if (!file.isDir()) { // We don't recover directory blobs
          // See if this blob has a link in it (meaning it's a place-holder
          // blob for when the upload to the temp blob is complete).
          String link = store.getLinkInFileMetadata(file.getKey());
          if (link != null) {
            // It has a link, see if the temp blob it is pointing to is
            // existent and old enough to be considered dangling.
            FileMetadata linkMetadata = store.retrieveMetadata(link);
            if (linkMetadata != null
                && linkMetadata.getLastModified() >= cutoffForDangling) {
              // Found one!
              handler.handleFile(file, linkMetadata);
            }
          }
        }
      }
      priorLastKey = listing.getPriorLastKey();
    } while (priorLastKey != null);
  }

  /**
   * Looks under the given root path for any blob that are left "dangling",
   * meaning that they are place-holder blobs that we created while we upload
   * the data to a temporary blob, but for some reason we crashed in the middle
   * of the upload and left them there. If any are found, we move them to the
   * destination given.
   * 
   * @param root
   *          The root path to consider.
   * @param destination
   *          The destination path to move any recovered files to.
   * @throws IOException
   */
  public void recoverFilesWithDanglingTempData(Path root, Path destination)
      throws IOException {

    LOG.debug("Recovering files with dangling temp data in {}", root);
    handleFilesWithDanglingTempData(root,
        new DanglingFileRecoverer(destination));
  }

  /**
   * Looks under the given root path for any blob that are left "dangling",
   * meaning that they are place-holder blobs that we created while we upload
   * the data to a temporary blob, but for some reason we crashed in the middle
   * of the upload and left them there. If any are found, we delete them.
   * 
   * @param root
   *          The root path to consider.
   * @throws IOException
   */
  public void deleteFilesWithDanglingTempData(Path root) throws IOException {

    LOG.debug("Deleting files with dangling temp data in {}", root);
    handleFilesWithDanglingTempData(root, new DanglingFileDeleter());
  }

  @Override
  protected void finalize() throws Throwable {
    LOG.debug("finalize() called.");
    close();
    super.finalize();
  }

  /**
   * Encode the key with a random prefix for load balancing in Azure storage.
   * Upload data to a random temporary file then do storage side renaming to
   * recover the original key.
   * 
   * @param aKey
   * @return Encoded version of the original key.
   */
  private static String encodeKey(String aKey) {
    // Get the tail end of the key name.
    //
    String fileName = aKey.substring(aKey.lastIndexOf(Path.SEPARATOR) + 1,
        aKey.length());

    // Construct the randomized prefix of the file name. The prefix ensures the
    // file always drops into the same folder but with a varying tail key name.
    String filePrefix = AZURE_TEMP_FOLDER + Path.SEPARATOR
        + UUID.randomUUID().toString();

    // Concatenate the randomized prefix with the tail of the key name.
    String randomizedKey = filePrefix + fileName;

    // Return to the caller with the randomized key.
    return randomizedKey;
  }

  private static void cleanup(Logger log, java.io.Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch(IOException e) {
        if (log != null) {
          log.debug("Exception in closing {}", closeable, e);
        }
      }
    }
  }
}
