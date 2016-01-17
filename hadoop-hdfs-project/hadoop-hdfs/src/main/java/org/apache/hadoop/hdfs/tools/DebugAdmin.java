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
package org.apache.hadoop.hdfs.tools;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

/**
 * This class implements debug operations on the HDFS command-line.
 *
 * These operations are only for debugging, and may change or disappear
 * between HDFS versions.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DebugAdmin extends Configured implements Tool {
  /**
   * All the debug commands we can run.
   */
  private DebugCommand DEBUG_COMMANDS[] = {
    new VerifyBlockChecksumCommand(),
    new RecoverLeaseCommand(),
    new HelpCommand()
  };

  /**
   * The base class for debug commands.
   */
  private abstract class DebugCommand {
    final String name;
    final String usageText;
    final String helpText;

    DebugCommand(String name, String usageText, String helpText) {
      this.name = name;
      this.usageText = usageText;
      this.helpText = helpText;
    }

    abstract int run(List<String> args) throws IOException;
  }

  private static int HEADER_LEN = 7;

  /**
   * The command for verifying a block metadata file and possibly block file.
   */
  private class VerifyBlockChecksumCommand extends DebugCommand {
    VerifyBlockChecksumCommand() {
      super("verify",
"verify [-meta <metadata-file>] [-block <block-file>]",
"  Verify HDFS metadata and block files.  If a block file is specified, we\n" +
"  will verify that the checksums in the metadata file match the block\n" +
"  file.");
    }

    int run(List<String> args) throws IOException {
      if (args.size() == 0) {
        System.out.println(usageText);
        System.out.println(helpText + "\n");
        return 1;
      }
      String blockFile = StringUtils.popOptionWithArgument("-block", args);
      String metaFile = StringUtils.popOptionWithArgument("-meta", args);
      if (metaFile == null) {
        System.err.println("You must specify a meta file with -meta");
        return 1;
      }

      FileInputStream metaStream = null, dataStream = null;
      FileChannel metaChannel = null, dataChannel = null;
      DataInputStream checksumStream = null;
      try {
        BlockMetadataHeader header;
        try {
          metaStream = new FileInputStream(metaFile);
          checksumStream = new DataInputStream(metaStream);
          header = BlockMetadataHeader.readHeader(checksumStream);
          metaChannel = metaStream.getChannel();
          metaChannel.position(HEADER_LEN);
        } catch (RuntimeException e) {
          System.err.println("Failed to read HDFS metadata file header for " +
              metaFile + ": " + StringUtils.stringifyException(e));
          return 1;
        } catch (IOException e) {
          System.err.println("Failed to read HDFS metadata file header for " +
              metaFile + ": " + StringUtils.stringifyException(e));
          return 1;
        }
        DataChecksum checksum = header.getChecksum();
        System.out.println("Checksum type: " + checksum.toString());
        if (blockFile == null) {
          return 0;
        }
        ByteBuffer metaBuf, dataBuf;
        try {
          dataStream = new FileInputStream(blockFile);
          dataChannel = dataStream.getChannel();
          final int CHECKSUMS_PER_BUF = 1024 * 32;
          metaBuf = ByteBuffer.allocate(checksum.
              getChecksumSize() * CHECKSUMS_PER_BUF);
          dataBuf = ByteBuffer.allocate(checksum.
              getBytesPerChecksum() * CHECKSUMS_PER_BUF);
        } catch (IOException e) {
          System.err.println("Failed to open HDFS block file for " +
              blockFile + ": " + StringUtils.stringifyException(e));
          return 1;
        }
        long offset = 0;
        while (true) {
          dataBuf.clear();
          int dataRead = -1;
          try {
            dataRead = dataChannel.read(dataBuf);
            if (dataRead < 0) {
              break;
            }
          } catch (IOException e) {
            System.err.println("Got I/O error reading block file " +
                blockFile + "from disk at offset " + dataChannel.position() +
                ": " + StringUtils.stringifyException(e));
            return 1;
          }
          try {
            int csumToRead =
                (((checksum.getBytesPerChecksum() - 1) + dataRead) /
                  checksum.getBytesPerChecksum()) *
                      checksum.getChecksumSize();
            metaBuf.clear();
            metaBuf.limit(csumToRead);
            metaChannel.read(metaBuf);
            dataBuf.flip();
            metaBuf.flip();
          } catch (IOException e) {
            System.err.println("Got I/O error reading metadata file " +
                metaFile + "from disk at offset " + metaChannel.position() +
                ": " +  StringUtils.stringifyException(e));
            return 1;
          }
          try {
            checksum.verifyChunkedSums(dataBuf, metaBuf,
                blockFile, offset);
          } catch (IOException e) {
            System.out.println("verifyChunkedSums error: " +
                StringUtils.stringifyException(e));
            return 1;
          }
          offset += dataRead;
        }
        System.out.println("Checksum verification succeeded on block file " +
            blockFile);
        return 0;
      } finally {
        IOUtils.cleanup(null, metaStream, dataStream, checksumStream);
      }
    }
  }

  /**
   * The command for recovering a file lease.
   */
  private class RecoverLeaseCommand extends DebugCommand {
    RecoverLeaseCommand() {
      super("recoverLease",
"recoverLease [-path <path>] [-retries <num-retries>]",
"  Recover the lease on the specified path.  The path must reside on an\n" +
"  HDFS filesystem.  The default number of retries is 1.");
    }

    private static final int TIMEOUT_MS = 5000;

    int run(List<String> args) throws IOException {
      if (args.size() == 0) {
        System.out.println(usageText);
        System.out.println(helpText + "\n");
        return 1;
      }
      String pathStr = StringUtils.popOptionWithArgument("-path", args);
      String retriesStr = StringUtils.popOptionWithArgument("-retries", args);
      if (pathStr == null) {
        System.err.println("You must supply a -path argument to " +
            "recoverLease.");
        return 1;
      }
      int maxRetries = 1;
      if (retriesStr != null) {
        try {
          maxRetries = Integer.parseInt(retriesStr);
        } catch (NumberFormatException e) {
          System.err.println("Failed to parse the argument to -retries: " +
              StringUtils.stringifyException(e));
          return 1;
        }
      }
      FileSystem fs;
      try {
        fs = FileSystem.newInstance(new URI(pathStr), getConf(), null);
      } catch (URISyntaxException e) {
        System.err.println("URISyntaxException for " + pathStr + ":" +
            StringUtils.stringifyException(e));
        return 1;
      } catch (InterruptedException e) {
        System.err.println("InterruptedException for " + pathStr + ":" +
            StringUtils.stringifyException(e));
        return 1;
      }
      DistributedFileSystem dfs = null;
      try {
        dfs = (DistributedFileSystem) fs;
      } catch (ClassCastException e) {
        System.err.println("Invalid filesystem for path " + pathStr + ": " +
            "needed scheme hdfs, but got: " + fs.getScheme());
        return 1;
      }
      for (int retry = 0; true; ) {
        boolean recovered = false;
        IOException ioe = null;
        try {
          recovered = dfs.recoverLease(new Path(pathStr));
        } catch (FileNotFoundException e) {
          System.err.println("recoverLease got exception: " + e.getMessage());
          System.err.println("Giving up on recoverLease for " + pathStr +
              " after 1 try");
          return 1;
        } catch (IOException e) {
          ioe = e;
        }
        if (recovered) {
          System.out.println("recoverLease SUCCEEDED on " + pathStr); 
          return 0;
        }
        if (ioe != null) {
          System.err.println("recoverLease got exception: " +
              ioe.getMessage());
        } else {
          System.err.println("recoverLease returned false.");
        }
        retry++;
        if (retry >= maxRetries) {
          break;
        }
        System.err.println("Retrying in " + TIMEOUT_MS + " ms...");
        Uninterruptibles.sleepUninterruptibly(TIMEOUT_MS,
            TimeUnit.MILLISECONDS);
        System.err.println("Retry #" + retry);
      }
      System.err.println("Giving up on recoverLease for " + pathStr + " after " +
          maxRetries + (maxRetries == 1 ? " try." : " tries."));
      return 1;
    }
  }

  /**
   * The command for getting help about other commands.
   */
  private class HelpCommand extends DebugCommand {
    HelpCommand() {
      super("help",
"help [command-name]",
"  Get help about a command.");
    }

    int run(List<String> args) {
      DebugCommand command = popCommand(args);
      if (command == null) {
        printUsage();
        return 0;
      }
      System.out.println(command.usageText);
      System.out.println(command.helpText + "\n");
      return 0;
    }
  }

  public DebugAdmin(Configuration conf) {
    super(conf);
  }

  private DebugCommand popCommand(List<String> args) {
    String commandStr = (args.size() == 0) ? "" : args.get(0);
    if (commandStr.startsWith("-")) {
      commandStr = commandStr.substring(1);
    }
    for (DebugCommand command : DEBUG_COMMANDS) {
      if (command.name.equals(commandStr)) {
        args.remove(0);
        return command;
      }
    }
    return null;
  }

  public int run(String[] argv) {
    LinkedList<String> args = new LinkedList<String>();
    for (int j = 0; j < argv.length; ++j) {
      args.add(argv[j]);
    }
    DebugCommand command = popCommand(args);
    if (command == null) {
      printUsage();
      return 0;
    }
    try {
      return command.run(args);
    } catch (IOException e) {
      System.err.println("IOException: " +
          StringUtils.stringifyException(e));
      return 1;
    } catch (RuntimeException e) {
      System.err.println("RuntimeException: " +
          StringUtils.stringifyException(e));
      return 1;
    }
  }

  private void printUsage() {
    System.out.println("Usage: hdfs debug <command> [arguments]\n");
    for (DebugCommand command : DEBUG_COMMANDS) {
      if (!command.name.equals("help")) {
        System.out.println(command.usageText);
      }
    }
  }

  public static void main(String[] argsArray) throws IOException {
    DebugAdmin debugAdmin = new DebugAdmin(new Configuration());
    System.exit(debugAdmin.run(argsArray));
  }
}
