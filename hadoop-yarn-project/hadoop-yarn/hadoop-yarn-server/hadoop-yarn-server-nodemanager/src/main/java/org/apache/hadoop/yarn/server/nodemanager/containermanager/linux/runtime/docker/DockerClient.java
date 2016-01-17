/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DockerClient {
  private static final Log LOG = LogFactory.getLog(DockerClient.class);
  private static final String TMP_FILE_PREFIX = "docker.";
  private static final String TMP_FILE_SUFFIX = ".cmd";
  private final String tmpDirPath;

  public DockerClient(Configuration conf) throws ContainerExecutionException {

    String tmpDirBase = conf.get("hadoop.tmp.dir");
    if (tmpDirBase == null) {
      throw new ContainerExecutionException("hadoop.tmp.dir not set!");
    }
    tmpDirPath = tmpDirBase + "/nm-docker-cmds";

    File tmpDir = new File(tmpDirPath);
    if (!(tmpDir.exists() || tmpDir.mkdirs())) {
      LOG.warn("Unable to create directory: " + tmpDirPath);
      throw new ContainerExecutionException("Unable to create directory: " +
          tmpDirPath);
    }
  }

  public String writeCommandToTempFile(DockerCommand cmd, String filePrefix)
      throws ContainerExecutionException {
    File dockerCommandFile = null;
    try {
      dockerCommandFile = File.createTempFile(TMP_FILE_PREFIX + filePrefix,
          TMP_FILE_SUFFIX, new
          File(tmpDirPath));

      Writer writer = new OutputStreamWriter(new FileOutputStream(dockerCommandFile),
          "UTF-8");
      PrintWriter printWriter = new PrintWriter(writer);
      printWriter.print(cmd.getCommandWithArguments());
      printWriter.close();

      return dockerCommandFile.getAbsolutePath();
    } catch (IOException e) {
      LOG.warn("Unable to write docker command to temporary file!");
      throw new ContainerExecutionException(e);
    }
  }
}
