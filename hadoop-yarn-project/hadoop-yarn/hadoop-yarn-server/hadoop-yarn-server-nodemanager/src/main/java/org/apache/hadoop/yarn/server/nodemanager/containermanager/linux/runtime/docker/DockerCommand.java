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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Unstable

/** Represents a docker sub-command
 * e.g 'run', 'load', 'inspect' etc.,
 */

public abstract class DockerCommand  {
  private final String command;
  private final List<String> commandWithArguments;

  protected DockerCommand(String command) {
    this.command = command;
    this.commandWithArguments = new ArrayList<>();
    commandWithArguments.add(command);
  }

  /** Returns the docker sub-command string being used
   * e.g 'run'
   */
  public final String getCommandOption() {
    return this.command;
  }

  /** Add command commandWithArguments - this method is only meant for use by
   * sub-classes
   * @param arguments to be added
   */
  protected final void addCommandArguments(String... arguments) {
    this.commandWithArguments.addAll(Arrays.asList(arguments));
  }

  public String getCommandWithArguments() {
    return StringUtils.join(" ", commandWithArguments);
  }
}