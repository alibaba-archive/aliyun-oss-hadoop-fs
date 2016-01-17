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

package org.apache.hadoop.tools;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster;
import org.apache.hadoop.yarn.applications.distributedshell.Client;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/**
 * This tool moves Aggregated Log files into HAR archives using the
 * {@link HadoopArchives} tool and the Distributed Shell via the
 * {@link HadoopArchiveLogsRunner}.
 */
public class HadoopArchiveLogs implements Tool {
  private static final Log LOG = LogFactory.getLog(HadoopArchiveLogs.class);

  private static final String HELP_OPTION = "help";
  private static final String MAX_ELIGIBLE_APPS_OPTION = "maxEligibleApps";
  private static final String MIN_NUM_LOG_FILES_OPTION = "minNumberLogFiles";
  private static final String MAX_TOTAL_LOGS_SIZE_OPTION = "maxTotalLogsSize";
  private static final String MEMORY_OPTION = "memory";
  private static final String VERBOSE_OPTION = "verbose";
  private static final String FORCE_OPTION = "force";
  private static final String NO_PROXY_OPTION = "noProxy";

  private static final int DEFAULT_MAX_ELIGIBLE = -1;
  private static final int DEFAULT_MIN_NUM_LOG_FILES = 20;
  private static final long DEFAULT_MAX_TOTAL_LOGS_SIZE = 1024L;
  private static final long DEFAULT_MEMORY = 1024L;

  @VisibleForTesting
  int maxEligible = DEFAULT_MAX_ELIGIBLE;
  @VisibleForTesting
  int minNumLogFiles = DEFAULT_MIN_NUM_LOG_FILES;
  @VisibleForTesting
  long maxTotalLogsSize = DEFAULT_MAX_TOTAL_LOGS_SIZE * 1024L * 1024L;
  @VisibleForTesting
  long memory = DEFAULT_MEMORY;
  private boolean verbose = false;
  @VisibleForTesting
  boolean force = false;
  @VisibleForTesting
  boolean proxy = true;

  @VisibleForTesting
  Set<AppInfo> eligibleApplications;

  private JobConf conf;

  public HadoopArchiveLogs(Configuration conf) {
    setConf(conf);
    eligibleApplications = new HashSet<>();
  }

  public static void main(String[] args) {
    JobConf job = new JobConf(HadoopArchiveLogs.class);

    HadoopArchiveLogs hal = new HadoopArchiveLogs(job);
    int ret = 0;

    try{
      ret = ToolRunner.run(hal, args);
    } catch(Exception e) {
      LOG.debug("Exception", e);
      System.err.println(e.getClass().getSimpleName());
      final String s = e.getLocalizedMessage();
      if (s != null) {
        System.err.println(s);
      } else {
        e.printStackTrace(System.err);
      }
      System.exit(1);
    }
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    int exitCode = 1;

    handleOpts(args);

    FileSystem fs = null;
    Path remoteRootLogDir = new Path(conf.get(
        YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    String suffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(conf);
    Path workingDir = new Path(remoteRootLogDir, "archive-logs-work");
    if (verbose) {
      LOG.info("Remote Log Dir Root: " + remoteRootLogDir);
      LOG.info("Log Suffix: " + suffix);
      LOG.info("Working Dir: " + workingDir);
    }
    try {
      fs = FileSystem.get(conf);
      if (prepareWorkingDir(fs, workingDir)) {

        checkFilesAndSeedApps(fs, remoteRootLogDir, suffix);

        filterAppsByAggregatedStatus();

        checkMaxEligible();

        if (eligibleApplications.isEmpty()) {
          LOG.info("No eligible applications to process");
          exitCode = 0;
        } else {
          StringBuilder sb =
              new StringBuilder("Will process the following applications:");
          for (AppInfo app : eligibleApplications) {
            sb.append("\n\t").append(app.getAppId());
          }
          LOG.info(sb.toString());

          File localScript = File.createTempFile("hadoop-archive-logs-", ".sh");
          generateScript(localScript, workingDir, remoteRootLogDir, suffix);

          exitCode = runDistributedShell(localScript) ? 0 : 1;
        }
      }
    } finally {
      if (fs != null) {
        // Cleanup working directory
        if (fs.exists(workingDir)) {
          fs.delete(workingDir, true);
        }
        fs.close();
      }
    }
    return exitCode;
  }

  private void handleOpts(String[] args) throws ParseException {
    Options opts = new Options();
    Option helpOpt = new Option(HELP_OPTION, false, "Prints this message");
    Option maxEligibleOpt = new Option(MAX_ELIGIBLE_APPS_OPTION, true,
        "The maximum number of eligible apps to process (default: "
            + DEFAULT_MAX_ELIGIBLE + " (all))");
    maxEligibleOpt.setArgName("n");
    Option minNumLogFilesOpt = new Option(MIN_NUM_LOG_FILES_OPTION, true,
        "The minimum number of log files required to be eligible (default: "
            + DEFAULT_MIN_NUM_LOG_FILES + ")");
    minNumLogFilesOpt.setArgName("n");
    Option maxTotalLogsSizeOpt = new Option(MAX_TOTAL_LOGS_SIZE_OPTION, true,
        "The maximum total logs size (in megabytes) required to be eligible" +
            " (default: " + DEFAULT_MAX_TOTAL_LOGS_SIZE + ")");
    maxTotalLogsSizeOpt.setArgName("megabytes");
    Option memoryOpt = new Option(MEMORY_OPTION, true,
        "The amount of memory (in megabytes) for each container (default: "
            + DEFAULT_MEMORY + ")");
    memoryOpt.setArgName("megabytes");
    Option verboseOpt = new Option(VERBOSE_OPTION, false,
        "Print more details.");
    Option forceOpt = new Option(FORCE_OPTION, false,
        "Force recreating the working directory if an existing one is found. " +
            "This should only be used if you know that another instance is " +
            "not currently running");
    Option noProxyOpt = new Option(NO_PROXY_OPTION, false,
        "When specified, all processing will be done as the user running this" +
            " command (or the Yarn user if DefaultContainerExecutor is in " +
            "use). When not specified, all processing will be done as the " +
            "user who owns that application; if the user running this command" +
            " is not allowed to impersonate that user, it will fail");
    opts.addOption(helpOpt);
    opts.addOption(maxEligibleOpt);
    opts.addOption(minNumLogFilesOpt);
    opts.addOption(maxTotalLogsSizeOpt);
    opts.addOption(memoryOpt);
    opts.addOption(verboseOpt);
    opts.addOption(forceOpt);
    opts.addOption(noProxyOpt);

    try {
      CommandLineParser parser = new GnuParser();
      CommandLine commandLine = parser.parse(opts, args);
      if (commandLine.hasOption(HELP_OPTION)) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("mapred archive-logs", opts);
        System.exit(0);
      }
      if (commandLine.hasOption(MAX_ELIGIBLE_APPS_OPTION)) {
        maxEligible = Integer.parseInt(
            commandLine.getOptionValue(MAX_ELIGIBLE_APPS_OPTION));
        if (maxEligible == 0) {
          LOG.info("Setting " + MAX_ELIGIBLE_APPS_OPTION + " to 0 accomplishes "
              + "nothing. Please either set it to a negative value "
              + "(default, all) or a more reasonable value.");
          System.exit(0);
        }
      }
      if (commandLine.hasOption(MIN_NUM_LOG_FILES_OPTION)) {
        minNumLogFiles = Integer.parseInt(
            commandLine.getOptionValue(MIN_NUM_LOG_FILES_OPTION));
      }
      if (commandLine.hasOption(MAX_TOTAL_LOGS_SIZE_OPTION)) {
        maxTotalLogsSize = Long.parseLong(
            commandLine.getOptionValue(MAX_TOTAL_LOGS_SIZE_OPTION));
        maxTotalLogsSize *= 1024L * 1024L;
      }
      if (commandLine.hasOption(MEMORY_OPTION)) {
        memory = Long.parseLong(commandLine.getOptionValue(MEMORY_OPTION));
      }
      if (commandLine.hasOption(VERBOSE_OPTION)) {
        verbose = true;
      }
      if (commandLine.hasOption(FORCE_OPTION)) {
        force = true;
      }
      if (commandLine.hasOption(NO_PROXY_OPTION)) {
        proxy = false;
      }
    } catch (ParseException pe) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("mapred archive-logs", opts);
      throw pe;
    }
  }

  @VisibleForTesting
  boolean prepareWorkingDir(FileSystem fs, Path workingDir) throws IOException {
    if (fs.exists(workingDir)) {
      if (force) {
        LOG.info("Existing Working Dir detected: -" + FORCE_OPTION +
            " specified -> recreating Working Dir");
        fs.delete(workingDir, true);
      } else {
        LOG.info("Existing Working Dir detected: -" + FORCE_OPTION +
            " not specified -> exiting");
        return false;
      }
    }
    fs.mkdirs(workingDir);
    fs.setPermission(workingDir,
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL, true));
    return true;
  }

  @VisibleForTesting
  void filterAppsByAggregatedStatus() throws IOException, YarnException {
    YarnClient client = YarnClient.createYarnClient();
    try {
      client.init(getConf());
      client.start();
      for (Iterator<AppInfo> it = eligibleApplications.iterator();
           it.hasNext();) {
        AppInfo app = it.next();
        try {
          ApplicationReport report = client.getApplicationReport(
              ConverterUtils.toApplicationId(app.getAppId()));
          LogAggregationStatus aggStatus = report.getLogAggregationStatus();
          if (aggStatus.equals(LogAggregationStatus.RUNNING) ||
              aggStatus.equals(LogAggregationStatus.RUNNING_WITH_FAILURE) ||
              aggStatus.equals(LogAggregationStatus.NOT_START) ||
              aggStatus.equals(LogAggregationStatus.DISABLED) ||
              aggStatus.equals(LogAggregationStatus.FAILED)) {
            if (verbose) {
              LOG.info("Skipping " + app.getAppId() +
                  " due to aggregation status being " + aggStatus);
            }
            it.remove();
          } else {
            if (verbose) {
              LOG.info(app.getAppId() + " has aggregation status " + aggStatus);
            }
            app.setFinishTime(report.getFinishTime());
          }
        } catch (ApplicationNotFoundException e) {
          // Assume the aggregation has finished
          if (verbose) {
            LOG.info(app.getAppId() + " not in the ResourceManager");
          }
        }
      }
    } finally {
      if (client != null) {
        client.stop();
      }
    }
  }

  @VisibleForTesting
  void checkFilesAndSeedApps(FileSystem fs, Path remoteRootLogDir,
       String suffix) throws IOException {
    for (RemoteIterator<FileStatus> userIt =
         fs.listStatusIterator(remoteRootLogDir); userIt.hasNext();) {
      Path userLogPath = userIt.next().getPath();
      try {
        for (RemoteIterator<FileStatus> appIt =
             fs.listStatusIterator(new Path(userLogPath, suffix));
             appIt.hasNext();) {
          Path appLogPath = appIt.next().getPath();
          try {
            FileStatus[] files = fs.listStatus(appLogPath);
            if (files.length >= minNumLogFiles) {
              boolean eligible = true;
              long totalFileSize = 0L;
              for (FileStatus file : files) {
                if (file.getPath().getName().equals(appLogPath.getName()
                    + ".har")) {
                  eligible = false;
                  if (verbose) {
                    LOG.info("Skipping " + appLogPath.getName() +
                        " due to existing .har file");
                  }
                  break;
                }
                totalFileSize += file.getLen();
                if (totalFileSize > maxTotalLogsSize) {
                  eligible = false;
                  if (verbose) {
                    LOG.info("Skipping " + appLogPath.getName() + " due to " +
                        "total file size being too large (" + totalFileSize +
                        " > " + maxTotalLogsSize + ")");
                  }
                  break;
                }
              }
              if (eligible) {
                if (verbose) {
                  LOG.info("Adding " + appLogPath.getName() + " for user " +
                      userLogPath.getName());
                }
                eligibleApplications.add(
                    new AppInfo(appLogPath.getName(), userLogPath.getName()));
              }
            } else {
              if (verbose) {
                LOG.info("Skipping " + appLogPath.getName() + " due to not " +
                    "having enough log files (" + files.length + " < " +
                    minNumLogFiles + ")");
              }
            }
          } catch (IOException ioe) {
            // Ignore any apps we can't read
            if (verbose) {
              LOG.info("Skipping logs under " + appLogPath + " due to " +
                  ioe.getMessage());
            }
          }
        }
      } catch (IOException ioe) {
        // Ignore any apps we can't read
        if (verbose) {
          LOG.info("Skipping all logs under " + userLogPath + " due to " +
              ioe.getMessage());
        }
      }
    }
  }

  @VisibleForTesting
  void checkMaxEligible() {
    // If we have too many eligible apps, remove the newest ones first
    if (maxEligible > 0 && eligibleApplications.size() > maxEligible) {
      if (verbose) {
        LOG.info("Too many applications (" + eligibleApplications.size() +
            " > " + maxEligible + ")");
      }
      List<AppInfo> sortedApplications =
          new ArrayList<AppInfo>(eligibleApplications);
      Collections.sort(sortedApplications, new Comparator<AppInfo>() {
        @Override
        public int compare(AppInfo o1, AppInfo o2) {
          int lCompare = Long.compare(o1.getFinishTime(), o2.getFinishTime());
          if (lCompare == 0) {
            return o1.getAppId().compareTo(o2.getAppId());
          }
          return lCompare;
        }
      });
      for (int i = maxEligible; i < sortedApplications.size(); i++) {
        if (verbose) {
          LOG.info("Removing " + sortedApplications.get(i));
        }
        eligibleApplications.remove(sortedApplications.get(i));
      }
    }
  }

  /*
  The generated script looks like this:
  #!/bin/bash
  set -e
  set -x
  if [ "$YARN_SHELL_ID" == "1" ]; then
        appId="application_1440448768987_0001"
        user="rkanter"
  elif [ "$YARN_SHELL_ID" == "2" ]; then
        appId="application_1440448768987_0002"
        user="rkanter"
  else
        echo "Unknown Mapping!"
        exit 1
  fi
  export HADOOP_CLIENT_OPTS="-Xmx1024m"
  export HADOOP_CLASSPATH=/dist/share/hadoop/tools/lib/hadoop-archive-logs-2.8.0-SNAPSHOT.jar:/dist/share/hadoop/tools/lib/hadoop-archives-2.8.0-SNAPSHOT.jar
  "$HADOOP_PREFIX"/bin/hadoop org.apache.hadoop.tools.HadoopArchiveLogsRunner -appId "$appId" -user "$user" -workingDir /tmp/logs/archive-logs-work -remoteRootLogDir /tmp/logs -suffix logs
   */
  @VisibleForTesting
  void generateScript(File localScript, Path workingDir,
        Path remoteRootLogDir, String suffix) throws IOException {
    if (verbose) {
      LOG.info("Generating script at: " + localScript.getAbsolutePath());
    }
    String halrJarPath = HadoopArchiveLogsRunner.class.getProtectionDomain()
        .getCodeSource().getLocation().getPath();
    String harJarPath = HadoopArchives.class.getProtectionDomain()
        .getCodeSource().getLocation().getPath();
    String classpath = halrJarPath + File.pathSeparator + harJarPath;
    FileWriterWithEncoding fw = null;
    try {
      fw = new FileWriterWithEncoding(localScript, "UTF-8");
      fw.write("#!/bin/bash\nset -e\nset -x\n");
      int containerCount = 1;
      for (AppInfo app : eligibleApplications) {
        fw.write("if [ \"$YARN_SHELL_ID\" == \"");
        fw.write(Integer.toString(containerCount));
        fw.write("\" ]; then\n\tappId=\"");
        fw.write(app.getAppId());
        fw.write("\"\n\tuser=\"");
        fw.write(app.getUser());
        fw.write("\"\nel");
        containerCount++;
      }
      fw.write("se\n\techo \"Unknown Mapping!\"\n\texit 1\nfi\n");
      fw.write("export HADOOP_CLIENT_OPTS=\"-Xmx");
      fw.write(Long.toString(memory));
      fw.write("m\"\n");
      fw.write("export HADOOP_CLASSPATH=");
      fw.write(classpath);
      fw.write("\n\"$HADOOP_PREFIX\"/bin/hadoop ");
      fw.write(HadoopArchiveLogsRunner.class.getName());
      fw.write(" -appId \"$appId\" -user \"$user\" -workingDir ");
      fw.write(workingDir.toString());
      fw.write(" -remoteRootLogDir ");
      fw.write(remoteRootLogDir.toString());
      fw.write(" -suffix ");
      fw.write(suffix);
      if (!proxy) {
        fw.write(" -noProxy\n");
      }
      fw.write("\n");
    } finally {
      if (fw != null) {
        fw.close();
      }
    }
  }

  private boolean runDistributedShell(File localScript) throws Exception {
    String[] dsArgs = {
        "--appname",
        "ArchiveLogs",
        "--jar",
        ApplicationMaster.class.getProtectionDomain().getCodeSource()
            .getLocation().getPath(),
        "--num_containers",
        Integer.toString(eligibleApplications.size()),
        "--container_memory",
        Long.toString(memory),
        "--shell_script",
        localScript.getAbsolutePath()
    };
    if (verbose) {
      LOG.info("Running Distributed Shell with arguments: " +
          Arrays.toString(dsArgs));
    }
    final Client dsClient = new Client(new Configuration(conf));
    dsClient.init(dsArgs);
    return dsClient.run();
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf, HadoopArchiveLogs.class);
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @VisibleForTesting
  static class AppInfo {
    private String appId;
    private String user;
    private long finishTime;

    AppInfo(String appId, String user) {
      this.appId = appId;
      this.user = user;
      this.finishTime = 0L;
    }

    public String getAppId() {
      return appId;
    }

    public String getUser() {
      return user;
    }

    public long getFinishTime() {
      return finishTime;
    }

    public void setFinishTime(long finishTime) {
      this.finishTime = finishTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      AppInfo appInfo = (AppInfo) o;

      if (appId != null
          ? !appId.equals(appInfo.appId) : appInfo.appId != null) {
        return false;
      }
      return !(user != null
          ? !user.equals(appInfo.user) : appInfo.user != null);
    }

    @Override
    public int hashCode() {
      int result = appId != null ? appId.hashCode() : 0;
      result = 31 * result + (user != null ? user.hashCode() : 0);
      return result;
    }
  }
}
