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

package org.apache.hadoop.yarn.server.webapp;

import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;

import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

public class WebServices {

  protected ApplicationBaseProtocol appBaseProt;

  public WebServices(ApplicationBaseProtocol appBaseProt) {
    this.appBaseProt = appBaseProt;
  }

  public AppsInfo getApps(HttpServletRequest req, HttpServletResponse res,
      String stateQuery, Set<String> statesQuery, String finalStatusQuery,
      String userQuery, String queueQuery, String count, String startedBegin,
      String startedEnd, String finishBegin, String finishEnd,
      Set<String> applicationTypes) {
    UserGroupInformation callerUGI = getUser(req);
    boolean checkEnd = false;
    boolean checkAppTypes = false;
    boolean checkAppStates = false;
    long countNum = Long.MAX_VALUE;

    // set values suitable in case both of begin/end not specified
    long sBegin = 0;
    long sEnd = Long.MAX_VALUE;
    long fBegin = 0;
    long fEnd = Long.MAX_VALUE;

    if (count != null && !count.isEmpty()) {
      countNum = Long.parseLong(count);
      if (countNum <= 0) {
        throw new BadRequestException("limit value must be greater then 0");
      }
    }

    if (startedBegin != null && !startedBegin.isEmpty()) {
      sBegin = Long.parseLong(startedBegin);
      if (sBegin < 0) {
        throw new BadRequestException("startedTimeBegin must be greater than 0");
      }
    }
    if (startedEnd != null && !startedEnd.isEmpty()) {
      sEnd = Long.parseLong(startedEnd);
      if (sEnd < 0) {
        throw new BadRequestException("startedTimeEnd must be greater than 0");
      }
    }
    if (sBegin > sEnd) {
      throw new BadRequestException(
        "startedTimeEnd must be greater than startTimeBegin");
    }

    if (finishBegin != null && !finishBegin.isEmpty()) {
      checkEnd = true;
      fBegin = Long.parseLong(finishBegin);
      if (fBegin < 0) {
        throw new BadRequestException("finishTimeBegin must be greater than 0");
      }
    }
    if (finishEnd != null && !finishEnd.isEmpty()) {
      checkEnd = true;
      fEnd = Long.parseLong(finishEnd);
      if (fEnd < 0) {
        throw new BadRequestException("finishTimeEnd must be greater than 0");
      }
    }
    if (fBegin > fEnd) {
      throw new BadRequestException(
        "finishTimeEnd must be greater than finishTimeBegin");
    }

    Set<String> appTypes = parseQueries(applicationTypes, false);
    if (!appTypes.isEmpty()) {
      checkAppTypes = true;
    }

    // stateQuery is deprecated.
    if (stateQuery != null && !stateQuery.isEmpty()) {
      statesQuery.add(stateQuery);
    }
    Set<String> appStates = parseQueries(statesQuery, true);
    if (!appStates.isEmpty()) {
      checkAppStates = true;
    }

    AppsInfo allApps = new AppsInfo();
    Collection<ApplicationReport> appReports = null;
    final GetApplicationsRequest request =
        GetApplicationsRequest.newInstance();
    request.setLimit(countNum);
    request.setStartRange(new LongRange(sBegin, sEnd));
    try {
      if (callerUGI == null) {
        // TODO: the request should take the params like what RMWebServices does
        // in YARN-1819.
        appReports = appBaseProt.getApplications(request).getApplicationList();
      } else {
        appReports = callerUGI.doAs(
            new PrivilegedExceptionAction<Collection<ApplicationReport>> () {
          @Override
          public Collection<ApplicationReport> run() throws Exception {
            return appBaseProt.getApplications(request).getApplicationList();
          }
        });
      }
    } catch (Exception e) {
      rewrapAndThrowException(e);
    }
    if (appReports == null) {
      return allApps;
    }
    for (ApplicationReport appReport : appReports) {

      if (checkAppStates &&
          !appStates.contains(StringUtils.toLowerCase(
              appReport.getYarnApplicationState().toString()))) {
        continue;
      }
      if (finalStatusQuery != null && !finalStatusQuery.isEmpty()) {
        FinalApplicationStatus.valueOf(finalStatusQuery);
        if (!appReport.getFinalApplicationStatus().toString()
          .equalsIgnoreCase(finalStatusQuery)) {
          continue;
        }
      }
      if (userQuery != null && !userQuery.isEmpty()) {
        if (!appReport.getUser().equals(userQuery)) {
          continue;
        }
      }
      if (queueQuery != null && !queueQuery.isEmpty()) {
        if (!appReport.getQueue().equals(queueQuery)) {
          continue;
        }
      }
      if (checkAppTypes &&
          !appTypes.contains(
              StringUtils.toLowerCase(appReport.getApplicationType().trim()))) {
        continue;
      }

      if (checkEnd
          && (appReport.getFinishTime() < fBegin || appReport.getFinishTime() > fEnd)) {
        continue;
      }
      AppInfo app = new AppInfo(appReport);

      allApps.add(app);
    }
    return allApps;
  }

  public AppInfo getApp(HttpServletRequest req, HttpServletResponse res,
      String appId) {
    UserGroupInformation callerUGI = getUser(req);
    final ApplicationId id = parseApplicationId(appId);
    ApplicationReport app = null;
    try {
      if (callerUGI == null) {
        GetApplicationReportRequest request =
            GetApplicationReportRequest.newInstance(id);
        app = appBaseProt.getApplicationReport(request).getApplicationReport();
      } else {
        app = callerUGI.doAs(
            new PrivilegedExceptionAction<ApplicationReport> () {
          @Override
          public ApplicationReport run() throws Exception {
            GetApplicationReportRequest request =
                GetApplicationReportRequest.newInstance(id);
            return appBaseProt.getApplicationReport(request).getApplicationReport();
          }
        });
      }
    } catch (Exception e) {
      rewrapAndThrowException(e);
    }
    if (app == null) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }
    return new AppInfo(app);
  }

  public AppAttemptsInfo getAppAttempts(HttpServletRequest req,
      HttpServletResponse res, String appId) {
    UserGroupInformation callerUGI = getUser(req);
    final ApplicationId id = parseApplicationId(appId);
    Collection<ApplicationAttemptReport> appAttemptReports = null;
    try {
      if (callerUGI == null) {
        GetApplicationAttemptsRequest request =
            GetApplicationAttemptsRequest.newInstance(id);
        appAttemptReports =
            appBaseProt.getApplicationAttempts(request)
              .getApplicationAttemptList();
      } else {
        appAttemptReports = callerUGI.doAs(
            new PrivilegedExceptionAction<Collection<ApplicationAttemptReport>> () {
          @Override
          public Collection<ApplicationAttemptReport> run() throws Exception {
            GetApplicationAttemptsRequest request =
                GetApplicationAttemptsRequest.newInstance(id);
            return appBaseProt.getApplicationAttempts(request)
                  .getApplicationAttemptList();
          }
        });
      }
    } catch (Exception e) {
      rewrapAndThrowException(e);
    }
    AppAttemptsInfo appAttemptsInfo = new AppAttemptsInfo();
    if (appAttemptReports == null) {
      return appAttemptsInfo;
    }
    for (ApplicationAttemptReport appAttemptReport : appAttemptReports) {
      AppAttemptInfo appAttemptInfo = new AppAttemptInfo(appAttemptReport);
      appAttemptsInfo.add(appAttemptInfo);
    }

    return appAttemptsInfo;
  }

  public AppAttemptInfo getAppAttempt(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {
    UserGroupInformation callerUGI = getUser(req);
    ApplicationId aid = parseApplicationId(appId);
    final ApplicationAttemptId aaid = parseApplicationAttemptId(appAttemptId);
    validateIds(aid, aaid, null);
    ApplicationAttemptReport appAttempt = null;
    try {
      if (callerUGI == null) {
        GetApplicationAttemptReportRequest request =
            GetApplicationAttemptReportRequest.newInstance(aaid);
        appAttempt =
            appBaseProt.getApplicationAttemptReport(request)
              .getApplicationAttemptReport();
      } else {
        appAttempt = callerUGI.doAs(
            new PrivilegedExceptionAction<ApplicationAttemptReport> () {
          @Override
          public ApplicationAttemptReport run() throws Exception {
            GetApplicationAttemptReportRequest request =
                GetApplicationAttemptReportRequest.newInstance(aaid);
            return appBaseProt.getApplicationAttemptReport(request)
                  .getApplicationAttemptReport();
          }
        });
      }
    } catch (Exception e) {
      rewrapAndThrowException(e);
    }
    if (appAttempt == null) {
      throw new NotFoundException("app attempt with id: " + appAttemptId
          + " not found");
    }
    return new AppAttemptInfo(appAttempt);
  }

  public ContainersInfo getContainers(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {
    UserGroupInformation callerUGI = getUser(req);
    ApplicationId aid = parseApplicationId(appId);
    final ApplicationAttemptId aaid = parseApplicationAttemptId(appAttemptId);
    validateIds(aid, aaid, null);
    Collection<ContainerReport> containerReports = null;
    try {
      if (callerUGI == null) {
        GetContainersRequest request = GetContainersRequest.newInstance(aaid);
        containerReports =
            appBaseProt.getContainers(request).getContainerList();
      } else {
        containerReports = callerUGI.doAs(
            new PrivilegedExceptionAction<Collection<ContainerReport>> () {
          @Override
          public Collection<ContainerReport> run() throws Exception {
            GetContainersRequest request = GetContainersRequest.newInstance(aaid);
            return appBaseProt.getContainers(request).getContainerList();
          }
        });
      }
    } catch (Exception e) {
      rewrapAndThrowException(e);
    }
    ContainersInfo containersInfo = new ContainersInfo();
    if (containerReports == null) {
      return containersInfo;
    }
    for (ContainerReport containerReport : containerReports) {
      ContainerInfo containerInfo = new ContainerInfo(containerReport);
      containersInfo.add(containerInfo);
    }
    return containersInfo;
  }

  public ContainerInfo getContainer(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId,
      String containerId) {
    UserGroupInformation callerUGI = getUser(req);
    ApplicationId aid = parseApplicationId(appId);
    ApplicationAttemptId aaid = parseApplicationAttemptId(appAttemptId);
    final ContainerId cid = parseContainerId(containerId);
    validateIds(aid, aaid, cid);
    ContainerReport container = null;
    try {
      if (callerUGI == null) {
        GetContainerReportRequest request =
            GetContainerReportRequest.newInstance(cid);
        container =
            appBaseProt.getContainerReport(request).getContainerReport();
      } else {
        container = callerUGI.doAs(
            new PrivilegedExceptionAction<ContainerReport> () {
          @Override
          public ContainerReport run() throws Exception {
            GetContainerReportRequest request =
                GetContainerReportRequest.newInstance(cid);
            return appBaseProt.getContainerReport(request).getContainerReport();
          }
        });
      }
    } catch (Exception e) {
      rewrapAndThrowException(e);
    }
    if (container == null) {
      throw new NotFoundException("container with id: " + containerId
          + " not found");
    }
    return new ContainerInfo(container);
  }

  protected void init(HttpServletResponse response) {
    // clear content type
    response.setContentType(null);
  }

  protected static Set<String>
      parseQueries(Set<String> queries, boolean isState) {
    Set<String> params = new HashSet<String>();
    if (!queries.isEmpty()) {
      for (String query : queries) {
        if (query != null && !query.trim().isEmpty()) {
          String[] paramStrs = query.split(",");
          for (String paramStr : paramStrs) {
            if (paramStr != null && !paramStr.trim().isEmpty()) {
              if (isState) {
                try {
                  // enum string is in the uppercase
                  YarnApplicationState.valueOf(
                      StringUtils.toUpperCase(paramStr.trim()));
                } catch (RuntimeException e) {
                  YarnApplicationState[] stateArray =
                      YarnApplicationState.values();
                  String allAppStates = Arrays.toString(stateArray);
                  throw new BadRequestException("Invalid application-state "
                      + paramStr.trim() + " specified. It should be one of "
                      + allAppStates);
                }
              }
              params.add(StringUtils.toLowerCase(paramStr.trim()));
            }
          }
        }
      }
    }
    return params;
  }

  protected static ApplicationId parseApplicationId(String appId) {
    if (appId == null || appId.isEmpty()) {
      throw new NotFoundException("appId, " + appId + ", is empty or null");
    }
    ApplicationId aid = ConverterUtils.toApplicationId(appId);
    if (aid == null) {
      throw new NotFoundException("appId is null");
    }
    return aid;
  }

  protected static ApplicationAttemptId parseApplicationAttemptId(
      String appAttemptId) {
    if (appAttemptId == null || appAttemptId.isEmpty()) {
      throw new NotFoundException("appAttemptId, " + appAttemptId
          + ", is empty or null");
    }
    ApplicationAttemptId aaid =
        ConverterUtils.toApplicationAttemptId(appAttemptId);
    if (aaid == null) {
      throw new NotFoundException("appAttemptId is null");
    }
    return aaid;
  }

  protected static ContainerId parseContainerId(String containerId) {
    if (containerId == null || containerId.isEmpty()) {
      throw new NotFoundException("containerId, " + containerId
          + ", is empty or null");
    }
    ContainerId cid = ConverterUtils.toContainerId(containerId);
    if (cid == null) {
      throw new NotFoundException("containerId is null");
    }
    return cid;
  }

  protected void validateIds(ApplicationId appId,
      ApplicationAttemptId appAttemptId, ContainerId containerId) {
    if (!appAttemptId.getApplicationId().equals(appId)) {
      throw new NotFoundException("appId and appAttemptId don't match");
    }
    if (containerId != null
        && !containerId.getApplicationAttemptId().equals(appAttemptId)) {
      throw new NotFoundException("appAttemptId and containerId don't match");
    }
  }

  protected static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUGI;
  }

  private static void rewrapAndThrowException(Exception e) {
    if (e instanceof UndeclaredThrowableException) {
      rewrapAndThrowThrowable(e.getCause());
    } else {
      rewrapAndThrowThrowable(e);
    }
  }

  private static void rewrapAndThrowThrowable(Throwable t) {
    if (t instanceof AuthorizationException) {
      throw new ForbiddenException(t);
    } else if (t instanceof ApplicationNotFoundException ||
        t instanceof ApplicationAttemptNotFoundException ||
        t instanceof ContainerNotFoundException) {
      throw new NotFoundException(t);
    } else {
      throw new WebApplicationException(t);
    }
  }

}
