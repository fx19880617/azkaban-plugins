/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.viewer.pinot.reportal;

import java.io.File;
import java.util.Map;

import azkaban.flow.CommonJobProperties;
import azkaban.pinot.reportal.util.Reportal;
import azkaban.utils.Props;

public class ReportalTypeManager
{
  public static final String PINOT_JOB = "ReportalPinot";
  public static final String PINOT_JOB_TYPE = "pinot-reportal-pinot";

  public static void createJobAndFiles(Reportal reportal,
                                       File jobFile,
                                       String jobName,
                                       String queryTitle,
                                       String queryType,
                                       String queryScript,
                                       String dependentJob,
                                       String userName,
                                       Map<String, String> extras) throws Exception
  {

    // Create props for the job
    Props propertiesFile = new Props();
    propertiesFile.put("title", queryTitle);

    ReportalType type = ReportalType.getTypeByName(queryType);

    if (type == null)
    {
      throw new Exception("Type " + queryType + " is invalid.");
    }

    propertiesFile.put("reportal.title", reportal.title);
    propertiesFile.put("reportal.job.title", jobName);
    propertiesFile.put("reportal.job.query", queryScript);
    if (userName != null)
    {
      propertiesFile.put("user.to.proxy", "${reportal.execution.user}");
      propertiesFile.put("reportal.proxy.user", userName);
    }

    type.buildJobFiles(reportal, propertiesFile, jobFile, jobName, queryScript, userName);

    propertiesFile.put(CommonJobProperties.JOB_TYPE, type.getJobTypeName());

    // Order dependency
    if (dependentJob != null)
    {
      propertiesFile.put(CommonJobProperties.DEPENDENCIES, dependentJob);
    }

    if (extras != null)
    {
      propertiesFile.putAll(extras);
    }

    propertiesFile.storeLocal(jobFile);
  }

  public static void createPinotJobAndFiles(Reportal reportal,
                                            File jobFile,
                                            String jobName,
                                            String queryTitle,
                                            String queryType,
                                            String queryScript,
                                            String dependentJob,
                                            String userName,
                                            Map<String, String> extras) throws Exception
  {

    // Create props for the job
    Props propertiesFile = new Props();
    propertiesFile.put("title", queryTitle);

    ReportalType type = ReportalType.PinotJob;

    propertiesFile.put("reportal.title", reportal.title);
    propertiesFile.put("reportal.job.title", jobName);
    propertiesFile.put("reportal.job.query", queryScript);

    if (userName != null)
    {
      propertiesFile.put("user.to.proxy", "${reportal.execution.user}");
      propertiesFile.put("reportal.proxy.user", userName);
    }
    System.out.println("Reportal Pinot Runner: Initializing");

    type.buildJobFiles(reportal, propertiesFile, jobFile, jobName, queryScript, userName);

    propertiesFile.put(CommonJobProperties.JOB_TYPE, type.getJobTypeName());

    // Order dependency
    if (dependentJob != null)
    {
      propertiesFile.put(CommonJobProperties.DEPENDENCIES, dependentJob);
    }

    if (extras != null)
    {
      propertiesFile.putAll(extras);
    }

    propertiesFile.storeLocal(jobFile);
  }
}
