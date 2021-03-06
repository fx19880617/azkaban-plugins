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

package azkaban.jobtype;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import azkaban.flow.CommonJobProperties;
import azkaban.pinot.reportal.util.BoundedOutputStream;
import azkaban.pinot.reportal.util.ReportalRunnerException;
import azkaban.utils.Props;

public abstract class ReportalAbstractRunner {

	private static final String REPORTAL_VARIABLE_PREFIX = "reportal.variable.";
	private static final String REPORTAL_PINOT_PREFIX = "reportal.pinot.";

	protected Props props;
	protected OutputStream outputStream;
	protected String proxyUser;
	protected String jobQuery;
	protected String jobTitle;
	protected String reportalTitle;
	protected String reportalStorageUser;
	protected int outputCapacity;
	protected Map<String, String> variables = new HashMap<String, String>();
	protected Map<String, String> paramsKeyValue = new HashMap<String, String>();
	protected Map<String, String> pinotVariables = new HashMap<String, String>();

	public ReportalAbstractRunner(Properties props) {
		Props prop = new Props();
		prop.put(props);
		this.props = prop;
	}

	public void run() throws Exception {
		System.out.println("Reportal: Setting up environment");

		// Check the properties file
		if (props == null) {
			throw new ReportalRunnerException(
					"Properties file not loaded correctly.");
		}

		// Get the hadoop token
		Configuration conf = new Configuration();
		if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
			conf.set("mapreduce.job.credentials.binary",
					System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
		}

		// Get properties
		String execId = props.getString(CommonJobProperties.EXEC_ID);
		outputCapacity = props.getInt("reportal.output.capacity",
				10 * 1024 * 1024);
		proxyUser = props.getString("reportal.proxy.user");
		jobQuery = props.getString("reportal.job.query");
		jobTitle = props.getString("reportal.job.title");

		reportalTitle = props.getString("reportal.title");
		reportalStorageUser = props.getString("reportal.storage.user",
				"reportal");

		// Get variables
		Map<String, String> reportalVariables = props
				.getMapByPrefix(REPORTAL_VARIABLE_PREFIX);

		// Parse variables
		for (Entry<String, String> entry : reportalVariables.entrySet()) {
			if (entry.getKey().endsWith("from")) {
				String fromValue = entry.getValue();
				String toKey = entry.getKey().substring(0,
						entry.getKey().length() - 4)
						+ "to";
				String toValue = reportalVariables.get(toKey);
				if (toValue != null) {
					variables.put(fromValue, toValue);
				}
			}
		}

		// Get pig params

		String params = props.getString("reportal.job.param", null);

		System.out.println("Paramaters passed in =" + params);

		if (!StringUtils.isBlank(params)) {
			String[] paramList = params.split(",");

			for (String param : paramList) {
				String[] paramKeyValue = param.split("=");
				paramsKeyValue.put(paramKeyValue[0], paramKeyValue[1]);

				System.out.println("paramKey=" + paramKeyValue[0]
						+ "paramKeyValue=" + paramKeyValue[1]);
			}
		}
		// Get Pinot variables
		Map<String, String> reportalPinotVariables = props
				.getMapByPrefix(REPORTAL_PINOT_PREFIX);
		for (String key : reportalPinotVariables.keySet()) {
			pinotVariables.put(key, reportalPinotVariables.get(key));
		}

		// Built-in variables
		variables.put("run_id", execId);
		variables.put("sys_date",
				Long.toString(System.currentTimeMillis() / 1000));

		Calendar cal = Calendar.getInstance();
		Date date = new Date();
		cal.setTime(date);

		String timeZone = props.getString("reportal.default.timezone", "UTC");
		TimeZone.setDefault(TimeZone.getTimeZone(timeZone));

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat hourFormat = new SimpleDateFormat("yyyy-MM-dd-HH");

		variables.put("hive_current_hour", hourFormat.format(cal.getTime()));
		variables.put("hive_current_day", dateFormat.format(cal.getTime()));
		cal.add(Calendar.HOUR, -1);
		variables.put("hive_last_hour", hourFormat.format(cal.getTime()));
		cal.add(Calendar.HOUR, 1);
		cal.add(Calendar.DATE, -1);
		variables.put("hive_yesterday", dateFormat.format(cal.getTime()));
		cal.add(Calendar.DATE, -6);
		variables.put("hive_last_seven_days", dateFormat.format(cal.getTime()));
		cal.add(Calendar.DATE, -1);
		variables.put("hive_last_eight_days", dateFormat.format(cal.getTime()));
		variables.put("owner", proxyUser);
		variables.put("title", reportalTitle);

		// Props debug
		System.out.println("Reportal Variables:");
		for (Entry<String, String> data : variables.entrySet()) {
			System.out.println(data.getKey() + " -> " + data.getValue());
		}

		if (requiresOutput()) {
			// Get output stream to data
			String locationTemp = ("./reportal/" + jobTitle + ".csv").replace(
					"//", "/");
			File tempOutput = new File(locationTemp);
			tempOutput.getParentFile().mkdirs();
			tempOutput.createNewFile();
			outputStream = new BoundedOutputStream(new BufferedOutputStream(
					new FileOutputStream(tempOutput)), outputCapacity);

			// Run the reportal
			runReportal();

			// Cleanup the reportal
			try {
				outputStream.close();
			} catch (IOException e) {
				// We can safely ignore this exception since we're just making
				// sure the stream is closed.
			}
		} else {
			runReportal();
		}
	}

	protected abstract void runReportal() throws Exception;

	protected boolean requiresOutput() {
		return true;
	}

	// need a new injectParams method

	protected String injectVariables(String line) {

		for (Entry<String, String> entry : variables.entrySet()) {
			line = line.replace(":" + entry.getKey(),
					sanitizeVariable(entry.getValue()));

		}

		for (Entry<String, String> entry : paramsKeyValue.entrySet()) {

			line = line.replace("$" + entry.getKey(),
					sanitizeVariable(entry.getValue()));

		}

		return line;
	}

	private String sanitizeVariable(String variable) {
		return variable.replace("'", "\\'").replace("\"", "\\\"");
	}
}
