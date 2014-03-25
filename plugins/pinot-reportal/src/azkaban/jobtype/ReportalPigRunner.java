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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;
import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.PigStats;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Hours;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.pinot.reportal.util.BoundedOutputStream;
import azkaban.pinot.reportal.util.PinotReportalHelper;
import azkaban.pinot.reportal.util.ReportalRunnerException;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;

import com.linkedin.pinot.hadoop.utils.LatestSegmentRefreshWatermarkUtil;

public class ReportalPigRunner extends ReportalAbstractRunner {

	Props prop;

	private static final Logger LOGGER = Logger
			.getLogger(ReportalPigRunner.class);

	private final String SUCCESS = "success";
	private final String FAILURE = "failure";
	private final String RUNNING = "running";

	private String PATH_TO_OUTPUT;
	private String START_WATERMARK;
	private String END_WATERMARK;
	private String CLUSTER_NAME;
	private String COLLECTION_NAME;
	private String CLUSTER_INFO_URL;

	// private String PATH_TO_OUTPUT = "/jobs/test_pinot_reportal_segment";
	// private String START_WATERMARK = "2014032303";
	// private String END_WATERMARK = "2014032305";
	// private String CLUSTER_NAME = "reportIn";
	// private String COLLECTION_NAME = "testPinotReportal";

	/*
	 * This needs to be taken via a param if the cluster is not reportin
	 */
	private String HOST_NAME_PORT = "http://eat1-app783.corp.linkedin.com:11984";

	private String START_DATE_HOUR = null;
	private String END_DATE_HOUR = null;

	public ReportalPigRunner(String jobName, Properties props) {
		super(props);
		prop = new Props();
		prop.put(props);
	}

	@SuppressWarnings("deprecation")
	@Override
	protected void runReportal() throws Exception {

		String startDateHourPacific = null;
		String endDateHourPacific = null;
		boolean dropFailureDoneFile = false;
		// pinot
		System.out.println("Pinot Reportal: Setting up Pig...");

		for (String key : props.getKeySet()) {
			System.out.println("props .... key: " + key + ", value: "
					+ props.getString(key));
		}

		azkaban.common.utils.Props pinotProps = new azkaban.common.utils.Props();

		String optionalPinotConfigsString = props
				.getString("reportal.pinot.optional-pinot-configs");
		PinotReportalHelper.parseOptionalPinotConfigs(pinotProps,
				optionalPinotConfigsString);

		PATH_TO_OUTPUT = props.getString(
				"reportal.pinot.pinot-segment-output-path", null);
		CLUSTER_NAME = props.getString("reportal.pinot.pinot-cluster-name",
				null);
		COLLECTION_NAME = props.getString("reportal.pinot.pinot-dataset-name",
				null);
		START_WATERMARK = pinotProps.getString("start.watermark", null);
		END_WATERMARK = pinotProps.getString("end.watermark", null);

		if (org.apache.commons.lang.StringUtils.isBlank(PATH_TO_OUTPUT)) {
			throw new Exception("Pinot segment output path is null/empty");
		}

		if (org.apache.commons.lang.StringUtils.isBlank(START_WATERMARK)) {
			throw new Exception(
					"Pinot optional param start.watermark is null/empty");
		}

		if (org.apache.commons.lang.StringUtils.isBlank(END_WATERMARK)) {
			throw new Exception(
					"Pinot optional param end.watermark is null/empty");
		}

		if (org.apache.commons.lang.StringUtils.isBlank(CLUSTER_NAME)) {
			throw new Exception("Pinot cluster name is null/empty");
		}

		if (org.apache.commons.lang.StringUtils.isBlank(COLLECTION_NAME)) {
			throw new Exception("Pinot dataset/collection name is null/empty");
		}

		CLUSTER_INFO_URL = HOST_NAME_PORT + "/info?clusterName=" + CLUSTER_NAME
				+ "&collectionName=" + COLLECTION_NAME;

		LOGGER.info("Segment output path = " + PATH_TO_OUTPUT);
		LOGGER.info("start.watermark = " + START_WATERMARK);
		LOGGER.info("end.watermark = " + END_WATERMARK);
		LOGGER.info("Cluster name = " + CLUSTER_NAME);
		LOGGER.info("Collection name = " + COLLECTION_NAME);
		LOGGER.info("Cluster info URL = " + CLUSTER_INFO_URL);

		Long startDateHour = new Long(START_WATERMARK);
		Long endDateHour = new Long(END_WATERMARK);

		DateTimeFormatter format = DateTimeFormat.forPattern("yyyyMMddHH");

		String startTime = LatestSegmentRefreshWatermarkUtil
				.getDateHourFormattedString(format.parseDateTime(
						START_WATERMARK).withZone(
						DateTimeZone.forTimeZone(TimeZone
								.getTimeZone("Etc/UTC"))));
		String endTime = LatestSegmentRefreshWatermarkUtil
				.getDateHourFormattedString(format.parseDateTime(END_WATERMARK)
						.withZone(
								DateTimeZone.forTimeZone(TimeZone
										.getTimeZone("Etc/UTC"))));

		DateTime maxZookeeperDateTime = LatestSegmentRefreshWatermarkUtil
				.getLatestWatermark(CLUSTER_INFO_URL);

		LOGGER.info("maxZookeeperDateTime: " + maxZookeeperDateTime);

		DateTime maxHDFSSuccessDateTime = getHDFSMaxWaterMark(PATH_TO_OUTPUT,
				SUCCESS);
		DateTime maxHDFSFailureDateTime = getHDFSMaxWaterMark(PATH_TO_OUTPUT,
				FAILURE);
		DateTime maxHDFSRunningDateTime = getHDFSMaxWaterMark(PATH_TO_OUTPUT,
				RUNNING);

		LOGGER.info("maxHDFSSuccessDateTime: " + maxHDFSSuccessDateTime);
		LOGGER.info("maxHDFSFailureDateTime: " + maxHDFSFailureDateTime);
		LOGGER.info("maxHDFSRunningDateTime: " + maxHDFSRunningDateTime);

		if (maxZookeeperDateTime != null && maxHDFSSuccessDateTime != null) {
			DateTime dateHourNow = new DateTime();

			DateTime startDateTime = maxHDFSSuccessDateTime.plusHours(1);
			DateTime endDateTime = dateHourNow.minusHours(2);
			Hours hourDiff = Hours.hoursBetween(startDateTime, endDateTime);

			if (hourDiff.getHours() > 60) {
				throw new Exception("Difference between startDateTime("
						+ startDateTime + ") and endDateTime(" + endDateTime
						+ ") is " + hourDiff);
			}

			startTime = getDateHourFormattedString(startDateTime);
			endTime = getDateHourFormattedString(endDateTime);

			startDateHourPacific = getDateHourFormattedString(startDateTime
					.withZone(DateTimeZone.forTimeZone(TimeZone
							.getTimeZone("America/Los_Angeles"))));
			endDateHourPacific = getDateHourFormattedString(endDateTime
					.withZone(DateTimeZone.forTimeZone(TimeZone
							.getTimeZone("America/Los_Angeles"))));

			if (maxHDFSSuccessDateTime.isBefore(maxZookeeperDateTime)) {
				/*
				 * should not happen unless the folder was deleted explicitly. A
				 * stricter rule would be to to check if the two watermarks are
				 * equal
				 */

				throw new Exception("maxZookeeperDateTime,"
						+ maxZookeeperDateTime
						+ " is greater than maxHDFSDateTime, "
						+ maxHDFSSuccessDateTime);
			}

			if (maxHDFSRunningDateTime.isAfter(maxHDFSSuccessDateTime)
					&& maxHDFSRunningDateTime.isAfter(maxHDFSFailureDateTime)) {

				dropFailureDoneFile = true;
				throw new Exception("The job for DateTime, "
						+ maxHDFSRunningDateTime + endDateHour
						+ " is still running..");
			}
		}

		else if (maxZookeeperDateTime != null && maxHDFSSuccessDateTime == null) {
			throw new Exception(
					"maxHDFSSuccessDateTime is null for maxZookeeperDateTime, "
							+ maxZookeeperDateTime);
		} else if (maxHDFSSuccessDateTime != null
				&& maxZookeeperDateTime == null) {
			throw new Exception(
					"maxZookeeperDateTime is null for maxHDFSSuccessDateTime, "
							+ maxHDFSSuccessDateTime);
		}

		startDateHour = (org.apache.commons.lang.StringUtils
				.isBlank(startDateHourPacific) ? startDateHour : Long
				.parseLong(startDateHourPacific));
		endDateHour = (org.apache.commons.lang.StringUtils
				.isBlank(endDateHourPacific) ? endDateHour : Long
				.parseLong(endDateHourPacific));

		if (startDateHour > endDateHour) {

			throw new Exception("startDateHour (Pacific) , " + startDateHour
					+ " is greater than endDateHour (Pacific) ," + endDateHour);
		}

		LOGGER.info("startDateHour(Pacific) =" + startDateHour
				+ " endDateHour(Pacific) =" + endDateHour);

		dropDoneFile(PATH_TO_OUTPUT, RUNNING, startTime, endTime);
		dropFailureDoneFile = true;

		// reportal
		LOGGER.info("Pinot Reportal Pig: Setting up Pig");

		String params = prop.getString("reportal.job.param", null);

		LOGGER.info("Paramaters passed in =" + params);

		// String[] paramList = params.split(",");
		Map<String, String> pigParamKeyValue = new HashMap<String, String>();
		// for (String param : paramList) {
		// String[] paramKeyValue = param.split("=");
		// pigParamKeyValue.put(paramKeyValue[0], paramKeyValue[1]);
		//
		// LOGGER.info("paramKey=" + paramKeyValue[0] + "paramKeyValue="
		// + paramKeyValue[1]);
		// }

		injectAllVariables(prop.getString(PIG_SCRIPT));

		String[] args = getParams(pigParamKeyValue);

		for (String arg : args) {
			LOGGER.info("arg=" + arg);
		}

		System.out.println("Pinot Reportal Pig: Running pig script");
		PrintStream oldOutputStream = System.out;

		File tempOutputFile = new File("./temp.out");
		OutputStream tempOutputStream = new BoundedOutputStream(
				new BufferedOutputStream(new FileOutputStream(tempOutputFile)),
				outputCapacity);
		PrintStream printStream = new PrintStream(tempOutputStream);
		System.setOut(printStream);

		PigStats stats = null;
		try {
			stats = PigRunner.run(args, null);

			System.setOut(oldOutputStream);

			printStream.close();

			// convert pig output to csv and write it to disk
			System.out.println("Reportal Pig: Converting output");
			InputStream tempInputStream = new BufferedInputStream(
					new FileInputStream(tempOutputFile));
			Scanner rowScanner = new Scanner(tempInputStream);
			PrintStream csvOutputStream = new PrintStream(outputStream);
			while (rowScanner.hasNextLine()) {
				String line = rowScanner.nextLine();
				// strip all quotes, and then quote the columns
				if (line.startsWith("(")) {
					line = transformDumpLine(line);
				} else {
					line = transformDescriptionLine(line);
				}
				csvOutputStream.println(line);
			}
			rowScanner.close();
			csvOutputStream.close();

			// Flush the temp file out
			tempOutputFile.delete();
		} catch (Exception ex) {

			if (startTime != null && endTime != null && dropFailureDoneFile) {
				dropDoneFile(PATH_TO_OUTPUT, FAILURE, startTime, endTime);
			}

			ex.printStackTrace();

		}

		if (stats != null && !stats.isSuccessful()) {

			if (startTime != null && endTime != null && dropFailureDoneFile) {
				dropDoneFile(PATH_TO_OUTPUT, FAILURE, startTime, endTime);
			}

			LOGGER.info("Reportal Pig: Handling errors");

			File pigLogFile = new File("./");

			File[] fileList = pigLogFile.listFiles();

			for (File file : fileList) {
				if (file.isFile() && file.getName().matches("^pig_.*\\.log$")) {
					handleError(file);
				}
			}

			// see jira ticket PIG-3313. Will remove these when we use pig
			// binary with that patch.

			LOGGER.info("Trying to do self kill, in case pig could not.");
			Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
			Thread[] threadArray = threadSet.toArray(new Thread[threadSet
					.size()]);
			for (Thread t : threadArray) {
				if (!t.isDaemon() && !t.equals(Thread.currentThread())) {
					System.out.println("Killing thread " + t);
					t.interrupt();
					t.stop();
				}
			}
			System.exit(1);

			throw new ReportalRunnerException("Pig job failed.");
		} else {

			if (startTime != null && endTime != null) {
				dropDoneFile(PATH_TO_OUTPUT, SUCCESS, startTime, endTime);
			}
			System.out.println("Reportal Pig: Ended successfully");
		}

	}

	// Pinot

	public void dropDoneFile(String destFolder, String status,
			String startTime, String endTime) throws IOException {
		Configuration conf = new Configuration();

		StringBuilder sb = new StringBuilder();

		String doneFileName = sb.append(status).append("_")
				.append(CLUSTER_NAME).append("_").append(COLLECTION_NAME)
				.append("_").append(startTime).append("_").append(endTime)
				.toString();

		if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {

			conf.set("mapreduce.job.credentials.binary",
					System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
		}

		String DONE_FILE_PREFIX = "__";
		String DONE_FILE_SUFFIX = ".done";

		FileSystem fs = FileSystem.get(conf);
		String doneFile = destFolder + Path.SEPARATOR + DONE_FILE_PREFIX
				+ doneFileName + DONE_FILE_SUFFIX;
		Path doneFilePath = new Path(doneFile);

		Path destFolderPath = new Path(destFolder);
		if (!fs.exists(destFolderPath)) {
			fs.mkdirs(destFolderPath);
		}

		if (fs.exists(doneFilePath)) {
			fs.delete(doneFilePath, true);
		}

		FSDataOutputStream out = fs.create(doneFilePath);
		out.writeBytes("");
		out.close();

		fs.setPermission(doneFilePath, new FsPermission(FsAction.ALL,
				FsAction.READ, FsAction.READ));

		LOGGER.info("Done writing the file " + doneFile);

	}

	// Pinot

	public String getDateHourFormattedString(DateTime dateTime) {
		StringBuilder sb = new StringBuilder();
		DateTime maxDateHour = dateTime;

		int startYear = maxDateHour.getYear();
		int startMonth = maxDateHour.getMonthOfYear();
		int startDay = maxDateHour.getDayOfMonth();
		int startHour = maxDateHour.getHourOfDay();

		String startMonthStr = (startMonth < 10) ? "0" + startMonth : String
				.valueOf(startMonth);

		String startDateStr = (startDay < 10) ? "0" + startDay : String
				.valueOf(startDay);

		String startHourStr = (startHour < 10) ? "0" + startHour : String
				.valueOf(startHour);

		return sb.append(startYear).append(startMonthStr).append(startDateStr)
				.append(startHourStr).toString();

	}

	// Pinot

	public DateTime getHDFSMaxWaterMark(String parentFolder, String status)
			throws IOException {

		DateTime maxHDFSDateTime = null;

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path parentPath = new Path(parentFolder);
		FileStatus[] files = fs.listStatus(parentPath);

		if (files != null) {
			LOGGER.info("files size: " + files.length + " status = " + status);
			for (FileStatus fileStatus : files) {
				if (!fileStatus.isDir()) {

					String fileName = fileStatus.getPath().getName();
					if (fileName.startsWith("__" + status)) {
						String[] fileNameInfo = fileName.split("_");
						String maxDateTimeStr = fileNameInfo[fileNameInfo.length - 1]
								.substring(0, 10);
						DateTimeFormatter format = DateTimeFormat
								.forPattern("YYYYMMddHH");
						DateTime maxDateTime = format
								.parseDateTime(maxDateTimeStr);
						if (maxHDFSDateTime == null
								|| (maxHDFSDateTime != null && maxDateTime
										.isAfter(maxHDFSDateTime))) {
							maxHDFSDateTime = maxDateTime;
						}
					}
				}
			}
		}

		return maxHDFSDateTime;

	}

	private static void handleError(File pigLog) throws Exception {
		System.out.println();
		System.out.println("====Pig logfile dump====");
		System.out.println("File: " + pigLog.getAbsolutePath());
		System.out.println();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(pigLog));
			String line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
			}
			reader.close();
			System.out.println();
			System.out.println("====End logfile dump====");
		} catch (FileNotFoundException e) {
			System.out.println("pig log file: " + pigLog + "  not found.");
		}
	}

	private String[] getParams(Map<String, String> additionalParams) {
		ArrayList<String> list = new ArrayList<String>();

		Map<String, String> map = getPigParams();
		if (map != null) {
			for (Map.Entry<String, String> entry : map.entrySet()) {
				list.add("-param");
				list.add(StringUtils.shellQuote(
						entry.getKey() + "=" + entry.getValue(),
						StringUtils.SINGLE_QUOTE));
			}
		}

		// for (Map.Entry<String, String> entry : additionalParams.entrySet()) {
		// list.add("-param");
		// list.add(StringUtils.shellQuote(
		// entry.getKey() + "=" + entry.getValue(),
		// StringUtils.SINGLE_QUOTE));
		//
		// }

		// Run in local mode if filesystem is set to local.
		if (prop.getString("reportal.output.filesystem", "local").equals(
				"local")) {
			list.add("-x");
			list.add("local");
		}

		// Register any additional Pig jars
		String additionalPigJars = prop.getString(PIG_ADDITIONAL_JARS, null);
		if (additionalPigJars != null && additionalPigJars.length() > 0) {
			list.add("-Dpig.additional.jars=" + additionalPigJars);
		}

		// Add UDF import list
		String udfImportList = prop.getString(UDF_IMPORT_LIST, null);
		if (udfImportList != null && udfImportList.length() > 0) {
			list.add("-Dudf.import.list=" + udfImportList);
		}

		// Add the script to execute
		list.add(prop.getString(PIG_SCRIPT));

		return list.toArray(new String[0]);
	}

	// skarthik:How to get pig params??
	protected Map<String, String> getPigParams() {
		return prop.getMapByPrefix(PIG_PARAM_PREFIX);
	}

	private String transformDescriptionLine(String line) {
		int start = line.indexOf(':');
		String cleanLine = line;
		if (start != -1 && start + 3 < line.length()) {
			cleanLine = line.substring(start + 3, line.length() - 1);
		}
		return "\"" + cleanLine.replace("\"", "").replace(",", "\",\"") + "\"";
	}

	private String transformDumpLine(String line) {
		String cleanLine = line.substring(1, line.length() - 1);
		return "\"" + cleanLine.replace("\"", "").replace(",", "\",\"") + "\"";
	}

	private void injectAllVariables(String file) throws FileNotFoundException {
		// Inject variables into the script
		System.out.println("Reportal Pig: Replacing variables");
		File inputFile = new File(file);
		File outputFile = new File(file + ".bak");
		InputStream scriptInputStream = new BufferedInputStream(
				new FileInputStream(inputFile));
		Scanner rowScanner = new Scanner(scriptInputStream);
		PrintStream scriptOutputStream = new PrintStream(
				new BufferedOutputStream(new FileOutputStream(outputFile)));
		while (rowScanner.hasNextLine()) {
			String line = rowScanner.nextLine();
			line = injectVariables(line);
			scriptOutputStream.println(line);

		}
		rowScanner.close();
		scriptOutputStream.close();
		outputFile.renameTo(inputFile);
	}

	public static final String PIG_PARAM_PREFIX = "param.";
	public static final String PIG_PARAM_FILES = "paramfile";
	public static final String PIG_SCRIPT = "reportal.pig.script";
	public static final String UDF_IMPORT_LIST = "udf.import.list";
	public static final String PIG_ADDITIONAL_JARS = "pig.additional.jars";
}
