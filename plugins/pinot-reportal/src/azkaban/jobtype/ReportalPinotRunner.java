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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.flow.CommonJobProperties;
import azkaban.pinot.reportal.util.CompositeException;
import azkaban.utils.Props;
import azkaban.pinot.reportal.util.PinotReportalHelper;

import com.linkedin.pinot.hadoop.creators.GeneratePinotData;
import com.linkedin.pinot.hadoop.jobs.azkaban.PushTarSegmentsJob;
import com.linkedin.pinot.hadoop.retention.PinotHDFSRetention;

public class ReportalPinotRunner extends ReportalAbstractRunner {

	Props prop;

	private static final Logger LOGGER = Logger
			.getLogger(ReportalPinotRunner.class);

	public ReportalPinotRunner(String jobName, Properties props) {
		super(props);
		prop = new Props();
		prop.put(props);
	}

	@Override
	protected void runReportal() throws Exception {
		System.out.println("Reportal Pinot Runner: Initializing");
		String execId = props.getString(CommonJobProperties.EXEC_ID);
		for (String key : props.getKeySet()) {
			System.out.println("key: " + key + ", value: "
					+ props.getString(key));
		}

		azkaban.common.utils.Props pinotSegmentCreationProps = new azkaban.common.utils.Props();

		String optionalPinotConfigsString = props
				.getString("reportal.pinot.optional-pinot-configs");
		PinotReportalHelper.parseOptionalPinotConfigs(
				pinotSegmentCreationProps, optionalPinotConfigsString);

		
		List<Exception> exceptions = new ArrayList<Exception>();

		// 1. Data Validation
		runPigJobForDataPreprocessAndValidation(pinotSegmentCreationProps);

		// 2. Pinot Segment Creation
		String name = "GeneratePinotFormatData";

		pinotSegmentCreationProps.put("path.to.input", "/tmp/reportal/"
				+ execId + "/input/preparedData");
		pinotSegmentCreationProps.put("path.to.temp.dir", "/tmp/reportal/"
				+ execId + "/temp");
		String segmentTarOutputPath = props.getString(
				"reportal.pinot.pinot-segment-output-path", "/tmp/reportal/"
						+ execId);

		DateTime dateTime = new DateTime();
		int month = dateTime.getMonthOfYear();
		int day = dateTime.getDayOfMonth();
		int hour = dateTime.getHourOfDay();
		String formattedTime = String.valueOf(dateTime.getYear())
				+ (month < 10 ? "0" + month : month)
				+ (day < 10 ? "0" + day : day)
				+ (hour < 10 ? "0" + hour : hour);

		String tempSegmentTarOutputPath = segmentTarOutputPath + "/"
				+ formattedTime + "/tar";
		Path finalOutputTarDir = new Path(tempSegmentTarOutputPath);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(finalOutputTarDir)) {
			fs.delete(finalOutputTarDir, true);
		}

		pinotSegmentCreationProps.put("path.to.output",
				tempSegmentTarOutputPath);
		pinotSegmentCreationProps.put("path.to.deps.jar",
				props.getString("reportal.pinot.dependency.jars.path"));
		pinotSegmentCreationProps.put("segment.cluster.name",
				props.getString("reportal.pinot.pinot-cluster-name"));
		pinotSegmentCreationProps.put("segment.collection.name",
				props.getString("reportal.pinot.pinot-dataset-name"));

		pinotSegmentCreationProps.put("segment.time.column.name",
				props.getString("reportal.pinot.pinot-time-column-name"));
		pinotSegmentCreationProps.put("segment.time.column.type",
				props.getString("reportal.pinot.pinot-time-column-type"));
		pinotSegmentCreationProps.put("segment.dimension.columns",
				props.getString("reportal.pinot.pinot-dimension-columns"));
		pinotSegmentCreationProps.put("segment.metric.columns",
				props.getString("reportal.pinot.pinot-metric-columns"));
		pinotSegmentCreationProps.put("segment.timestamp.columns",
				props.getString("reportal.pinot.pinot-timestamp-columns"));

	

		// props.put("segment.name.appendDate",
		// properties.getProperty("segment.name.appendDate"));

		GeneratePinotData generatePinotData = new GeneratePinotData(name,
				pinotSegmentCreationProps);
		generatePinotData.run();

		System.out.println("Pinot data generation job completed.");
		// 3. Push Segment
		azkaban.common.utils.Props pushProps = new azkaban.common.utils.Props();
		pushProps.put("path.to.input", tempSegmentTarOutputPath);
		pushProps.put(
				"push.to.hosts",
				props.getString("reportal.pinot.data.push.host."
						+ props.getString("reportal.pinot.pinot-push-fabric")));
		pushProps.put(
				"push.to.port",
				props.getString("reportal.pinot.data.push.port."
						+ props.getString("reportal.pinot.pinot-push-fabric")));
		PushTarSegmentsJob pushTarSegmentsJob = new PushTarSegmentsJob(
				"pushGeneratedPinotData", pushProps);
		pushTarSegmentsJob.run();

		if (exceptions.size() > 0) {
			throw new CompositeException(exceptions);
		}

		// Configuration conf = new Configuration();
		// FileSystem fs = FileSystem.get(conf);
		// Path segmentTarOutputDatePath = new Path(segmentTarOutputPath + "/"
		// + formattedTime);
		// if (fs.exists(segmentTarOutputDatePath)) {
		// fs.delete(segmentTarOutputDatePath, true);
		// }

		// System.out.println("*** tempSegmentTarOutputPath="
		// + tempSegmentTarOutputPath + " ***segmentTarOutputDatePath="
		// + segmentTarOutputDatePath);

		// fs.rename(new Path(tempSegmentTarOutputPath),
		// segmentTarOutputDatePath);

		System.out.println("Starting pinot retention job...");

		azkaban.common.utils.Props pinotHDFSRetentionProps = new azkaban.common.utils.Props();
		pinotHDFSRetentionProps.put("path.to.base.dir",
				props.getString("reportal.pinot.pinot-segment-output-path"));
		pinotHDFSRetentionProps.put("tar.retention.in.days", 24);

		PinotHDFSRetention hdfsRetention = new PinotHDFSRetention(
				"PinotHDFSCleanupJob", pinotHDFSRetentionProps);
		hdfsRetention.run();

		System.out.println("Reportal Pinot Runner: Ended successfully");

	}

	private void runPigJobForDataPreprocessAndValidation(
			azkaban.common.utils.Props pinotSegmentCreationProps)
			throws Exception {
		PigServer pigServer = new PigServer(ExecType.MAPREDUCE,
				prop.toProperties());
		try {
			String pigScript = generatePigScript(pinotSegmentCreationProps);
			System.out
					.println("------------------------------------------\nData Preprocess and Validation Pig Script: \n"
							+ pigScript
							+ "\n------------------------------------------");
			pigServer.registerQuery(pigScript);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String generatePigScript(
			azkaban.common.utils.Props pinotSegmentCreationProps)
			throws IOException {
		String pigScript = "";
		// LOAD data:
		pigScript += "raw_data = LOAD '"
				+ props.getString("reportal.pinot.pinot-data-input-path")
				// + "/*.avro"
				+ "' USING com.linkedin.pig.LiAvroStorage();\n";

		// Columns projection:
		String projected_columns = props.getString(
				"reportal.pinot.pinot-projected-columns", null);

		if (projected_columns == null || projected_columns.length() == 0) {
			pigScript += "proj_data = raw_data;\n";
		} else {
			pigScript += "proj_data = FOREACH raw_data GENERATE "
					+ projected_columns + ";\n";
		}

		// Columns Filtering:
		String filtering_condition = props.getString(
				"reportal.pinot.pinot-filtering-conditions", null);

		if (filtering_condition == null || filtering_condition.length() == 0) {
			pigScript += "filtered_data = proj_data;\n";
		} else {
			pigScript += "filtered_data = FILTER proj_data BY ("
					+ filtering_condition + ");\n";
		}

		// SORT DATA:
		String sorted_columns = props
				.getString("reportal.pinot.pinot-primary-key-columns");
		int segmentsNumber = pinotSegmentCreationProps.getInt(
				"reportal.pinot.parallelism", 1);
		pigScript += "srted_data = ORDER filtered_data BY " + sorted_columns
				+ " ASC PARALLEL " + segmentsNumber + ";\n";

		// // STORE data:
		String preparedDataPath = "/tmp/reportal/"
				+ props.getString(CommonJobProperties.EXEC_ID)
				+ "/input/preparedData";
		pigScript += "STORE srted_data INTO '" + preparedDataPath
				+ "' USING com.linkedin.pig.LiAvroStorage();\n";
		return pigScript;
	}

	@Override
	protected boolean requiresOutput() {
		return false;
	}

	public static final String PIG_PARAM_PREFIX = "param.";
	public static final String PIG_PARAM_FILES = "paramfile";
	public static final String PIG_SCRIPT = "reportal.pig.script";
	public static final String UDF_IMPORT_LIST = "udf.import.list";
	public static final String PIG_ADDITIONAL_JARS = "pig.additional.jars";
}
