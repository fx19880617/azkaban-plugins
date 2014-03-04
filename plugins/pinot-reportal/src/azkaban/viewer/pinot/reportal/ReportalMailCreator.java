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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.mail.DefaultMailCreator;
import azkaban.executor.mail.MailCreator;
import azkaban.project.Project;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.EmailMessage;
import azkaban.webapp.AzkabanWebServer;

public class ReportalMailCreator implements MailCreator {
	public static AzkabanWebServer azkaban = null;
	public static HadoopSecurityManager hadoopSecurityManager = null;
	public static String outputLocation = "";
	public static String outputFileSystem = "";
	public static String reportalStorageUser = "";
	public static File reportalMailTempDirectory;
	public static final String REPORTAL_MAIL_CREATOR = "ReportalMailCreator";
	public static final int NUM_PREVIEW_ROWS = 50;

	static {
		DefaultMailCreator.registerCreator(REPORTAL_MAIL_CREATOR, new ReportalMailCreator());
	}

	@Override
	public boolean createFirstErrorMessage(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars) {

		ExecutionOptions option = flow.getExecutionOptions();
		Set<String> emailList = new HashSet<String>(option.getFailureEmails());

		return createEmail(flow, emailList, message, "Failure", azkabanName, clientHostname, clientPortNumber, false);
	}

	@Override
	public boolean createErrorEmail(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars) {

		ExecutionOptions option = flow.getExecutionOptions();
		Set<String> emailList = new HashSet<String>(option.getFailureEmails());

		return createEmail(flow, emailList, message, "Failure", azkabanName, clientHostname, clientPortNumber, false);
	}

	@Override
	public boolean createSuccessEmail(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars) {

		ExecutionOptions option = flow.getExecutionOptions();
		Set<String> emailList = new HashSet<String>(option.getSuccessEmails());

		return createEmail(flow, emailList, message, "Success", azkabanName, clientHostname, clientPortNumber, true);
	}

	private boolean createEmail(ExecutableFlow flow, Set<String> emailList, EmailMessage message, String status, String azkabanName, String clientHostname, String clientPortNumber, boolean printData) {

		Project project = azkaban.getProjectManager().getProject(flow.getProjectId());

		if (emailList != null && !emailList.isEmpty()) {
			message.addAllToAddress(emailList);
			message.setMimeType("text/html");
			message.setSubject("Report " + status + ": " + project.getMetadata().get("title"));
			String urlPrefix = "https://" + clientHostname + ":" + clientPortNumber + "/pinot-reportal";
			try {
				return createMessage(project, flow, message, urlPrefix, printData);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return false;
	}

	private boolean createMessage(Project project, ExecutableFlow flow, EmailMessage message, String urlPrefix, boolean printData) throws Exception {
		message.println("<html>");
		message.println("<head></head>");
		message.println("<body style='font-family: verdana; color: #000000; background-color: #cccccc; padding: 20px;'>");
		message.println("<div style='background-color: #ffffff; border: 1px solid #aaaaaa; padding: 20px;-webkit-border-radius: 15px; -moz-border-radius: 15px; border-radius: 15px;'>");
		// Title
		message.println("<b>" + project.getMetadata().get("title") + "</b>");
		message.println("<div style='font-size: .8em; margin-top: .5em; margin-bottom: .5em;'>");
		// Status
		message.println(flow.getStatus().name());
		// Link to logs
		message.println("(<a href='" + urlPrefix + "?view&logs&id=" + flow.getProjectId() + "&execid=" + flow.getExecutionId() + "'>Logs</a>)");
		// Link to Data
		message.println("(<a href='" + urlPrefix + "?view&id=" + flow.getProjectId() + "&execid=" + flow.getExecutionId() + "'>Result data</a>)");
		// Link to Edit
		message.println("(<a href='" + urlPrefix + "?edit&id=" + flow.getProjectId() + "'>Edit</a>)");
		message.println("</div>");
		message.println("<div style='margin-top: .5em; margin-bottom: .5em;'>");
		// Description
		message.println(project.getDescription());
		message.println("</div>");

		// Print variable values, if any
		Map<String, String> flowParameters = flow.getExecutionOptions().getFlowParameters();
		int i = 0;
		while (flowParameters.containsKey("reportal.variable." + i + ".from")) {
			if (i == 0) {
				message.println("<div style='margin-top: 10px; margin-bottom: 10px; border-bottom: 1px solid #ccc; padding-bottom: 5px; font-weight: bold;'>");
				message.println("Variables");
				message.println("</div>");
				message.println("<table border='1' cellspacing='0' cellpadding='2' style='font-size: 14px;'>");
				message.println("<thead><tr><th><b>Name</b></th><th><b>Value</b></th></tr></thead>");
				message.println("<tbody>");
			}

			message.println("<tr>");
			message.println("<td>" + flowParameters.get("reportal.variable." + i + ".from") + "</td>");
			message.println("<td>" + flowParameters.get("reportal.variable." + i + ".to") + "</td>");
			message.println("</tr>");

			i++;
		}
		message.println("<div>");

		int pinotClusterConfigNumber = 2;
		int pinotDatasetConfigNumber = 3;
    for (String key : project.getMetadata().keySet()) {
      if (project.getMetadata().get(key).equals("pinot-cluster-name")) {
        pinotClusterConfigNumber = Integer.parseInt(key.replace("pinotConfig", "").replace("title", ""));
      }
      if (project.getMetadata().get(key).equals("pinot-dataset-name")) {
        pinotDatasetConfigNumber = Integer.parseInt(key.replace("pinotConfig", "").replace("title", ""));
      }
    }

		String url = azkaban.getServerProps().getString("reportal.pinot.easybi.url") + "?ds=" + azkaban.getServerProps().getString("reportal.pinot.easybi.data.source." + project.getMetadata().get("pinotConfig" + pinotClusterConfigNumber + "name")) + "&table=" + project.getMetadata().get("pinotConfig" + pinotDatasetConfigNumber + "name");
		message.println("<a href='" + url + "'>EasyBI UI</a>");
		message.println("</div>");
    message.println("<div style='margin-top: .5em; margin-bottom: .5em;'>");
    message.println(url);
    message.println("</div>");
		message.println("</div>").println("</body>").println("</html>");

		return true;
	}
}
