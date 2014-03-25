package azkaban.pinot.reportal.util;

public class PinotReportalHelper {

	public static void parseOptionalPinotConfigs(
			azkaban.common.utils.Props pinotSegmentCreationProps,
			String optionalPinotConfigs) {
		optionalPinotConfigs.replace(" ", "");
		String[] optionalConfigStrings = optionalPinotConfigs.split("\\r?\\n");
		for (String optionalConfigString : optionalConfigStrings) {
			String[] variableStrings = optionalConfigString.split("=");
			if (variableStrings.length == 2) {
				pinotSegmentCreationProps.put(variableStrings[0],
						variableStrings[1]);
			} else {
				System.out.println("Config: " + optionalConfigString
						+ " is not a valid pinot config.");
			}
		}
	}

}
