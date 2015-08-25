package org.opendatakraken.cli.prepare;

import org.opendatakraken.cli.Main;

public class PrepareSchemaHelper {

	private static String[] arguments = new String[21];
	
	public static void initArguments() {
		
		// Function to test
		arguments[0]  = "tablecopy";
		// Source and target properties
		arguments[1]  = "-srcdbconnpropertyfile";
		arguments[3]  = "-srcdbconnkeywordfile";
		arguments[5]  = "-sourceschema";
		arguments[7]  = "-trgdbconnpropertyfile";
		arguments[9]  = "-trgdbconnkeywordfile";
		arguments[11] = "-targetschema";
		// Common options
		arguments[13] = "-trgtableprefix";
		arguments[14] = "";
		arguments[15] = "-trgtablesuffix";
		arguments[16] = "";
		arguments[17] = "-trgcreate";
		arguments[18] = "true";
		arguments[19] = "-dropifexists";
		arguments[20] = "true";
		
	}
	
	public static void initSource(String[] values) {
		
		for (int i = 0; i < values.length; i++) {
			arguments[2 + i * 2] = values[i];
		}
		
	}
	
	public static void initTarget(String[] values) {
		
		for (int i = 0; i < values.length; i++) {
			arguments[8 + i * 2] = values[i];
		}
		
	}
	
	public static void execute() throws Exception {
		// Perform test
		initArguments();
		Main.main(arguments);
		
	}

}
