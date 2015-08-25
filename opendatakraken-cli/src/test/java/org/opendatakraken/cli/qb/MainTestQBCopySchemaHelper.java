package org.opendatakraken.cli.qb;

import org.opendatakraken.cli.Main;

public class MainTestQBCopySchemaHelper {

	private static String[] arguments = new String[19];
	
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
		arguments[13] = "-trgcreate";
		arguments[14] = "false";
		arguments[15] = "-dropifexists";
		arguments[16] = "false";
		arguments[17] = "-commitfrequency";
		arguments[18] = "100";
		
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
		/*for (int i = 0; i < arguments.length; i++) {
			System.out.println(arguments[i]);
		}*/
		Main.main(arguments);
		
	}

}
