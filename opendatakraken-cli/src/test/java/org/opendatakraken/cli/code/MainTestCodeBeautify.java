package org.opendatakraken.cli.code;

import static org.junit.Assert.*;

import org.junit.Test;
import org.opendatakraken.cli.Main;
import org.opendatakraken.core.data.RandomDataGenerator;
import org.slf4j.LoggerFactory;

public class MainTestCodeBeautify {
	
	private String[] arguments = new String[3];
	
	public void initArguments() {
		
		// Function to test
		arguments[0]  = "codebeautify";

	}
	
	@Test
	public void testParentheses() {
		
		initArguments();
		
		// Target properties
		arguments[1] = "-originaltext";
		arguments[2] = "CREATE PROCEDURE testproc AS BEGIN SELECT /*+ bau bau*/ abc, 'cde', fgh INTO v_q,v_r,v_t FROM tabletest, (SELECT abc FROM baubau); END;";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}