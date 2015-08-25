package org.opendatakraken.cli;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.opendatakraken.cli.copy.table.MainTestCopyTableFromMySQL;

@RunWith(Suite.class)
@SuiteClasses({MainTestDBProperties.class, MainTestCopyTableFromMySQL.class})

public class AllTests {

}
