package org.opendatakraken.core.db;

import java.sql.*;

import org.slf4j.LoggerFactory;

/**
 * This class contains methods to execute procedures stored in a RDBMS
 * @author marangon
 */
public class ProcedureExecutor {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(ProcedureExecutor.class);

	// Declarations of bean properties
	// Source properties
    private DBConnection connection = null;
	private String procedureName = "";
	private String[] procedureParameters = null;
	private String statement = "";

	// Declarations of internally used variables
	private CallableStatement callStmt = null;
	private PreparedStatement prepStmt = null;
	private String output = "Failure";

	/**
	 * Constructor
	 */
	public ProcedureExecutor() {
		super();
	}

    // Set source properties methods
    public void setConnection(DBConnection property) {
    	connection = property;
    }

	/**
	 * Set the name of the procedure to execute
	 */
	public void setProcedureName(String pn) {
		procedureName = pn;
	}

	/**
	 * Set the parameters values in positional way
	 */
	public void setProcedureParameters(String[] pp) {
		procedureParameters = pp;
	}

	/**
	 * Set statement string
	 */
	public void setStatement(String st) {
		statement = st;
	}


	/**
	 * Get the eventual generated output
	 */
	public String getOutput() {
		return output;
	}

    /**
	 * Execute the procedure
	 */
	public void execute() {

		try {
			if (!(statement == null || statement.equals(""))) {
				logger.info("Statement to execute: " + statement);
				prepStmt = connection.getConnection().prepareStatement(statement);
				//Execute statement
				prepStmt.execute();
				prepStmt.close();
			}
			else if (!(procedureName == null || procedureName.equals(""))) {
				logger.info("Procedure to execute: " + procedureName);
				callStmt = connection.getConnection().prepareCall("{call " + procedureName + "}");
				try {
					for (int i = 0; i < procedureParameters.length; i++) {
						System.out.println("Param "+ i + ": " + procedureParameters[i]);
						callStmt.setString(i + 1, procedureParameters[i]);
					}
				}
				catch(NullPointerException e) {
					System.out.println("No procedure parameters");
				}
				//Execute statement
				callStmt.execute();
				callStmt.close();
			}
			else {
				logger.info("Nothing to execute.");
			}
			
			connection.getConnection().close();
			output = "Success";
			logger.info("Procedure executed.");

		}
		catch (SQLException e1) {
			System.out.println("Cannot execute procedure");
			e1.printStackTrace();
			try {
				connection.getConnection().close();
			}
			catch(SQLException e2) {
				e2.printStackTrace();
			}
		}
	}
}
