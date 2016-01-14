package org.opendatakraken.core.db;
import java.sql.*;

import org.apache.commons.io.IOUtils;
import org.slf4j.LoggerFactory;

public class DataManipulator {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(DataManipulator.class);

	private String columnName = "";
	
	private String sourceProductName = "";
	private String sourceType = "";
	private String sourceTypeAttribute = "";
	
	private String targetProductName = "";
	private String targetType = "";
	private String targetTypeAttribute = "";
	private int targetLength = 0;
	
	private PreparedStatement statement;
	private ResultSet resultSet;
	
	// Internally used
	Object object = null;
	//String className = "";
	java.sql.SQLXML xml = null;
	String blobToString;
    
	int position;
	
    // Constructor
    public DataManipulator() {
        super();
    }
    
    // Set methods
    public void setColumnName(String property) {
    	columnName = property;
    }
    
    public void setSourceProductName(String property) {
    	sourceProductName = property;
    }
    
    public void setSourceType(String property) {
    	sourceType = property;
    }
    
    public void setSourceTypeAttribute(String property) {
    	sourceTypeAttribute = property;
    }
    
    public void setTargetProductName(String property) {
    	targetProductName = property;
    }
    
    public void setTargetType(String property) {
    	targetType = property;
    }
    
    public void setTargetTypeAttribute(String property) {
    	targetTypeAttribute = property;
    }
    
    public void setTargetLength(int property) {
    	targetLength = property;
    }
    
    public void setStatement(PreparedStatement property) {
    	statement = property;
    }
    
    public void setResultSet(ResultSet property) {
    	resultSet = property;
    }
    
    public void setPosition(int property) {
    	position = property;
    }
    
    /**
     * Get type
     **/
    public int getSQLType() {
    	
    	int sqlType = 0;

		if (
			(
				targetProductName.contains("ANYWHERE") ||
				targetProductName.contains("DERBY")
			) &&
			targetType.contains("XML")
		) {
			sqlType = java.sql.Types.CLOB;
  		}
		else if (
			targetProductName.contains("MICROSOFT") ||
			targetProductName.contains("ANYWHERE") ||
			targetProductName.contains("IQ") ||
			targetProductName.contains("DERBY")
	    ) {
	       	if (targetTypeAttribute.contains("BIT")) {
	       		sqlType = Types.BINARY;
	    	}
	       	else if (targetType.equals("UNIQUEIDENTIFIER")) {
	        	sqlType = Types.BINARY;
	      	}
	       	else if (targetType.equalsIgnoreCase("UNIQUEIDENTIFIERSTR")) {
	        	sqlType = Types.VARCHAR;
	      	}
	        else if (targetType.toUpperCase().contains("XML")) {
	        	sqlType = Types.CLOB;
	      	}
	        else if (targetType.toUpperCase().contains("BINARY")) {
	        	sqlType = Types.BINARY;
	      	}
	        else if (targetType.toUpperCase().contains("BLOB")) {
	        	sqlType = Types.BLOB;
	      	}
	      	else if (targetType.toUpperCase().contains("DOUBLE")) {
	      		sqlType = Types.DOUBLE;
	      	}
	        else if (targetType.toUpperCase().contains("CLOB")) {
	        	sqlType = Types.CLOB;
	      	}
	      	else if (targetType.toUpperCase().contains("FLOAT")) {
	      		sqlType = Types.FLOAT;
	      	}
	      	else if (targetType.toUpperCase().contains("REAL")) {
	      		sqlType = Types.REAL;
	      	}
	      	else if (targetType.toUpperCase().contains("TEXT")) {
	      		sqlType = Types.CHAR;
	      	}
	        else if (targetType.toUpperCase().contains("TIMESTAMP")) {
	        	sqlType = Types.TIMESTAMP;
	      	}
	      	else if (targetType.toUpperCase().contains("DATE")) {
	      		sqlType = Types.DATE;
	      	}
	      	else if (targetType.toUpperCase().contains("TIME")) {
	      		sqlType = Types.TIME;
	      	}
	  		else if (targetType.toUpperCase().contains("CHAR")) {
	  			sqlType = Types.CHAR;
	  		}
	  		else if (targetType.toUpperCase().contains("VARCHAR")) {
	  			sqlType = Types.VARCHAR;
	  		}
	  		else if (targetType.toUpperCase().contains("NUMERIC")) {
	  			sqlType = Types.NUMERIC;
	  		}
	        else {
	           	sqlType = Types.NULL;
	        }
	    }
	    else {
	        sqlType = Types.NULL;
	    }
    	return sqlType;
    }
    
    /**
     * Set null to a statement
     **/
    public void setNull() throws Exception {
    	if (
    		targetProductName.toUpperCase().contains("HIVE") ||
    		targetProductName.toUpperCase().contains("IMPALA")
    	) {
        	statement.setString(position, "");
    	}
    	else {
            if (targetTypeAttribute.contains("BIT")) {
            	statement.setNull(position, Types.BINARY);
          	}
            else if (targetType.toUpperCase().contains("CHAR")) {
            	statement.setNull(position, Types.CHAR);
          	}
            else if (targetType.toUpperCase().equals("UNIQUEIDENTIFIER")) {
            	statement.setNull(position, Types.BINARY);
            }
           	else if (targetType.equalsIgnoreCase("UNIQUEIDENTIFIERSTR")) {
            	statement.setNull(position, Types.CHAR);
          	}
            else if (targetType.toUpperCase().contains("CLOB")) {
            	statement.setNull(position, Types.CLOB);
          	}
            else if (targetType.toUpperCase().contains("BLOB")) {
            	statement.setNull(position, Types.BLOB);
          	}
            else if (targetType.toUpperCase().contains("BINARY")) {
            	statement.setNull(position, Types.BINARY);
          	}
            else if (targetType.toUpperCase().contains("GRAPHIC")) {
            	statement.setNull(position, Types.BINARY);
          	}
          	else if (targetType.toUpperCase().contains("INT")) {
          		statement.setNull(position, Types.INTEGER);
          	}
          	else if (targetType.toUpperCase().contains("DECIMAL")) {
          		statement.setNull(position, Types.DECIMAL);
          	}
          	else if (targetType.toUpperCase().contains("NUMERIC")) {
          		statement.setNull(position, Types.DECIMAL);
          	}
          	else if (targetType.toUpperCase().contains("DOUBLE")) {
          		statement.setNull(position, Types.DOUBLE);
          	}
          	else if (targetType.toUpperCase().contains("FLOAT")) {
          		statement.setNull(position, Types.FLOAT);
          	}
          	else if (targetType.toUpperCase().contains("REAL")) {
          		statement.setNull(position, Types.REAL);
          	}
          	else if (targetType.toUpperCase().contains("TEXT")) {
          		statement.setNull(position, Types.CHAR);
          	}
            else if (targetType.toUpperCase().contains("TIMESTAMP")) {
            	statement.setNull(position, Types.TIMESTAMP);
          	}
          	else if (targetType.toUpperCase().contains("DATE")) {
          		statement.setNull(position, Types.DATE);
          	}
          	else if (targetType.toUpperCase().contains("TIME")) {
          		statement.setNull(position, Types.TIME);
          	}
          	else if (
          		targetType.toUpperCase().contains("XML") &&
          		targetProductName.toUpperCase().contains("SQL ANYWHERE")
          	) {
          		statement.setNull(position, Types.CHAR);
          	}
            else {
               	statement.setNull(position, Types.NULL);
            }
    	}
    }
    
    /**
     * Copy object from a recordset to a statement
     **/
    public void copyObject() throws Exception {
		try {
			// In case of HIVE source use lowercase column names
			if (
				sourceProductName.toUpperCase().contains("HIVE") ||
		    	sourceProductName.toUpperCase().contains("IMPALA")
		    ) {
				columnName = columnName.toLowerCase();
			}

			// Get object from source column except for RDBMS not supporting getObject method
			if (
				!(
					sourceProductName.toUpperCase().contains("MYSQL") &&
					(
						sourceType.equalsIgnoreCase("DATETIME") ||
						sourceType.equalsIgnoreCase("TIMESTAMP")
					)
				) &&
				!sourceProductName.toUpperCase().contains("HIVE") &&
				!sourceProductName.toUpperCase().contains("INFORMIX") &&
				!(
					sourceProductName.toUpperCase().contains("HSQL") &&
					sourceType.equalsIgnoreCase("BIT")
				)
			) {
				object = resultSet.getObject(columnName);
			}
			if (
				object == null &&
				!sourceProductName.toUpperCase().contains("HIVE") &&
				!sourceProductName.toUpperCase().contains("INFORMIX") &&
				!(
					sourceProductName.toUpperCase().contains("HSQL") &&
					sourceType.equalsIgnoreCase("BIT")
				)
			) {
				setNull();
			}
			else if (
			   	(
			   		sourceType.toUpperCase().contains("LOB") ||
			   		sourceType.toUpperCase().contains("XML") ||
			   		sourceType.toUpperCase().contains("LONG")
	    		) &&
			   	sourceProductName.toUpperCase().contains("DERBY")
			) {
				// Cannot handle LOBs and LONGs in Apache Derby
				setNull();
			}
			else if (
				sourceType.toUpperCase().contains("BOOL") &&
	    		(
					targetProductName.toUpperCase().contains("HIVE") ||
					sourceProductName.toUpperCase().contains("HIVE") ||
					sourceProductName.toUpperCase().contains("IMPALA") ||
		    		sourceProductName.toUpperCase().contains("INFORMIX") ||
	    			sourceProductName.toUpperCase().contains("POSTGRE") ||
	    			sourceProductName.toUpperCase().contains("VERTICA")
	    		)
	    	) {
				statement.setBoolean(position, resultSet.getBoolean(columnName));
			}
			else if (
				targetType.toUpperCase().contains("BOOL") &&
				targetProductName.toUpperCase().contains("HIVE") &&
				targetProductName.toUpperCase().contains("IMPALA")
			) {
				statement.setBoolean(position, resultSet.getBoolean(columnName));
			}
			else if (
				(
					targetType.toUpperCase().contains("INT") &&
					!targetType.toUpperCase().contains("BINTEXT") &&
					!targetProductName.toUpperCase().contains("HIVE") &&
					!targetProductName.toUpperCase().contains("IMPALA")
				) ||
				(
					sourceType.toUpperCase().contains("INT") &&
					sourceProductName.toUpperCase().contains("HIVE")
				)
			) {
				statement.setBigDecimal(position, resultSet.getBigDecimal(columnName));
			}
			else if (targetType.toUpperCase().contains("FLOAT")) {
				statement.setFloat(position, resultSet.getFloat(columnName));
			}
			else if (targetType.toUpperCase().contains("DOUBLE")) {
				statement.setDouble(position, resultSet.getDouble(columnName));
			}
			else if (
	    		(
	    			targetType.toUpperCase().contains("NUMERIC") ||
	    			targetType.toUpperCase().contains("DECIMAL")
	    		) &&
				(
					targetProductName.toUpperCase().contains("SQL SERVER") ||
					targetProductName.toUpperCase().contains("EXASOL")
				)
			) {
				if (
					sourceType.toUpperCase().contains("MONEY") &&
					sourceProductName.toUpperCase().contains("POSTGRESQL") &&
					targetProductName.toUpperCase().contains("EXASOL")
				) {
					statement.setDouble(position,resultSet.getDouble(columnName));
				}
				else {
				   	statement.setDouble(position, Double.parseDouble(resultSet.getString(columnName)));
				}
			}
			else if (
				targetType.toUpperCase().contains("INT") ||
			    targetType.toUpperCase().contains("MONEY") ||
	    		targetType.toUpperCase().contains("NUMERIC") ||
	    		targetType.toUpperCase().contains("DECIMAL")
			) {
				if (
					!targetProductName.toUpperCase().contains("HIVE") &&
					!targetProductName.toUpperCase().contains("IMPALA")
				) {
					statement.setBigDecimal(position, resultSet.getBigDecimal(columnName));
				}
				else {
					statement.setDouble(position, resultSet.getDouble(columnName));
				}
			}
			else if (
			    targetType.equalsIgnoreCase("DECFLOAT") &&
			   	targetProductName.toUpperCase().contains("DB2")
			) {
				statement.setFloat(position, resultSet.getFloat(columnName));
			}
			else if (
    			sourceType.toUpperCase().toUpperCase().contains("BIT") &&
				(
					sourceProductName.toUpperCase().contains("HSQL") ||
					sourceProductName.toUpperCase().contains("MYSQL") ||
					sourceProductName.toUpperCase().contains("POSTGRES")
				)
			) {
				if (
					sourceProductName.toUpperCase().contains("MYSQL") &&
					(
						targetProductName.toUpperCase().contains("INFORMIX") ||
						targetProductName.toUpperCase().contains("MYSQL")
					)
				) {
				    statement.setBytes(position, resultSet.getBytes(columnName));
				}
				else {
				    statement.setString(position, resultSet.getString(columnName));
				}
			}
			else if (
				targetType.equalsIgnoreCase("TIME")
			) {
				statement.setTime(position, resultSet.getTime(columnName));
			}
			else if (
				(
					targetType.toUpperCase().contains("TIME") ||
				    targetType.toUpperCase().contains("DATE")
				) &&
				(
					targetProductName.toUpperCase().contains("HIVE") ||
					targetProductName.toUpperCase().contains("IMPALA")
				)
			) {
				statement.setString(position, resultSet.getString(columnName));
			}
			else if (
			    targetType.toUpperCase().contains("TIMESTAMP") ||
			    targetType.toUpperCase().contains("DATETIME")
			) {
				if (
					sourceProductName.toUpperCase().contains("SQL ANYWHERE") &&
					(
						targetProductName.toUpperCase().contains("DB2") ||
						targetProductName.toUpperCase().contains("SQL ANYWHERE")
					)
				) {
		    		statement.setObject(position, resultSet.getObject(columnName));
				}
				else if (
					sourceProductName.toUpperCase().contains("NETEZZA") &&
					sourceType.equalsIgnoreCase("TIME")
				) {
					statement.setTime(position, resultSet.getTime(columnName));
				}
				else if (
					sourceProductName.toUpperCase().contains("HIVE") &&
					sourceType.equalsIgnoreCase("DATE")
				) {
					statement.setDate(position, java.sql.Date.valueOf(resultSet.getString(columnName)));
				}
				else if (
					sourceProductName.toUpperCase().contains("HIVE") &&
					sourceType.equalsIgnoreCase("TIMESTAMP")
				) {
					statement.setTimestamp(position, java.sql.Timestamp.valueOf(resultSet.getString(columnName)));
				}
				else if (sourceProductName.toUpperCase().contains("MYSQL")) {
					java.sql.Timestamp ts = null;
					try {
						ts = resultSet.getTimestamp(columnName);
					}
					catch (Exception ts_e) {
					}
					statement.setTimestamp(position, ts);
				}
				else {
					statement.setTimestamp(position, resultSet.getTimestamp(columnName));
				}
			}
			else if (
			   	sourceType.toUpperCase().contains("TIMESTAMP") &&
    			sourceProductName.toUpperCase().contains("ORACLE")
			) {
				statement.setTimestamp(position, resultSet.getTimestamp(columnName));
			}
			else if (
			   	targetType.toUpperCase().contains("GRAPHIC") &&
			   	targetProductName.toUpperCase().contains("DB2")
			) {
				statement.setString(position, resultSet.getString(columnName));
			}
			else if (
			   	sourceType.toUpperCase().contains("XML") &&
		    	(
			   		targetProductName.toUpperCase().contains("DB2") ||
			   		targetProductName.toUpperCase().contains("DERBY") ||
		    		targetProductName.toUpperCase().contains("MYSQL") ||
		    		targetProductName.toUpperCase().contains("ORACLE") ||
		    		targetProductName.toUpperCase().contains("MICROSOFT") ||
		    		targetProductName.toUpperCase().contains("H2") ||
		    		targetProductName.toUpperCase().contains("HSQL") ||
		    		targetProductName.toUpperCase().contains("ANYWHERE")
		    	)
		    ) {
			   	statement.setString(position, resultSet.getString(columnName));
		    }
			else if (
			   	targetType.equalsIgnoreCase("XML") &&
			   	targetProductName.toUpperCase().contains("POSTGRES")
			) {
				xml = statement.getConnection().createSQLXML();
				xml.setString(resultSet.getString(columnName));
				statement.setSQLXML(position, xml);
			}
			else if (
				targetTypeAttribute.toUpperCase().contains("FOR BIT DATA") ||
	    		targetType.equalsIgnoreCase("BYTE") ||
				targetType.toUpperCase().contains("BLOB") ||
				targetType.toUpperCase().contains("BYTEA") ||
				targetType.toUpperCase().contains("BINARY") ||
				targetType.toUpperCase().contains("VARBYTE")
			) {
				if (targetProductName.toUpperCase().contains("HIVE")) {
					statement.setString(position, resultSet.getString(columnName));
				}
				else if (
					targetProductName.toUpperCase().contains("FIREBIRD") &&
					(
						sourceType.toUpperCase().contains("CLOB") ||
						sourceType.toUpperCase().contains("TEXT") ||
						sourceType.toUpperCase().contains("CHAR") ||
						sourceType.toUpperCase().contains("STRING")
					)
				) {
					statement.setString(position, resultSet.getString(columnName));
			    }
				else if (
					sourceProductName.toUpperCase().contains("HIVE") ||
					sourceProductName.toUpperCase().contains("SQL ANYWHERE")
				) {
					if (resultSet.getString(columnName) != null) {
						statement.setBytes(position, resultSet.getString(columnName).getBytes());
					}
					else {
						statement.setNull(position, Types.BINARY);
					}
				}
				else if (
					sourceProductName.toUpperCase().contains("TERADATA") &&
					(
						targetProductName.toUpperCase().contains("FIREBIRD") ||
						targetProductName.toUpperCase().contains("POSTGRESQL") ||
						targetProductName.toUpperCase().contains("TERADATA")
					)
				) {
					statement.setBytes(position, IOUtils.toByteArray(resultSet.getBinaryStream(columnName)));
				}
				else if (sourceProductName.toUpperCase().contains("TERADATA")) {
					statement.setBinaryStream(position, resultSet.getBinaryStream(columnName));
				}
				else {
					statement.setBytes(position, resultSet.getBytes(columnName));
				}
			}
			else if (
				targetType.toUpperCase().contains("CLOB") ||
				targetType.toUpperCase().contains("TEXT") ||
				targetType.toUpperCase().contains("CHAR")
			) {
				if (
					(
						sourceType.toUpperCase().contains("BLOB") ||
						sourceType.toUpperCase().contains("BINARY") ||
						sourceType.toUpperCase().contains("BYTE")
					)
				) {
					if (
						sourceProductName.toUpperCase().contains("HIVE") ||
						sourceProductName.toUpperCase().contains("SQL ANYWHERE")
					) {
						statement.setString(position, resultSet.getString(columnName));
					}
					else if (sourceProductName.toUpperCase().contains("TERADATA")) {
						statement.setBytes(position, IOUtils.toByteArray(resultSet.getBinaryStream(columnName)));
					}
					else {
						statement.setBytes(position, resultSet.getBytes(columnName));
					}
				}
				else if (
					sourceTypeAttribute.contains("FOR BIT DATA") &&
					!targetProductName.toUpperCase().contains("NETEZZA")
				) {
					statement.setBytes(position, resultSet.getBytes(columnName));
				}
				else {
					statement.setString(position, resultSet.getString(columnName));
				}
			}
			else if (targetType.toUpperCase().contains("STRING")) {
				if (
					targetProductName.toUpperCase().contains("HIVE") &&
					(
						sourceType.toUpperCase().contains("BLOB") ||
						sourceType.toUpperCase().contains("BINARY") ||
						sourceType.toUpperCase().contains("BYTE")
					) &&
					(
						sourceProductName.toUpperCase().contains("HDB") ||
						sourceProductName.toUpperCase().contains("HSQL") ||
						sourceProductName.toUpperCase().contains("INFORMIX") ||
						sourceProductName.toUpperCase().contains("MYSQL") ||
						sourceProductName.toUpperCase().contains("SQL ANYWHERE") ||
						sourceProductName.toUpperCase().contains("SQL SERVER") ||
						sourceProductName.toUpperCase().contains("TERADATA")
					)
				) {
					// Cannot insert binaries into Hive Strings
					setNull();
				}
				else if (
					sourceType.toUpperCase().contains("BLOB") &&
					(
						sourceProductName.toUpperCase().contains("HSQL") ||
						sourceProductName.toUpperCase().contains("ORACLE")
					)
				) {
					blobToString = new String(resultSet.getBytes(columnName));
					statement.setString(position, blobToString);
				}
				else {
					statement.setString(position, resultSet.getString(columnName));
				}
			}
			else {
	    		statement.setObject(position, resultSet.getObject(columnName));
	    	}
		}
		catch (Exception e){
			logger.error(columnName + ": " + sourceType + " => " + targetType + "\n" + e.getMessage());
			e.printStackTrace();
			setNull();
			throw e;
		}
    }

    public void setObject(Object property) throws Exception {
		try {
			if (targetType.contains("SDO")) {
				logger.debug("!!!!!!!!!!!!!!! ORACLE SDO TYPE !!!!!!!!!!!!!!!");
				statement.setNull(position, Types.NULL);
			}
			else if (property == null) {
				statement.setNull(position, getSQLType());
			}
			else if (
				targetProductName.toUpperCase().contains("HIVE") ||
				targetProductName.toUpperCase().contains("IMPALA")
			) {
				if (targetType.toUpperCase().contains("BOOLEAN")) {
					statement.setBoolean(position, (boolean) property);
				}
				else if (
					targetType.toUpperCase().contains("DECIMAL") ||
					targetType.toUpperCase().contains("INT")
				) {
					statement.setInt(position, (int) property);
				}
				else if (
					targetType.toUpperCase().contains("FLOAT") ||
					targetType.toUpperCase().contains("REAL")
				) {
					statement.setFloat(position, (float) property);
				}
				else if (
					targetType.toUpperCase().contains("DOUBLE")
				) {
					statement.setDouble(position, (double) property);
				}
				else if (
					targetType.toUpperCase().contains("DATE") ||
					targetType.toUpperCase().contains("TIME")
				) {
					statement.setString(position, String.valueOf(property));
				}
				else if (
					targetType.toUpperCase().contains("CHAR") ||
					targetType.toUpperCase().contains("STRING")
				) {
					statement.setString(position, (String) property);
				}
				else if (
					targetType.toUpperCase().contains("BINARY")
				) {
					statement.setString(position, (String) property.toString());
				}
				else {
					logger.error(columnName + ": " + targetType);
				}
			}
			else {
				if (
					targetType.toUpperCase().contains("BIT") &&
					targetProductName.toUpperCase().contains("POSTGRESQL")
				) {
					statement.setString(position, (String) property);
				}
				else if (
					targetType.toUpperCase().contains("XML") &&
					targetProductName.toUpperCase().contains("POSTGRESQL")
				) {
					xml = statement.getConnection().createSQLXML();
					xml.setString((String) property);
					statement.setSQLXML(position, xml);
				}
				else {
		    		statement.setObject(position, property);
	    		}
			}
		}
		catch (Exception e){
			logger.error(columnName + ": " + sourceType + " => " + targetType + "\n" + e.getMessage());
			e.printStackTrace();
			setNull();
		}
    }
}
