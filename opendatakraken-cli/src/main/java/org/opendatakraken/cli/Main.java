package org.opendatakraken.cli;

import org.opendatakraken.etl.sapds.BodiAbapDataFlow;
import org.opendatakraken.etl.sapds.BodiWorkFlow;
import org.opendatakraken.etl.sapds.BodiDataFlowForAbap;
import org.opendatakraken.etl.sapds.BodiJob;
import org.opendatakraken.etl.sapds.BodiDataFlow;
import org.opendatakraken.core.db.DictionaryExtractor;
import org.opendatakraken.core.file.FileImporter;
import org.opendatakraken.core.xml.XMLTransformer;
import java.io.*;
import java.util.*;

import javax.xml.parsers.*;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.*;
import org.w3c.dom.*;
import org.apache.commons.cli.*;

public class Main {

    static final org.slf4j.Logger logger = LoggerFactory.getLogger(Main.class);

    private static Options cmdOptions;
    private static CommandLine cmd;
    private static Properties properties;

    /**
     * @param args Arguments
     */
    public static void main(String[] args) throws Exception {
        logger.info("###################################################################");
        logger.info("START");

        // Get main action from 1. argument
        String action = args[0];
        //
        String[] arguments = new String[args.length - 1];

        for (int i = 0; i < arguments.length; i++) {
            arguments[i] = args[i + 1];
        }

        // Parse the command line arguments
        configureCmdOptions();
        CommandLineParser parser = new DefaultParser();
        try {
            cmd = parser.parse(cmdOptions, arguments);
        } catch (Exception e) {
            logger.error("UNEXPECTED EXCEPTION: " + e.toString());
            e.printStackTrace();
            throw e;
        }

        // Optionally load parameters from property file
        if (cmd.hasOption("propertyfile")) {
            // Get options from optional property file
            try {
                properties = new Properties();
                properties.load(new FileInputStream(cmd.getOptionValue("propertyfile")));
            } catch (Exception e) {
                logger.error("Cannot read property file: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
        }

        logger.info("Action: " + action);
        // Print help
        if (action.equalsIgnoreCase("help")) {
            help();
        }

        // Generate a TOAD project file
        if (action.equalsIgnoreCase("toadproject")) {
            toadProject();
        }

        // Generate a Wrapper Script
        if (action.equalsIgnoreCase("wrapperscript")) {
            wrapperScript();
        }

        // Generate deployment file
        if (action.equalsIgnoreCase("dwsodeploy")) {
            dwsoDeploy();
        }

        // Generate deployment file
        if (action.equalsIgnoreCase("codebeautify")) {
            codeBeautify();
        }

        // Send an email
        if (action.equalsIgnoreCase("sendmail")) {
            sendEmail();
        }

        // Install back-end side framework components
        if (action.equalsIgnoreCase("installdblibrary")) {
            installDBLibrary();
        }

        // Execute a stored procedure
        if (action.equalsIgnoreCase("executeprocedure")) {
            executeProcedure();
        }

        // Get database properties
        if (action.equalsIgnoreCase("dbproperties")) {
            getDatabaseProperties();
        }

        // Copy tables between 2 databases
        if (action.equalsIgnoreCase("tablecopy")) {
            copyTables();
        }

        // Generate random data
        if (action.equalsIgnoreCase("generaterandomdata")) {
            generateRandomData();
        }

        // Import a series of csv files
        if (action.equalsIgnoreCase("importcsvseries")) {
            importCSVSeries();
        }

        // Merge similar files
        if (action.equalsIgnoreCase("mergefiles")) {
            mergeFiles();
        }

        // Create XML definitions for common ETL software
        if (action.equalsIgnoreCase("createetlxml")) {
            createETLXML();
        }
        logger.info("FINISH");
        logger.info("###################################################################");
    }

    @SuppressWarnings("static-access")
    private static void configureCmdOptions() throws Exception {
        logger.info("Configure command line options");

        cmdOptions = new Options();
        Option option = new Option("help", "Print this message");
        cmdOptions.addOption(option);

        org.w3c.dom.Document optionsXML = null;
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            javax.xml.parsers.DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            optionsXML = docBuilder.parse(Thread.currentThread().getContextClassLoader().getResource("cmd/cmdOptions.xml").toString());
            optionsXML.getDocumentElement().normalize();
        } catch (Exception e) {
            logger.error("Cannot load option file: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
        NodeList nList = optionsXML.getElementsByTagName("option");
        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                option = Option.builder(eElement.getElementsByTagName("name").item(0).getChildNodes().item(0).getNodeValue()).hasArg()
                        .argName(eElement.getElementsByTagName("argName").item(0).getChildNodes().item(0).getNodeValue())
                        .desc(eElement.getElementsByTagName("description").item(0).getChildNodes().item(0).getNodeValue())
                        .build();
                cmdOptions.addOption(option);
            }
        }
        logger.info("Options configured");
    }

    private static String getOption(String optionName) {
        String optionValue = null;

        if (cmd.getOptionValue(optionName) == null
                || cmd.getOptionValue(optionName).equalsIgnoreCase("")) {
            try {
                optionValue = properties.getProperty(optionName);
            } catch (NullPointerException npe) {
                logger.debug(optionName + ": No such option");
            }
        } else {
            optionValue = cmd.getOptionValue(optionName);
        }

        return optionValue;
    }

    /**
     * Helper class for creating connections
     */
    private static org.opendatakraken.core.db.DBConnection createConnection(String argumentPrefix) {

        org.opendatakraken.core.db.DBConnection connectionBean = new org.opendatakraken.core.db.DBConnection();
        connectionBean.setPropertyFile(getOption(argumentPrefix + "dbconnpropertyfile"));
        connectionBean.setKeyWordFile(getOption(argumentPrefix + "dbconnkeywordfile"));
        connectionBean.setDatabaseDriver(getOption(argumentPrefix + "dbdriverclass"));
        connectionBean.setConnectionURL(getOption(argumentPrefix + "dbconnectionurl"));
        connectionBean.setUserName(getOption(argumentPrefix + "dbusername"));
        connectionBean.setPassWord(getOption(argumentPrefix + "dbpassword"));

        return connectionBean;
    }

    /*
     * Procedures for single functionalities
     */
    private static void help() throws Exception {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(300);
        formatter.printHelp("obiTools", cmdOptions);
    }

    private static void toadProject() throws Exception {
        org.opendatakraken.core.toad.ToadProjectFileCreator toadProjectCreator = new org.opendatakraken.core.toad.ToadProjectFileCreator();
        toadProjectCreator.createProject(
                getOption("toadprojectname"),
                getOption("toadprojectfolder"),
                getOption("toadprojectfileslocation")
        );
    }

    private static void wrapperScript() throws Exception {
        org.opendatakraken.core.script.WrapperScriptCreator wrapperScriptCreator = new org.opendatakraken.core.script.WrapperScriptCreator();
        wrapperScriptCreator.createWrapper(
                getOption("wrapperscript"),
                getOption("rootfolder"),
                getOption("defaultsubfolders")
        );
    }

    private static void dwsoDeploy() throws Exception {
        org.opendatakraken.core.script.InstallScriptCreator dwsoInstallScriptCreator = new org.opendatakraken.core.script.InstallScriptCreator();
        dwsoInstallScriptCreator.createScript(
                getOption("deployfilename"),
                getOption("deployfileforlder"),
                getOption("dwsofolder"),
                getOption("dwsoinstallfile")
        );
    }
    
    private static void codeBeautify() throws Exception {
    	org.opendatakraken.code.Beautifier codeBeautifier = new org.opendatakraken.code.Beautifier();
    	codeBeautifier.setOriginalText(getOption("originaltext"));
    	codeBeautifier.process();
    }

    private static void sendEmail() throws Exception {
        org.opendatakraken.core.mail.Mailer mailSender = new org.opendatakraken.core.mail.Mailer();
        String mailContent = null;
        if (cmd.hasOption("mailcontentsource")) {
            if (getOption("mailcontentsource").equalsIgnoreCase("database")) {
                org.opendatakraken.core.db.QueryExecutor query = new org.opendatakraken.core.db.QueryExecutor();
                query.setDatabaseDriver(getOption("dbdriverclass"));
                query.setConnectionURL(getOption("dbconnectionurl"));
                query.setUserName(getOption("dbusername"));
                query.setPassWord(getOption("dbpassword"));
                query.setQueryText(getOption("dbquery"));
                ByteArrayInputStream bufferIn = null;
                if (getOption("dboutputformat").equalsIgnoreCase("wrs")) {
                    // Get the stream from the webrowset
                    ByteArrayOutputStream bufferOut = new ByteArrayOutputStream();
                    query.generate();
                    query.setStream(bufferOut);
                    query.streamWRS();
                    bufferIn = new ByteArrayInputStream(bufferOut.toByteArray());
                }
                if (getOption("dboutputformat").equalsIgnoreCase("raw")) {
                    bufferIn = new ByteArrayInputStream(query.getRawOutput().getBytes());
                }
                if (cmd.hasOption("mailcontenttype")) {
                    // Load the stylesheet
                    FileImporter fileInput = new FileImporter();
                    fileInput = new FileImporter();
                    fileInput.setDirectoryName("xsl");
                    fileInput.setFileName(getOption("mailcontentformat") + ".xsl");
                    // Get the transformed xml
                    ByteArrayOutputStream transformOut = new ByteArrayOutputStream();
                    XMLTransformer transformer = new XMLTransformer();
                    transformer.setStyleSheet(fileInput.getReader());
                    transformer.setStreamInput(bufferIn);
                    transformer.setStreamOutput(transformOut);
                    transformer.transform();

                    if (getOption("mailcontenttype").equalsIgnoreCase("html")) {
                        mailContent = "";
                        if (cmd.hasOption("mailcontentstyle")) {
                            FileImporter file = new FileImporter();
                            file.setDirectoryName("css");
                            file.setFileName(getOption("mailcontentstyle") + ".css");
                            mailContent += "<head><style>" + file.getString() + "</style></head>";
                        }
                        mailContent += "<body>" + transformOut.toString() + "</body>";
                    }
                } else {
                    mailContent = bufferIn.toString();
                }
            }
            if (getOption("mailcontentsource").equalsIgnoreCase("file")) {
                org.opendatakraken.core.file.FileImporter file = new org.opendatakraken.core.file.FileImporter();
                file.setDirectoryName(getOption("infilefolder"));
                file.setFileName(getOption("infilename"));
                mailContent = file.getString();
            }
        } else {
            mailContent = getOption("mailcontent");
        }
        mailSender.setSmtpServer(getOption("mailhost"));
        mailSender.setSenderAddress(getOption("mailfrom"));
        mailSender.setReceiverList(getOption("mailto"));
        mailSender.setMailSubject(getOption("mailsubject"));
        mailSender.setMailContent(mailContent);
        mailSender.sendMail();
    }

    private static void mergeFiles() throws Exception {
        org.opendatakraken.core.file.FileMerger fileMerge = new org.opendatakraken.core.file.FileMerger();
        fileMerge.setInputZipFile(getOption("sourcezipfile"));
        fileMerge.setInputDirectory(getOption("sourcedirectory"));
        fileMerge.setOutputFileNames(getOption("outputfilenamelist").split(","));
        fileMerge.setDistributionPatterns(getOption("distributionpattern").split(","));
        fileMerge.setAddFileNameOption(Boolean.parseBoolean(getOption("filenameoption")));
        fileMerge.setColumnSeparator(getOption("columnseparator"));

        try {
            fileMerge.mergeFiles();
        } catch (Exception e) {
            logger.error("UNEXPECTED EXCEPTION: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    private static void importCSVSeries() throws Exception {
        String sourceZipFile = getOption("sourcezipfile");
        if (!(sourceZipFile == null || sourceZipFile.equals(""))) {
            org.opendatakraken.core.db.DBConnection sourceConnectionBean = new org.opendatakraken.core.db.DBConnection();
            sourceConnectionBean.setPropertyFile(getOption("srcdbconnpropertyfile"));
            sourceConnectionBean.setDatabaseDriver(getOption("srcdbdriverclass"));
            sourceConnectionBean.setConnectionURL("jdbc:relique:csv:zip:" + sourceZipFile);
            sourceConnectionBean.setUserName(getOption("srcdbusername"));
            sourceConnectionBean.setPassWord(getOption("srcdbpassword"));
	    	//sourceConnectionBean.openConnection();

            org.opendatakraken.core.db.DBConnection targetConnectionBean = createConnection("trg");
            targetConnectionBean.openConnection();

            // Import a series of csv files with the same structure in the same table
            org.opendatakraken.core.db.CsvSeriesImporter importCsvSeries = new org.opendatakraken.core.db.CsvSeriesImporter();
            importCsvSeries.setSourceConnection(sourceConnectionBean);
            importCsvSeries.setSourceZipFile(sourceZipFile);
            importCsvSeries.setSourceWhereClause(getOption("sourcewhereclause"));
            importCsvSeries.setTargetConnection(targetConnectionBean);
            importCsvSeries.setTargetTable(getOption("targettable"));
            importCsvSeries.setFileNameColumn(getOption("filenamecolumn"));

            importCsvSeries.setCommitFrequency(Integer.parseInt(getOption("commitfrequency")));

            try {
                importCsvSeries.importCsvSeries();
                targetConnectionBean.closeConnection();
            } catch (Exception e) {
                logger.error("UNEXPECTED EXCEPTION: " + e.getMessage());
                e.printStackTrace();
                try {
                    targetConnectionBean.closeConnection();
                } finally {

                }
                throw e;
            }
        }
    }

    private static void getDatabaseProperties() throws Exception {
        logger.info("Get database properties");

        org.opendatakraken.core.db.DBConnection connectionBean = createConnection("");

        try {
            connectionBean.openConnection();
        } catch (Exception e) {
            logger.error("UNEXPECTED EXCEPTION");
            e.printStackTrace();
            throw e;
        }
        connectionBean.closeConnection();
        logger.info("Properties retrieved");
    }

    private static void generateRandomData() throws Exception {
        logger.info("Generate random data in database tables");

        org.opendatakraken.core.db.DBConnection targetConnectionBean = createConnection("trg");

        logger.info("Connection prepared");

        String targetSchema = getOption("targetschema");
        logger.info("Target schema: " + targetSchema);
        String targetTable = getOption("targettable");
        logger.info("Target table: " + targetTable);
        String[] targetTableList = null;
        targetConnectionBean.setSchemaName(targetSchema);
        if (targetSchema != null
                && (targetTable == null || targetTable.equals(""))) {
            logger.info("Fill all tables of a schema");
            targetTableList = targetConnectionBean.getTableList();
        } else {
            logger.info("Fill a single table");
            targetTableList = new String[1];
            targetTableList[0] = targetTable;
        }

        try {

            // Open target connection
            targetConnectionBean.openConnection();
            for (int i = 0; i < targetTableList.length; i++) {
                // Copy the content of a source sql query into a target rdbms table
                logger.info("Feeding table: " + targetTableList[i]);
                org.opendatakraken.core.data.RandomDataGenerator generator = new org.opendatakraken.core.data.RandomDataGenerator();

                generator.setConnection(targetConnectionBean);
                generator.setTargetSchema(targetSchema);
                generator.setTargetTable(targetTableList[i]);
                generator.setPreserveDataOption(Boolean.parseBoolean(getOption("trgpreservedata")));
                if (getOption("numberofrows") != null) {
                    generator.setNumberOfRows(Integer.parseInt(getOption("numberofrows")));
                }
                if (getOption("commitfrequency") != null) {
                    generator.setCommitFrequency(Integer.parseInt(getOption("commitfrequency")));
                }
                generator.generateData();
            }
            // Close target connection
            targetConnectionBean.closeConnection();
        } catch (Exception e) {
            logger.error("UNEXPECTED EXCEPTION: " + e.getMessage());
            e.printStackTrace();
            try {
                targetConnectionBean.closeConnection();
            } finally {

            }
            throw e;
        }
    }

    private static void executeProcedure() throws Exception {
        logger.info("Execute statement or procedure");

        org.opendatakraken.core.db.DBConnection connectionBean = createConnection("");

        try {
            connectionBean.openConnection();
            org.opendatakraken.core.db.ProcedureExecutor executeBean = new org.opendatakraken.core.db.ProcedureExecutor();
            executeBean.setProcedureName(getOption("dbprocedure"));
            executeBean.setStatement(getOption("dbstatement"));
            executeBean.execute();
        } catch (Exception e) {
            logger.error("UNEXPECTED EXCEPTION");
            e.printStackTrace();
            throw e;
        }
    }

    private static void installDBLibrary() throws Exception {
        logger.info("Install framework");
        org.opendatakraken.dblibrary.DBLibraryInstaller installer = new org.opendatakraken.dblibrary.DBLibraryInstaller();
        org.opendatakraken.core.db.DBConnection connectionBean = createConnection("");

        try {
            connectionBean.openConnection();
        } catch (Exception e) {
            logger.error("UNEXPECTED EXCEPTION");
            logger.error(e.getMessage());
            throw e;
        }

        installer.setSourceConnection(connectionBean);
        installer.setDatabaseProduct(getOption("dbproduct"));
        installer.setCatalog(getOption("dbcatalog"));
        installer.setSchema(getOption("dbschema"));
        installer.setModule(getOption("module"));

        String parameterNames = getOption("parameternames");
        String parameterValues = getOption("parametervalues");
        if (!(parameterNames == null)
                && !(parameterValues == null)
                && !parameterNames.equals("")
                && !parameterValues.equals("")) {
            installer.setParameterNames(parameterNames.split(","));
            installer.setParameterValues(parameterValues.split(","));
        }

        installer.install();

        connectionBean.closeConnection();
        logger.info("Framework installed");
    }

    private static void copyTables() throws Exception {

        boolean copySchema = false;

        logger.info("Copy an entire schema, a single table or the result of a query from a database to another");

        org.opendatakraken.core.db.DBConnection sourceConnectionBean = createConnection("src");
        org.opendatakraken.core.db.DBConnection targetConnectionBean = createConnection("trg");

        logger.info("Source and target connections prepared");

        String sourceSchema = getOption("sourceschema");
        logger.info("Source schema: " + sourceSchema);
        String sourceTable = getOption("sourcetable");
        logger.info("Source table: " + sourceTable);
        String sourceQuery = getOption("sourcequery");
        logger.info("Source query: " + sourceQuery);
        String targetSchema = getOption("targetschema");
        logger.info("Target schema: " + targetSchema);
        String targetTable = getOption("targettable");
        logger.info("Target table: " + targetTable);
        String[] sourceTableList = null;
        String[] targetTableList = null;

        String tablePrefix = "";
        String tableSuffix = "";

        sourceConnectionBean.setSchemaName(sourceSchema);
        if (sourceSchema != null
                && (sourceTable == null || sourceSchema.equals(""))
                && (sourceQuery == null || sourceSchema.equals(""))) {
            copySchema = true;
        }

        sourceConnectionBean.openConnection();

        if (copySchema) {
            logger.info("Copy all objects of a schema");
            tablePrefix = getOption("trgtableprefix");
            logger.info("Table prefix: " + tablePrefix);
            tableSuffix = getOption("trgtablesuffix");
            logger.info("Table suffix: " + tableSuffix);
            sourceTableList = sourceConnectionBean.getTableList();
            targetTableList = new String[sourceTableList.length];

            if (tablePrefix == null || tablePrefix.equals("")) {
                tablePrefix = "";
            } else {
                tablePrefix = tablePrefix + "_";
            }

            if (tableSuffix == null || tableSuffix.equals("")) {
                tableSuffix = "";
            } else {
                tableSuffix = "_" + tableSuffix;
            }

            for (int i = 0; i < sourceTableList.length; i++) {
                targetTableList[i] = tablePrefix + sourceTableList[i] + tableSuffix;
                logger.debug("Target: " + targetTableList[i] + " - source: " + sourceTableList[i]);
            }
        } else {
            logger.info("Copy a single table or the result of a query");
            sourceTableList = new String[1];
            targetTableList = new String[1];
            sourceTableList[0] = sourceTable;
            targetTableList[0] = targetTable;
        }

        try {
            String[] columnNames;
            String[] columnDefs;

            if (Boolean.parseBoolean(getOption("trgcreate"))) {
                logger.info("Create tables if they don't exist");
                // Open target connection
                targetConnectionBean.openConnection();
                // Get source dictionary
                org.opendatakraken.core.db.DictionaryConverter dictionaryConversionBean = new org.opendatakraken.core.db.DictionaryConverter();
                org.opendatakraken.core.db.TableCreator tableCreate = new org.opendatakraken.core.db.TableCreator();
                dictionaryConversionBean.setSourceConnection(sourceConnectionBean);
                dictionaryConversionBean.setTargetConnection(targetConnectionBean);
                dictionaryConversionBean.setSourceSchema(sourceSchema);
                tableCreate.setTargetConnection(targetConnectionBean);
                tableCreate.setTargetSchema(targetSchema);
                for (int i = 0; i < sourceTableList.length; i++) {
                    logger.info("Creating table: " + targetTableList[i] + " from table " + sourceTableList[i]);
                    dictionaryConversionBean.setSourceTable(sourceTableList[i]);
                    dictionaryConversionBean.setSourceQuery(sourceQuery);
		    		//
                    //
                    dictionaryConversionBean.retrieveColumns();
                    columnNames = dictionaryConversionBean.getTargetColumnNames();
                    columnDefs = dictionaryConversionBean.getTargetColumnDefinition();
                    // Create a table basing on the result
                    if (copySchema) {
                        targetTableList[i] = targetConnectionBean.getNormalizedObjectName(sourceTableList[i], tablePrefix, tableSuffix);
                    }
                    tableCreate.setTargetTable(targetTableList[i]);
                    tableCreate.setTargetColumns(columnNames);
                    tableCreate.setTargetColumnDefinitions(columnDefs);
                    tableCreate.setDropIfExistsOption(Boolean.parseBoolean(getOption("dropifexists")));
                    tableCreate.createTable();
                }
                targetConnectionBean.closeConnection();
            }
            // Open target connection
            targetConnectionBean.openConnection();
            for (int i = 0; i < targetTableList.length; i++) {
                // Copy the content of a source sql query into a target rdbms table
                logger.info("Feeding table: " + sourceTableList[i]);
                org.opendatakraken.core.db.DataCopier dataCopy = new org.opendatakraken.core.db.DataCopier();
                dataCopy.setSourceConnection(sourceConnectionBean);
                dataCopy.setSourceSchema(sourceSchema);
                dataCopy.setSourceTable(sourceTableList[i]);
                dataCopy.setSourceQuery(sourceQuery);

                dataCopy.setTargetConnection(targetConnectionBean);
                dataCopy.setTargetSchema(targetSchema);
                dataCopy.setTargetTable(targetTableList[i]);
                dataCopy.setPreserveDataOption(Boolean.parseBoolean(getOption("trgpreservedata")));

                String mappingDefFile = getOption("mapdeffile");
                dataCopy.setMappingDefFile(mappingDefFile);

                if (getOption("commitfrequency") != null) {
                    dataCopy.setCommitFrequency(Integer.parseInt(getOption("commitfrequency")));
                }
                if (mappingDefFile != null) {
                    dataCopy.retrieveMappingDefinition();
                }
                dataCopy.retrieveColumnList();
                dataCopy.executeSelect();
                dataCopy.executeInsert();
            }
            // Close target connection
            targetConnectionBean.closeConnection();
            // Close source connection
            sourceConnectionBean.closeConnection();
        } catch (Exception e) {
            logger.error("UNEXPECTED EXCEPTION: " + e.getMessage());
            e.printStackTrace();
            try {
                sourceConnectionBean.closeConnection();
            } finally {

            }
            try {
                targetConnectionBean.closeConnection();
            } finally {

            }
            throw e;
        }
    }

    private static void createETLXML() throws Exception {
        logger.info("Create ETL XML");

        String bodiAbapDataFlowPrefix = getOption("bodiabapdataflowprefix");
        String bodiDataFlowPrefix = getOption("bodidataflowprefix");
        String bodiWorkFlowPrefix = getOption("bodiworkflowprefix");
        String bodiJobPrefix = getOption("bodijobprefix");
        boolean isAbap = false;
        /*if (getOption("bodijobisabap").equalsIgnoreCase("Y")) {
         isAbap = true;
         }*/
        String bodiSourceDataStore = getOption("bodisourcedatastore");
        String bodiTargetDataStore = getOption("boditargetdatastore");
        String bodiExportFile = getOption("bodiexportfile");

        logger.info("Copy an entire schema, a single table or the result of a query from a database to another");

        org.opendatakraken.core.db.DBConnection sourceConnectionBean = new org.opendatakraken.core.db.DBConnection();
        sourceConnectionBean.setPropertyFile(getOption("srcdbconnpropertyfile"));
        sourceConnectionBean.setKeyWordFile(getOption("srcdbconnkeywordfile"));
        sourceConnectionBean.setDatabaseDriver(getOption("srcdbdriverclass"));
        sourceConnectionBean.setConnectionURL(getOption("srcdbconnectionurl"));
        sourceConnectionBean.setUserName(getOption("srcdbusername"));
        sourceConnectionBean.setPassWord(getOption("srcdbpassword"));
        sourceConnectionBean.openConnection();

        logger.info("Source connection prepared");

        String sourceSchema = getOption("sourceschema");
        logger.info("Source schema: " + sourceSchema);
        String sourceTable = getOption("sourcetable");
        logger.info("Source table: " + sourceTable);
        String sourceQuery = getOption("sourcequery");
        logger.info("Source query: " + sourceQuery);
        String targetSchema = getOption("targetschema");
        logger.info("Target schema: " + targetSchema);
        String targetTable = getOption("targettable");
        logger.info("Target table: " + targetTable);

        String[] sourceTableList = null;
        String[] targetTableList = null;
        sourceConnectionBean.setSchemaName(sourceSchema);
        if ((sourceSchema != null)
                && (sourceTable == null || sourceSchema.equals(""))
                && (sourceQuery == null || sourceSchema.equals(""))) {
            logger.info("Copy all objects of a schema");
            sourceTableList = sourceConnectionBean.getTableList();
            targetTableList = sourceTableList;
        } else {
            logger.info("Copy a single table or the result of a query");
            sourceTableList = new String[1];
            targetTableList = new String[1];
            sourceTableList[0] = sourceTable;
            targetTableList[0] = targetTable;
        }
        // Create xml document objects
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document document = documentBuilder.newDocument();
        Element export = document.createElement("DataIntegratorExport");

        try {
            /*String[] columnNames;
             String[] columnDefs;*/
            logger.info("Found " + sourceTableList.length + " tables");

            for (int i = 0; i < sourceTableList.length; i++) {

                logger.debug("Table " + sourceTableList[i]);

                DictionaryExtractor sourceDictionary = new DictionaryExtractor();
                sourceDictionary.setSourceConnection(sourceConnectionBean);
                sourceDictionary.setSourceTable(sourceTableList[i]);
                sourceDictionary.setSourceQuery(sourceQuery);
                sourceDictionary.retrieveColumns();
                /*columnNames = sourceDictionary.getColumnNames();
                 columnDefs = sourceDictionary.getColumnDefinition();*/

                if (isAbap) {
                    // Generate and append abap dataflow
                    BodiAbapDataFlow abapDataFlow = new BodiAbapDataFlow();
                    abapDataFlow.setSourceDataStore(bodiSourceDataStore);
                    abapDataFlow.setDataFlowName(bodiAbapDataFlowPrefix + "_" + sourceTableList[i]);
                    abapDataFlow.setSourceTableName(sourceTableList[i]);
                    export.appendChild(abapDataFlow.getElement(document));
                    // Generate and append dataflow
                    BodiDataFlowForAbap dataFlow = new BodiDataFlowForAbap();
                    dataFlow.setAbapDataFlowName(bodiAbapDataFlowPrefix + "_" + targetTableList[i]);
                    dataFlow.setTargetDataStore(bodiTargetDataStore);
                    dataFlow.setTargetOwnerName(targetSchema);
                    dataFlow.setTargetTableName(targetTableList[i]);
                    dataFlow.setDataFlowName(bodiDataFlowPrefix + "_" + targetTableList[i]);
                    export.appendChild(dataFlow.getElement(document));
                } else {
					// Generate and append source table
					/*BodiTableBean bodiSourceTable = new BodiTableBean();
                     bodiSourceTable.setDataStore(bodiSourceDataStore);
                     bodiSourceTable.setOwnerName(sourceSchema);
                     bodiSourceTable.setTableName(sourceTable);
                     bodiSourceTable.setColumns(columnNames);
                     bodiSourceTable.setDataTypes(columnDefs);
                     bodiSourceTable.setDefaultColumns(new String[0]);
                     bodiSourceTable.setDefaultTypes(new String[0]);
                     export.appendChild(bodiSourceTable.getElement(document));*/

					// Generate and append target table
					/*BodiTableBean bodiTargetTable = new BodiTableBean();
                     bodiTargetTable.setDataStore(bodiTargetDataStore);
                     bodiTargetTable.setOwnerName(targetSchema);
                     bodiTargetTable.setTableName(targetTable);
                     bodiTargetTable.setColumns(columnNames);
                     bodiTargetTable.setDataTypes(columnDefs);*/
                    /*bodiTargetTable.setDefaultColumns(defaultColumns);
                     bodiTargetTable.setDefaultTypes(defaultColTypes);*/
					//export.appendChild(bodiTargetTable.getElement(document));
                    // Generate and append dataflow
                    BodiDataFlow dataFlow = new BodiDataFlow();
                    dataFlow.setSourceDataStore(bodiSourceDataStore);
                    dataFlow.setTargetDataStore(bodiTargetDataStore);
                    dataFlow.setTargetOwnerName(targetSchema);
                    dataFlow.setTargetTableName(targetTableList[i]);
					//
					/*dataFlow.setSourceColumns(srcColumns);
                     dataFlow.setTargetColumns(trgColumns);
                     dataFlow.setDataTypes(trgTypes);
                     //
                     dataFlow.setDefaultColumns(defaultColumns);
                     dataFlow.setDefaultValues(defaultValues);
                     dataFlow.setDefaultTypes(defaultColTypes);*/
                    //
                    dataFlow.setDataFlowName(bodiDataFlowPrefix + "_" + targetTableList[i]);
                    dataFlow.setSourceOwnerName(sourceSchema);
                    dataFlow.setSourceTableName(sourceTableList[i]);
                    export.appendChild(dataFlow.getElement(document));
					//exportParam.appendChild(targetTable.getElement(document));

                }
                // Generate and append workflow
                BodiWorkFlow workFlow = new BodiWorkFlow();
                workFlow.setDataFlowName(bodiDataFlowPrefix + "_" + targetTableList[i]);
                workFlow.setWorkFlowName(bodiWorkFlowPrefix + "_" + targetTableList[i]);
                export.appendChild(workFlow.getElement(document));
                // Generate and append job
                BodiJob job = new BodiJob();
                job.setWorkFlowName(bodiWorkFlowPrefix + "_" + targetTableList[i]);
                job.setJobName(bodiJobPrefix + "_" + targetTableList[i]);
                job.setStageSourceCode(sourceSchema);
                job.setStageObjectName(sourceTableList[i]);
                export.appendChild(job.getElement(document));
            }
            // Close source connection
            sourceConnectionBean.closeConnection();
            // write the content into file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.transform(new DOMSource(export), new StreamResult(new File(bodiExportFile)));
        } catch (Exception e) {
            logger.error("UNEXPECTED EXCEPTION: " + e.getMessage());
            e.printStackTrace();
            try {
                sourceConnectionBean.closeConnection();
            } finally {

            }
            throw e;
        }
    }
}
