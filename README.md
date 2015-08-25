# Open Data Kraken

The Open Data Kraken is a suite of tools to facilitate every day tasks in business intelligence and data warehousing projects.

The software is in pre-release status.

# Installation

Download the latest release and unpack it in a folder you like.
The command line tool is in the _/bin_ folder.

## JDBC Drivers

JDBC drivers should be placed in the _/lib/jdbc_ folder. Drivers for Hive already reside in the _/lib/hadoop_ folder.

## Data sources

Files containing connection parameters to datasources should be placed in the _/datasources_ folder.

Datasource files must have the extension _.properties_.

## Logs

Logs are generated in the folder _/log_.

# Features

At the moment only these features are supported:

## Print help
Command: help

Example (on Windows):
```
bin\openbi help
```
## Generate TOAD project file
Command: toadproject

Options:
- toadprojectname = name of the project and of the .tpr file
- toadprojectfolder = folder where the project file is generated
- toadprojectfileslocation = location of the included files

Example (on Windows):
```
bin\openbi toadproject ^
	-toadprojectname oracle-dwh-objects ^
    -toadprojectfolder D:/DEV/projects/TOAD_projects ^
    -toadprojectfileslocation D:/DEV/projects/SVN/oracle-dwh-objects
```
## Print generic RDBMS properties
Command: dbproperties

Options:
- dbdriverclass = class name of jdbc driver
- dbconnectionurl = db connection url
- dbusername = db login user
- dbpassword = db password
- dbconnpropertyfile = file in folder _datasources_ containing the above parameters
- dbconnkeywordfile file in folder _conf_ containing a list of db reserved keywords

Example (on Windows):
```
bin\openbi dbproperties ^
	-dbconnpropertyfile localhost_mysql_test
```
## Copy a table or an entire schema from a RDBMS to another
Command: tablecopy

Options:

_source connection_
- srcdbdriverclass = class name of jdbc driver
- srcdbconnectionurl = db connection url
- srcdbusername = db login user
- srcdbpassword = db password
- srcdbconnpropertyfile = file in folder _datasources_ containing the above parameters. The file must have the _.properties_ extension but must be referenced without extension in the command line call.
- srcdbconnkeywordfile file in folder _conf_ containing a list of db reserved keywords

_target connection_
- tha above 6 parameters with trg... instead of src...

_other options_
- sourceschema = source schema to copy (optional if sourcetable given)
- sourcetable = source table, if not given, all tables of the schema are copied
- targetschema = target schema
- targettable = target table (optional)

- trgcreate = set true if target table is to be created
- dropifexists = set true if table has to be dropped and recreated
- trgpreservedata = set true if target table must not be emptied
- commitfrequency = rows for each commit

Example (on Windows):
```
bin\openbi tablecopy ^
	-srcdbconnpropertyfile localhost_mysql_test ^
	-sourcetable tab_test ^
	-trgdbconnpropertyfile localhost_mysql_dwhstage ^
	-targetschema dwhstage ^
	-targettable stg_mys_tab_test
```