use tempdb;

-- TEST
DROP DATABASE TEST;

DROP LOGIN test;

CREATE DATABASE TEST;

CREATE LOGIN test WITH PASSWORD='test', DEFAULT_DATABASE=TEST, CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;

ALTER AUTHORIZATION ON DATABASE::test TO TEST;


-- SUGARCRM
DROP DATABASE sugarcrm;

DROP LOGIN sugarcrm;

CREATE DATABASE sugarcrm;

CREATE LOGIN sugarcrm WITH PASSWORD='sugarcrm', DEFAULT_DATABASE=sugarcrm, CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;

ALTER AUTHORIZATION ON DATABASE::sugarcrm TO sugarcrm;


-- DWH + STAGE 
DROP DATABASE dwh;

DROP LOGIN dwh;

DROP LOGIN dwhreport;

CREATE DATABASE dwh;

CREATE LOGIN dwh WITH PASSWORD='dwh', DEFAULT_DATABASE=dwh, CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;

ALTER AUTHORIZATION ON DATABASE::dwh TO dwh;

CREATE LOGIN dwhreport WITH PASSWORD='dwhreport', DEFAULT_DATABASE=dwh, CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;

ALTER AUTHORIZATION ON DATABASE::dwh TO dwhreport;

use dwh;

create schema stage;
create schema report;