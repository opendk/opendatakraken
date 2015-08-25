DROP TABLE test.tab_test;
--
CREATE TABLE test.tab_test(
	COL_BOOLEAN	BOOLEAN,
	COL_DECIMAL	DECIMAL(36,30),
	COL_DOUBLE DOUBLE,
	COL_CHAR CHAR(100),
	COL_VARCHAR	VARCHAR(2000000),
	COL_DATE DATE,
	COL_TIMESTAMP TIMESTAMP,
	COL_TIMESTAMPLTZ TIMESTAMP WITH LOCAL TIME ZONE,
	COL_INTERVALDS INTERVAL DAY TO SECOND,
	COL_INTERVALYM INTERVAL YEAR TO MONTH,
	COL_GEOMETRY	GEOMETRY
);