DROP TABLE tab_test;

CREATE TABLE tab_test (
	COL_BOOLEAN BOOLEAN,
	--
	COL_TINYINT TINYINT,
	COL_SMALLINT SMALLINT,
	COL_INT INT,
	COL_BIGINT BIGINT,
	--
	COL_DECIMAL DECIMAL(38,38),
	--
	COL_DOUBLE DOUBLE,
	COL_FLOAT FLOAT,
	COL_REAL REAL,
	--
	COL_TIMESTAMP TIMESTAMP,
	--
	COL_STRING string
);

INSERT OVERWRITE TABLE tab_test VALUES (false,0,0,0,0,0,0,0,null,'a');