
CREATE TABLE IF NOT EXISTS DATA_MODEL_PROCESS_DEV.STUDENT (
        `ID` BIGINT COMMENT "",
	`NAME` STRING COMMENT "",
	`SEX` STRING COMMENT "")
    COMMENT ""
    PARTITIONED BY (DS STRING)
    LIFECYCLE 5;
    
CREATE TABLE IF NOT EXISTS DATA_MODEL_PROCESS_DEV.STUDENT1 (
        `ID` BIGINT COMMENT "",
	`NAME` STRING COMMENT "",
	`SEX` STRING COMMENT "")
    COMMENT ""
    PARTITIONED BY (DS STRING)
    LIFECYCLE 5;
    
CREATE TABLE IF NOT EXISTS DATA_MODEL_PROCESS_DEV.MYTESTTABLE (
        `BIGINT1` BIGINT COMMENT "",
	`BLOB1` STRING COMMENT "姓名",
	`CHAR20` STRING COMMENT "性别",
	`DATE1` STRING COMMENT "",
	`DATETIME1` DATETIME COMMENT "",
	`DECIMAL1` DECIMAL COMMENT "",
	`DOUBLE1` DOUBLE COMMENT "",
	`FLOAT1` DOUBLE COMMENT "",
	`INTEGER1` BIGINT COMMENT "",
	`SMALLINT1` SMALLINT COMMENT "",
	`TEXT1` STRING COMMENT "",
	`TIME1` STRING COMMENT "",
	`TIMESTAMP1` STRING COMMENT "",
	`TINYINT1` TINYINT COMMENT "",
	`VARCHAR200` STRING COMMENT "")
    COMMENT ""
    PARTITIONED BY (DS STRING)
    LIFECYCLE 5;
    
CREATE TABLE IF NOT EXISTS DATA_MODEL_PROCESS_DEV.TEST (
        `ID` BIGINT COMMENT "",
	`NAME` STRING COMMENT "",
	`SEX` STRING COMMENT "")
    COMMENT ""
    PARTITIONED BY (DS STRING)
    LIFECYCLE 5;
    