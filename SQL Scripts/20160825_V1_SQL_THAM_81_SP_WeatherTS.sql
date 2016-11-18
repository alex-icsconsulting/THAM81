/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
	Owner: 			Alexander Cook
	Published: 		25/08/2016
	Date Last Ran: 	25/08/2016 15:30
	Version:		1.0.0

########################################################################################
*/
create or replace 
procedure IMP_WEATHER_STATION_DATA (ics_audit_id adms_config.t_i_audit_process_runs.ics_audit_id%type) as
/*
Version history to contain:
Procedure name: IMP_WEATHER_HISTORIC_TS
Date created: 25/08/2016
Current version 1-0
History – Date Amended, Amended By, Comment

*/
V_EXISTS number;
V_PROCESSID varchar2(32);
E_MISSING EXCEPTION;
E_ARCHIVE EXCEPTION;
NSCHEMA VARCHAR2(30) := 'BASE';
V_MSG VARCHAR2(500);
N_VALID1 PLS_INTEGER;
N_VALID2 PLS_INTEGER;
V_MODULE VARCHAR2(100) := 'IMP_WEATHER_STATION_DATA';
V_MODULE1 VARCHAR2(1000);
v_strhistoricDate  VARCHAR2 (10) := 'YYYY-MM-DD';
begin

V_PROCESSID := ADMS_CONFIG.pkg_adms_util.get_process_id(ics_audit_id);
--process_audit
v_module1 := 'STARTING';
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID,'PROCESS STARTED OK','START',NULL , sysdate, null,ics_audit_id);
if ADMS_CONFIG.PKG_ADMS_UTIL.CHECK_REQUIRED_OBJECTS_EXIST(NSCHEMA,V_PROCESSID) then
  RAISE E_MISSING;
end if;

IF NOT archive_data(ICS_AUDIT_ID,V_PROCESSID) THEN
  RAISE E_ARCHIVE;
END IF;
--DATA IMPORT ROUTINE STARTS HERE--

---------------------------------------
--HISTORIC DATA MERGED INTO T_I TABLE--
---------------------------------------
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'START T_R_HISTORIC MERGE', 'START T_R_HISTORIC MERGE', NULL, SYSDATE, NULL,ics_audit_id);

MERGE INTO 
  T_I_WEATHER_STATION_TS wthr
USING
     (
     SELECT
     (Select T_I_WEATHER_STATION.ICS_WEATHER_STATION_ID from base.T_I_WEATHER_STATION where T_R_HISTORIC_WEATHER_DATA.region = base.T_I_WEATHER_STATION.NAME) as ICS_WEATHER_STATION_ID,
      to_date(substr(ENVIRO_DATE,0,10),'YYYY-MM-DD') as dte,
        REGION,
        TOTAL_RAIN_MM,
        TEMP_MAX,
        TEMP_MIN
      FROM
        base_stage.T_R_HISTORIC_WEATHER_DATA
      WHERE  ((TOTAL_RAIN_MM is not Null and TEMP_MAX is not null and TEMP_MIN is not null) or to_date(substr(ENVIRO_DATE,0,10),'YYYY-MM-DD') is not null or region is null)
     ) wthrhis
ON ((wthr.sample_date = wthrhis.dte) and (wthr.station_name = wthrhis.region))
WHEN MATCHED THEN UPDATE SET
  wthr.ICS_WEATHER_STATION_ID = wthrhis.ICS_WEATHER_STATION_ID,
  wthr.STATION_NAME =   wthrhis.REGION,
  wthr.SAMPLE_DATE  =   wthrhis.dte,
  wthr.TEMP_MAX     =   wthrhis.TEMP_MAX,
  wthr.TEMP_MIN     =   wthrhis.TEMP_MIN,
  wthr.RAINFALL     =   wthrhis.TOTAL_RAIN_MM
WHEN NOT MATCHED THEN INSERT
  (wthr.ICS_WEATHER_STATION_ID,
  wthr.STATION_NAME,
  wthr.SAMPLE_DATE,
  wthr.TEMP_MAX,
  wthr.TEMP_MIN,
  wthr.RAINFALL)
VALUES
  (
  wthrhis.ICS_WEATHER_STATION_ID,
  wthrhis.REGION,
  wthrhis.dte,
  wthrhis.TEMP_MAX,
  wthrhis.TEMP_MIN,
  wthrhis.TOTAL_RAIN_MM
  );

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'T_R_HISTORIC MERGED', 'T_R_HISTORIC MERGED', NULL, SYSDATE, NULL,ics_audit_id);
---------------------------
--NEW DATA INTO T_I_TABLE--
---------------------------
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'START: T_R_WEATHER_TIME_SERIES MERGE', 'START: T_R_WEATHER_TIME_SERIES MERGE', NULL, SYSDATE, NULL,ics_audit_id);

MERGE INTO 
  T_I_WEATHER_STATION_TS wthr
USING
     (
      SELECT
        (SELECT 
            T_I_WEATHER_STATION.ICS_WEATHER_STATION_ID 
          FROM 
            BASE.T_I_WEATHER_STATION 
          WHERE (SELECT 
                  adms_string_value 
                FROM 
                  adms_config.T_I_LKP_CATEGORICAL
                WHERE 
                  BASE_STAGE.T_R_WEATHER_TIME_SERIES.WEATHERSTATION = T_I_LKP_CATEGORICAL.source_value_1
                ) = base.T_I_WEATHER_STATION.NAME) as ICS_WEATHER_STATION_ID,
        DATERECEIVED,
      (SELECT 
       adms_string_value
      FROM 
         adms_config.T_I_LKP_CATEGORICAL
        WHERE 
T_R_WEATHER_TIME_SERIES.WEATHERSTATION = T_I_LKP_CATEGORICAL.source_value_1
) as REGION,
        MAX,
        MIN,
        RAIN
      FROM
        BASE_STAGE.T_R_WEATHER_TIME_SERIES
      WHERE  ((RAIN is not Null and MAX is not null and MIN is not null) or DATERECEIVED is not null)
     ) wthrhis
ON ((wthr.sample_date = wthrhis.DATERECEIVED) and (wthr.station_name = wthrhis.region))
WHEN MATCHED THEN UPDATE SET
  wthr.ICS_WEATHER_STATION_ID = wthrhis.ICS_WEATHER_STATION_ID,
  wthr.STATION_NAME =   wthrhis.REGION,
  wthr.SAMPLE_DATE  =   wthrhis.DATERECEIVED,
  wthr.TEMP_MAX     =   wthrhis.MAX,
  wthr.TEMP_MIN     =   wthrhis.MIN,
  wthr.RAINFALL     =   wthrhis.RAIN
WHEN NOT MATCHED THEN INSERT
  (wthr.ICS_WEATHER_STATION_ID,
  wthr.STATION_NAME,
  wthr.SAMPLE_DATE,
  wthr.TEMP_MAX,
  wthr.TEMP_MIN,
  wthr.RAINFALL)
VALUES
  (
  wthrhis.ICS_WEATHER_STATION_ID,
  wthrhis.REGION,
  wthrhis.DATERECEIVED,
  wthrhis.MAX,
  wthrhis.MIN,
  wthrhis.RAIN
);

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'T_R_WEATHER_TIME_SERIES MERGED', 'T_R_WEATHER_TIME_SERIES MERGED', NULL, SYSDATE, NULL,ics_audit_id);






COMMIT;

DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => NSCHEMA, TABNAME => 'T_I_WEATHER_STATION_TS', ESTIMATE_PERCENT=>100);

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'FINISH', 'FINISH', NULL, SYSDATE, NULL,ics_audit_id);
Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'SUCCESS',Null,Sysdate);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'SUCCESS', 'COMPLETED');

EXCEPTION
  WHEN E_MISSING THEN
    ROLLBACK;
    v_msg := 'MISSING DATABASE OBJECTS';
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(    NSCHEMA, V_PROCESSID, V_MODULE1, 'ERROR', V_MSG, SYSDATE, NULL,ics_audit_id);
    Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'ERROR',Null,Sysdate);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'ERROR', 'REQUIRED DATABASE OBJECTS MISSING');
  WHEN E_ARCHIVE THEN
    ROLLBACK;
    v_msg := 'ARCHIVING';
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(    NSCHEMA, V_PROCESSID, V_MODULE1, 'ERROR', V_MSG, SYSDATE, NULL,ics_audit_id);
    Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'ERROR',Null,Sysdate);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'ERROR', 'ERROR ARCHIVING DATA');
  WHEN OTHERS THEN
    ROLLBACK;
    v_msg := substr(sqlerrm, 1, 500);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(    NSCHEMA, V_PROCESSID, V_MODULE1, 'ERROR', V_MSG, SYSDATE, NULL,ics_audit_id);
    Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'ERROR',Null,Sysdate);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'ERROR', v_msg);
end;