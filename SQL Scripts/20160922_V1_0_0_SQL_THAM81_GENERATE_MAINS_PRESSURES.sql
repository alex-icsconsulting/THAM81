create or replace procedure GENERATE_MAINS_PRESSURES (ics_audit_id adms_config.t_i_audit_process_runs.ics_audit_id%type) as

/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
					~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	Script Title:	GENERATE_MAINS_PRESSURES
	Owner: 			Alexander Cook
	Published: 		22/09/2016
	Date Last Ran: 	22/09/2016 09:16
	Version:		1.0.0
	Called by:		JOB_GENERATE_MAINS_PRESSURES

	Tables 		|			I 				| 			O
	------------|---------------------------|--------------------------
	            | VW_P_MAIN_DMA_FMZ_AVG_PRES|     
	            |---------------------------|	T_P_MAINS_PRESSURE
	            | T_P_MAINS_HEIGHTS         |
	------------|---------------------------|--------------------------- 

	Purpose:		Generates the mains pressure data

	Change History:
					1.0.0		Initial deployment					ACWC

								All test cases run and passed.
########################################################################################
*/



V_EXISTS number;
V_PROCESSID varchar2(32);
E_MISSING EXCEPTION;
E_ARCHIVE EXCEPTION;
NSCHEMA VARCHAR2(30) := '<SCHEMA>';
V_MSG VARCHAR2(500);
N_VALID1 PLS_INTEGER;
N_VALID2 PLS_INTEGER;
V_MODULE varchar2(100) := '???';
V_MODULE1 VARCHAR2(1000);
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

merge into T_P_MAINS_PRESSURE prod
using
(
Select asset.ICS_ASSET_ID, asset.ICS_DMA_ID, asset.ICS_FMZ_ID, asset.DMA_AVG_PRESSURE, asset.FMZ_AVG_PRESSURE, coalesce(  HEIGHT_VALUE_1, HEIGHT_VALUE_2,HEIGHT_VALUE_3 )as height, 
case 
  when DMA_AVG_PRESSURE is null then FMZ_AVG_PRESSURE-coalesce(  HEIGHT_VALUE_1, HEIGHT_VALUE_2,HEIGHT_VALUE_3 )
  else 
    case 
      when coalesce(  HEIGHT_VALUE_1, HEIGHT_VALUE_2,HEIGHT_VALUE_3 ) is null then -9999
      else DMA_AVG_PRESSURE-coalesce(  HEIGHT_VALUE_1, HEIGHT_VALUE_2,HEIGHT_VALUE_3 )
      end
end as MAINS_PRESSURE
from 
VW_P_MAIN_DMA_FMZ_AVG_PRES asset
left join
T_P_MAINS_HEIGHTS hi
on 
asset.ics_asset_id = hi.ics_asset_id
) stage
on (prod.ics_asset_id = stage.ics_asset_id)
when matched then update set

prod.ICS_DMA_ID = stage.ICS_DMA_ID,
prod.ICS_FMZ_ID = stage.ICS_FMZ_ID,
prod.DMA_AVG_PRESSURE = stage.DMA_AVG_PRESSURE,
prod.FMZ_AVG_PRESSURE = stage.FMZ_AVG_PRESSURE,
prod.HEIGHT = stage.HEIGHT,
prod.MAINS_PRESSURE = stage.MAINS_PRESSURE

when not matched then insert
(prod.ICS_ASSET_ID,
prod.ICS_DMA_ID,
prod.ICS_FMZ_ID,
prod.DMA_AVG_PRESSURE,
prod.FMZ_AVG_PRESSURE,
prod.HEIGHT,
prod.MAINS_PRESSURE)
VALUES
(stage.ICS_ASSET_ID,
stage.ICS_DMA_ID,
stage.ICS_FMZ_ID,
stage.DMA_AVG_PRESSURE,
stage.FMZ_AVG_PRESSURE,
stage.HEIGHT,
stage.MAINS_PRESSURE)
;



COMMIT;

DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => NSCHEMA, TABNAME => '<TABLE>', ESTIMATE_PERCENT=>100);

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'FINISH', 'FINISH', NULL, SYSDATE, NULL,ics_audit_id);
Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'SUCCESS',Null,Sysdate);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'SUCCESS', 'EXECUTED SATIS - version 1_0_0');

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