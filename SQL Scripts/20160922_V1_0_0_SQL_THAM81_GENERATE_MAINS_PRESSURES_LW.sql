create or replace procedure GENERATE_MAINS_PRESSURES_LW (ics_audit_id adms_config.t_i_audit_process_runs.ics_audit_id%type) as

/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
					~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	Script Title:	     GENERATE_MAINS_PRESSURES
	Owner: 			       Alexander Cook
	Published: 		     22/09/2016
	Date Last Ran: 	   22/09/2016 10:20
	Version:		       1.0.0
	Called by:		     JOB_GENERATE_MAINS_PRESSURES

	Tables 	  	|			         I 			    	| 		               	O
	------------|---------------------------|--------------------------
	            | VW_P_MAIN_DMA_FMZ_AVG_PRES|     
	            |---------------------------|	T_P_MAINS_PRESSURE_LW
	            | VW_I_ASSET_CORE           |
	------------|---------------------------|--------------------------- 

	Purpose:		Generates the mains pressure data length weighted

	Change History:
					1.0.0		Initial deployment					ACWC

								All test cases run and passed.
########################################################################################
*/



V_EXISTS number;
V_PROCESSID varchar2(32);
E_MISSING EXCEPTION;
E_ARCHIVE EXCEPTION;
NSCHEMA VARCHAR2(30) := 'WATERINF';
V_MSG VARCHAR2(500);
N_VALID1 PLS_INTEGER;
N_VALID2 PLS_INTEGER;
V_MODULE varchar2(100) := 'T_P_MAINS_PRESSURE_LW
';
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

merge into T_P_MAINS_PRESSURE_LW prod
using(
select t1.ICS_ASSET_ID,
case when MAINS_PRESSURE = -9999 THEN -9999
else round(((t1.MAINS_PRESSURE * t1.MAINS_PRESSURE) / t2.DMA_LENGTH ),6) end as LW_MAINS_PRESSURE
from
(  
  select pres.ICS_ASSET_ID, pres.ICS_DMA_ID, ASSET_LENGTH, MAINS_PRESSURE from
  T_P_MAINS_PRESSURE pres
  left join
  VW_I_ASSET_CORE main
  on pres.ICS_ASSET_ID = main.ICS_ASSET_ID
  where asset_function_bin = 'DISTRIBUTION'
  )t1
  left join
  (Select ICS_DMA_ID, sum(asset_length) dma_length from VW_I_ASSET_CORE group by ICS_DMA_ID) t2
  on t1.ICS_DMA_ID = t2.ICS_DMA_ID
) stage
on (prod.ics_asset_id = stage.ics_Asset_id)
when matched then update set
prod.LW_MAINS_PRESSURE = stage.LW_MAINS_PRESSURE
when not matched then insert
(
prod.ics_asset_id,
prod.lw_mains_pressure
)
values
(
stage.ics_asset_id,
stage.lw_mains_pressure
)
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