create or replace procedure IMP_FMZ_AVG_PRESSURE(ics_audit_id adms_config.t_i_audit_process_runs.ics_audit_id%type) as

/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
					~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	Script Title:	IMP_FMZ_AVG_PRESSURE
	Owner: 			Alexander Cook
	Published: 		21/09/2016
	Date Last Run: 	21/09/2016 20:12
	Version:		1.1.0
	Called by:		JOB_IMP_FMZ_AVG_PRESSURE

	Tables 		|			I 				| 			O
	------------|---------------------------|--------------------------
	            |        T_I_DMA     		|     
	            |---------------------------|	T_I_FMZ_AVG_PRESSURE
	            | t_r_zonal_avg_pressures   |
	------------|---------------------------|--------------------------- 

	Purpose:		Merges the FMZ Average values post data loading. The ICS_FMZ_ID 
					is assigned using a left join

	Change History:
					1.0.0		Initial deployment					ACWC

						TESTING
							1:-		Check merge statement output for duplicate 
									ICS_FMZ_ID values.						PASS
							2:-		Check for duplicates in output table
																			FAIL
									Actions Taken
										1:- Truncate T_I_FMZ_AVG_PRESSURE	PASS
										2:-	Run Merge statement				PASS
										3:-	Run Test Case Two				PASS
										4:-	Run Merge statement				PASS
										5:-	Run Test Case Two				PASS
										6:-	Run ADMS Process 				PASS
										7:-	Run Test Case Two				PASS

									Test case failure attributed to values left in 
									table post deployment. Version 1.0.1 issued. 

					1.0.1		Post deployment update		21/09/16		ACWC
					
								No Changes made, version updated in response to 
								V1.0.0 failure


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
V_MODULE varchar2(100) := 'IMP_FMZ_AVG_PRESSURE';
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

MERGE INTO T_I_FMZ_AVG_PRESSURE prod
using
(
Select ICS_fmz_ID,  AVG_PRESSURE as  FMZ_AVG_PRESSURE
from
waterinf_stage.t_r_zonal_avg_pressures a
left join
T_I_fmz b
on a.zcode = b.client_uid
) stage
on (prod.ics_fmz_id = stage.ics_fmz_id)
when matched then update set
prod.FMZ_AVG_PRESSURE = stage.FMZ_AVG_PRESSURE
when not matched then insert
(prod.ics_fmz_id, prod.FMZ_AVG_PRESSURE)
values
(stage.ics_fmz_id, stage.FMZ_AVG_PRESSURE);


COMMIT;

DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => NSCHEMA, TABNAME => 'T_I_FMZ_AVG_PRESSURE', ESTIMATE_PERCENT=>100);

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'FINISH', 'FINISH', NULL, SYSDATE, NULL,ics_audit_id);
Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'SUCCESS',Null,Sysdate);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'SUCCESS', 'T_I_FMZ_AVG_PRESSURE IMPORTED');

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