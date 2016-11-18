create or replace procedure IMP_DMA_AVG_PRESSURE(ics_audit_id adms_config.t_i_audit_process_runs.ics_audit_id%type) as
/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
					~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	Script Title:	IMP_DMA_AVG_PRESSURE
	Owner: 			Alexander Cook
	Published: 		13/09/2016
	Date Last Ran: 	21/09/2016 19:34
	Version:		1.1.0
	Called by:		JOB_IMP_DMA_AVG_PRESSURE

	Tables 		|			I 			| 			O
	------------|-----------------------|--------------------------
	            |        T_I_DMA        |     
	            |-----------------------|	T_I_DMA_AVG_PRESSURES
	            | T_R_DMA_AVG_PRESSURES |
	------------|-----------------------|--------------------------- 

	Purpose:		Mergesthe DMA Average values post data loading. The ICS_DMA_ID 
					is assigned using a left join

	Change History:
					1.0.0		Initial deployment					ACWC

							Version 1.0.0 failed the following test cases:
										a. Duplicate values preset in ICS_DMA_ID
										reason: The table was not cleaned post deployment
												thus leaving not required ICS_DMA_IDs 
												still present. Table truncated and ADMS 
												routine run, test case passes. 

					1.1.0		Post deployment update	21/09/16	ACWC

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
V_MODULE varchar2(100) := 'IMP_DMA_AVG_PRESSURE';
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



--when on full data set this table requires truncating!!!
merge into T_I_DMA_AVG_PRESSURES prod
using
(
Select ICS_DMA_ID, BEST_ESTIMATE_OF_DMA_HEAD__M_ as DMA_AVG_PRESURE from 
waterinf_stage.T_R_DMA_AVG_PRESSURES a
left join
T_I_DMA prod
on a.DMA = prod.CLient_UID
) stage
on (prod.ICS_DMA_ID = stage.ICS_DMA_ID)
when matched then update set
prod.DMA_AVG_PRESSURE = stage.DMA_AVG_PRESURE
when not matched then insert
(prod.ICS_DMA_ID,
prod.DMA_AVG_PRESSURE)
values
(stage.ICS_DMA_ID,
stage.DMA_AVG_PRESURE);



COMMIT;

DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => NSCHEMA, TABNAME => 'T_I_DMA_AVG_PRESSURES', ESTIMATE_PERCENT=>100);

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'FINISH', 'FINISH', NULL, SYSDATE, NULL,ics_audit_id);
Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'SUCCESS',Null,Sysdate);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'SUCCESS', 'DMA AVG PRESS IMP COMPLETE');

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