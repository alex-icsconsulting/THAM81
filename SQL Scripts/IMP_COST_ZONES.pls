create or replace PROCEDURE          "IMP_COST_ZONES" 
(p_runid ADMS_CONFIG.T_I_AUDIT_PROCESS_RUNS.ICS_AUDIT_ID%TYPE) as


/******************************************************************************
** Name: IMP_COST_ZONES
** ID  : 1
** Desc: IMP_COST_ZONES
** Auth: FORD
** Date: 26/09/2016
** Copyright ICS Consulting Ltd 2016
*******************************************************************************
** Change History
*******************************************************************************
** Date	        Author              Description	
** ----------   -----------------   --------------------------------------
** 
** 
******************************************************************************/


e_missing exception;
v_msg nvarchar2(512);
v_user nvarchar2(100) := null; --no longer used
v_pid varchar2(32);

V_EXISTS number;
V_PROCESSID varchar2(32);

e_archive exception;

N_VALID1 PLS_INTEGER;
N_VALID2 PLS_INTEGER;
V_MODULE varchar2(100) := 'IMP_MAINS_NODES';
V_MODULE1 VARCHAR2(1000);

-- altered 11/11/14 DF to use merges instead of delete+insert
V_COUNT PLS_INTEGER;


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
BEGIN

--allocate auditing values
v_pid := adms_config.pkg_adms_util.get_process_id(p_runid);

--process_audit
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF', v_pid, 'START - PROCESS BEGAN OK', 'START',null, sysdate, v_user, p_runid);
if ADMS_CONFIG.PKG_ADMS_UTIL.CHECK_REQUIRED_OBJECTS_EXIST(	'WATERINF', v_pid) then
  raise e_missing;
end if;

/*
-- DROP OLD TABLES
Select count(*) into v_exists from user_tables where table_name = 'T_I_COST_ZONE';
if v_exists > 0 then 
  execute immediate q'[drop table T_I_COST_ZONE purge]';
end if;
-- DROP OLD INDEX

SELECT COUNT(*) INTO V_COUNT FROM USER_INDEXES WHERE INDEX_NAME = 'IDX_I_COST_ZONE';
IF V_COUNT = 1 THEN
  execute immediate q'[drop index IDX_I_COST_ZONE]';
END IF;

execute immediate q'[
CREATE TABLE T_I_COST_ZONE AS
(SELECT SYS_GUID() AS ICS_COST_ZONE_ID, DMANAME, DMAAREACODE, MAINS_COST_ZONE, 'SPATIAL RELATIONSHIP' AS NOTE FROM WATERINF_STAGE.T_R_COST_ZONES)]';

--CREATE INDEX
execute immediate q'[CREATE INDEX IDX_COST_ZONE ON T_I_COST_ZONE(ICS_COST_ZONE_ID)]';
*/

delete from T_I_COST_ZONE;
INSERT INTO T_I_COST_ZONE
SELECT SYS_GUID() AS ICS_COST_ZONE_ID, MAINS_COST_ZONE, GEOM FROM WATERINF_STAGE.T_R_COST_ZONES;

--rebuild index on results table
execute immediate q'[alter index SDX_I_COST_ZONE rebuild]';
 execute immediate q'[alter index IDX_I_COST_ZONE rebuild]';
 
 --process audit
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF', v_pid, 'FINISH', 'FINISH', NULL, sysdate, v_user, p_runid);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_FINISH(P_RUNID, 'SUCCESS', null, sysdate);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(P_RUNID, 'SUCCESS',  'PROCESS COMPLETED');
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
EXCEPTION

--process audit
when e_missing then
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF', v_pid, 'START', 'ERROR', 'REQUIRED DATABASE OBJECTS MISSING', sysdate, v_user, p_runid);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_FINISH(P_RUNID, 'ERROR',  'REQUIRED DATABASE OBJECTS MISSING', sysdate);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(P_RUNID, 'ERROR', 'REQUIRED DATABASE OBJECTS MISSING');
when others then
    v_msg := substr(DBMS_UTILITY.format_error_backtrace, 1, 500);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF',v_pid, 'FINISH', 'ERROR', v_msg, sysdate, v_user, p_runid);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_FINISH(P_RUNID, 'ERROR', v_msg, sysdate);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(P_RUNID, 'ERROR',  'PROCESS FAILED - PLEASE CONTACT SUPPORT');
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
END;