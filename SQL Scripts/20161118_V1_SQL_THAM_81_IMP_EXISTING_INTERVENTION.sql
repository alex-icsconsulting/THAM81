








create or replace procedure          IMP_EXISTING_INTERVENTION (ics_audit_id adms_config.t_i_audit_process_runs.ics_audit_id%type) as
/******************************************************************************
** Name: IMP_EXISTING_INTERVENTION
** ID  : 
** Desc: 
** Auth: ALEXANDER CW COOK
** Rvd:  
** Date: 10/11/2016
** Copyright ICS Consulting Ltd 2016
*******************************************************************************
** Change History
*******************************************************************************
** Date	        Author              Description	
** ----------   -----------------   --------------------------------------
** 
******************************************************************************/

V_EXISTS number;
V_PROCESSID varchar2(32);
E_MISSING EXCEPTION;
E_ARCHIVE EXCEPTION;
NSCHEMA VARCHAR2(30) := 'WATERINF';
V_MSG VARCHAR2(500);
N_VALID1 PLS_INTEGER;
N_VALID2 PLS_INTEGER;
V_MODULE varchar2(100) := 'IMP_EXISTING_INTERVENTION';
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

DELETE FROM T_I_EXISTING_INTERVENTION;

INSERT INTO T_I_EXISTING_INTERVENTION (ICS_ASSET_ID, CLIENT_UID, EXISTINGINTERVENTION)
SELECT 
  ICS_ASSET_ID,
  CLIENT_UID,
  coalesce (case when UPPER(AMP6MONITORNOTES) like '%HYDROGUARD%' THEN 'Ashridge Hydroguard'
          when UPPER(AMP6MONITORNOTES) like '%SYRINIX%' THEN 'Syrinix Trunkminder'
          when UPPER(AMP6MONITORNOTES) like '%ALARP%' THEN 'Syrinix Trunkminder'
           when UPPER(AMP6MONITORNOTES) like '%LUL%' THEN 'Ashridge Hydroguard'
  END, to_char(AIM.ExistingIntervention), 'None')  EXISTINGINTERVENTION 

FROM VW_I_ASSET_CORE core
LEFT JOIN waterinf_stage.T_R_EXISTING_INTERVENTION EI on  core.CLIENT_UID = EI.GISID
LEFT JOIN WATERINF_STAGE.T_R_HISTORIC_AIM_INFILL AIM on core.CLIENT_UID = AIM.MAINGISID
where ASSET_FUNCTION_BIN = 'TRUNK'

;


COMMIT;

DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => NSCHEMA, TABNAME => 'T_I_EXISTING_INTERVENTION', ESTIMATE_PERCENT=>100);

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'FINISH', 'FINISH', NULL, SYSDATE, NULL,ics_audit_id);
Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'SUCCESS',Null,Sysdate);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'SUCCESS', 'EI COMPLETE');

EXCEPTION
  WHEN E_MISSING THEN
    ROLLBACK;
    v_msg := 'MISSING DATABASE OBJECTS';
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(    NSCHEMA, V_PROCESSID, V_MODULE1, 'ERROR', V_MSG, SYSDATE, NULL,ics_audit_id);
    Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'ERROR',Null,Sysdate);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'ERROR', 'T_I_EXISTING_INTERVENTION IMPORTED');
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
