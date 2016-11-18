/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
	Owner: 			
	Published: 		
	Date Last Ran: 	-
	Version:		1.0.0

	Purpose:		GENERATE PMA FLAG

	Change History:
					1.0.0		Initial Deployment

########################################################################################

*/




--Identifying the hydraulic DMA for each PMA

create or replace procedure GENERATE_PMA_FLAG (ics_audit_id adms_config.t_i_audit_process_runs.ics_audit_id%type) as
/*
Version history to contain:
Procedure name: ???
Date created: dd/mm/yyyy
Current version 1-0
History – Date Amended, Amended By, Comment

*/
V_EXISTS number;
V_PROCESSID varchar2(32);
E_MISSING EXCEPTION;
E_ARCHIVE EXCEPTION;
NSCHEMA VARCHAR2(30) := 'WATERINF';
V_MSG VARCHAR2(500);
N_VALID1 PLS_INTEGER;
N_VALID2 PLS_INTEGER;
V_MODULE varchar2(100) := 'GENERATE_PMA_FLAG';
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

Merge into T_P_PMA_FLAG prod
using
(
Select 
  pma.ics_dma_id, 
  pma_length, 
  DMA_ASSET_LENGTH,
  CASE 
    WHEN (pma_length / DMA_ASSET_LENGTH)>=0.5 THEN 1
    ELSE 0
  END AS pma_flag
from
  t_p_pma_dma_length pma
left join
  (select 
    ics_dma_id,  
    sum(asset_length) as DMA_ASSET_LENGTH
  from 
    vw_i_asset_core 
  where 
    asset_function_bin = 'DISTRIBUTION'  
  group by ics_dma_id ) asslen
on pma.ics_dma_id = asslen.ics_dma_id
) stage
ON (prod.ICS_DMA_ID = stage.ICS_DMA_ID)
when matched then update set
prod.pma_length = stage.pma_length,
prod.DMA_ASSET_LENGTH = stage.DMA_ASSET_LENGTH,
prod.PMA_FLAG = stage.PMA_FLAG
when not matched then insert
(prod.ics_dma_id,
prod.pma_length,
prod.DMA_ASSET_LENGTH,
prod.PMA_FLAG)
values
(stage.ics_dma_id,
stage.pma_length,
stage.DMA_ASSET_LENGTH,
stage.PMA_FLAG);


COMMIT;

DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => NSCHEMA, TABNAME => 'T_P_PMA_FLAG', ESTIMATE_PERCENT=>100);

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'FINISH', 'FINISH', NULL, SYSDATE, NULL,ics_audit_id);
Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'SUCCESS',Null,Sysdate);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'SUCCESS', 'FLAGS GENERATED');

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