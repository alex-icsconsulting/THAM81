create or replace PROCEDURE ALLOCATE_SOILS_TO_MAINS (p_runid ADMS_CONFIG.T_I_AUDIT_PROCESS_RUNS.ICS_AUDIT_ID%TYPE) AS

/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
	Owner: 			Alexander Cook
	Published: 		13/0/2016
	Date Last Ran: 	26/09/2016 15:56
	Version:		1.1.0

	Change History:
        1.0.0 -- 	      Initial Deployment



########################################################################################

*/

v_exists number;
E_MISSING EXCEPTION;
e_archive exception;
v_check1 number;
v_check2 number;
v_msg nvarchar2(500);
v_user nvarchar2(100) := null; --no longer used
v_pid varchar2(32);
v_result boolean;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
BEGIN

--allocate auditing values
v_pid := adms_config.pkg_adms_util.get_process_id(p_runid);

--process_audit
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF', v_pid, 'START - PROCESS STARTED OK', 'START', null, sysdate, v_user, p_runid);
if ADMS_CONFIG.PKG_ADMS_UTIL.CHECK_REQUIRED_OBJECTS_EXIST(	'WATERINF',v_pid) then
  raise e_missing;
end if;

--archive data d mills 08/08/2014
IF NOT archive_data(p_runid,v_pid) THEN
  RAISE E_ARCHIVE;
end if;

  execute immediate q'[
                      INSERT INTO T_GTT_MAINS_SOIL_INTERACT (
                      SELECT 
                        a.ICS_SOIL_CORR_FE_ID,    
                        b.ics_asset_id,    
                        SDO_GEOM.SDO_LENGTH(SDO_GEOM.SDO_INTERSECTION(b.GEOM, a.GEOM, .05), .05) howlong
                      FROM 
                        base.T_I_SOILS a,
                        T_I_MAINS b
                      WHERE 
                        sdo_anyinteract(b.GEOM, a.GEOM )= 'TRUE' )]';
                        
  execute immediate q'[Delete from T_GTT_MAINS_SOIL_INTERACT where interact_size is null or interact_size = 0]';
  
  execute immediate q'[  INSERT INTO T_GTT_MAINS_SOIL_MAX (ICS_ASSET_ID, MAX_INTERACT)
                      (SELECT 
                        ICS_ASSET_ID,
                        MAX(interact_size) biggest
                      FROM 
                        T_GTT_MAINS_SOIL_INTERACT
                      GROUP BY 
                        ICS_ASSET_ID)]';
                        
execute immediate q'[
                      INSERT INTO T_GTT_MAINS_SOIL_ALLOCATE
                      (SELECT 
                      B.ics_asset_id,
                      a.ICS_SOIL_CORR_FE_ID
                      FROM  
                      T_GTT_MAINS_SOIL_MAX B
                      LEFT JOIN                    
                      T_GTT_MAINS_SOIL_INTERACT a
                      ON
                      B.ics_asset_id = A.ics_asset_id 
                      AND a.interact_size = b.MAX_INTERACT)]';
                      
execute immediate q'[iNSERT INTO T_GTT_MAINS_SOIL_STAGE
                     (
                     sELECT B.ICS_SOIL_CORR_FE_ID, A.ics_asset_id, CORR_fe, sswell
                     FROM
                     T_GTT_MAINS_SOIL_ALLOCATE A
                     LEFT JOIN
                     BASE.t_i_SOILS B
                     ON A.ICS_ASSET_id = B.ICS_SOIL_CORR_FE_ID)]';                      
 
execute immediate q'[insert into t_p_SOILS (select ICS_ASSET_ID, FE, CORR from T_GTT_MAINS_SOIL_STAGE where rowid in (select first_value(rowid) over (partition by ics_asset_id order by rowid) from T_GTT_MAINS_SOIL_STAGE))   ]';                
  
--process audit
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF', v_pid, 'SPATIAL PROCESS - COMPLETED', 'SUCCESS', null , sysdate, v_user, p_runid);


--copy results into persistent table, crudely removing duplicate sewer IDs along the way
commit;




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
WHEN E_ARCHIVE THEN
    ROLLBACK;
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF', V_PID, 'ARCHIVING', 'ERROR', 'ERROR ARCHIVING DATA', SYSDATE, NULL,P_RUNID);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_FINISH(P_RUNID,'ERROR','ERROR ARCHIVING DATA',sysdate);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(p_runid, 'ERROR', 'ERROR ARCHIVING DATA');    
when others then
    v_msg := substr(DBMS_UTILITY.format_error_backtrace, 1, 500);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF',v_pid, 'FINISH', 'ERROR', v_msg, sysdate, v_user, p_runid);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_FINISH(P_RUNID, 'ERROR', v_msg, sysdate);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(P_RUNID, 'ERROR',  'PROCESS FAILED - PLEASE CONTACT SUPPORT');
    
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------   
END;