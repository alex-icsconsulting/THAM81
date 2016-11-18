/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
	Owner: 			Alexander Cook
	Published: 		13/0/2016
	Date Last Ran: 	25/08/2016 15:30
	Version:		1.1.0

	Change History:
					1.0.0		Initial Deployment
					1.1.0		Script changed to use Global tempoary tables. 
								Data is truncated at the begining and end of the script
								Table names have been changed as follows:
									T_TMP_REG1   	-> 		T_GTT_BOROUGH_DMA_INTERACT
									T_TMP_REG2		->		T_GTT_BOROUGH_LONGEST_SECT_DMA
									T_TMP_REG3		->		T_GTT_BOUOUGH_DMA_GREATEST
									T_TMP_REG4		->		T_GTT_BOROUGH_PREPROCESSED   **** DROPEED ****


								Field names have been changed as follows:	
								T_GTT_BOROUGH_DMA_INTERACT
								--------------------------
									HOWBIG			->		AREA

								T_GTT_BOROUGH_LONGEST_SECT_DMA
								------------------------------
									BIGGEST			->		AREA_MAX

########################################################################################
*/

create or replace procedure ALLOCATE_BOROUGH_TO_DMA(ics_audit_id adms_config.t_i_audit_process_runs.ics_audit_id%type) as
V_EXISTS number;
V_PROCESSID varchar2(32);
E_MISSING EXCEPTION;
E_ARCHIVE EXCEPTION;
NSCHEMA VARCHAR2(30) := 'WATERINF';
V_MSG VARCHAR2(500);
N_VALID1 PLS_INTEGER;
N_VALID2 PLS_INTEGER;
V_MODULE varchar2(100) := 'ALLOCATE_BOROUGH_TO_DMA';
V_MODULE1 VARCHAR2(1000);
begin

V_PROCESSID := ADMS_CONFIG.pkg_adms_util.get_process_id(ics_audit_id);
--process_audit
v_module1 := 'STARTING';
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	NSCHEMA, V_PROCESSID,'PROCESS STARTED OK','START',NULL , sysdate, null,ics_audit_id);
if ADMS_CONFIG.PKG_ADMS_UTIL.CHECK_REQUIRED_OBJECTS_EXIST(NSCHEMA,V_PROCESSID) then
  RAISE E_MISSING;
end if;


IF NOT archive_data(ICS_AUDIT_ID,V_PROCESSID) THEN
  RAISE E_ARCHIVE;
END IF;

--TRUNCATE TABLES IN CASE OF OLD DATA BEING PRESENT
EXECUTE IMMEDIATE q'[TRUNCATE TABLE T_GTT_BOROUGH_DMA_INTERACT]';
EXECUTE IMMEDIATE q'[TRUNCATE TABLE T_GTT_BOROUGH_LONGEST_SECT_DMA]';
EXECUTE IMMEDIATE q'[TRUNCATE TABLE T_GTT_BOUOUGH_DMA_GREATEST]';
COMMIT;
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF', V_PROCESSID, 'TRUNCATION - COMPLETED', 'SUCCESS', null , sysdate, null, ICS_AUDIT_ID);
--PROCEDURE START
EXECUTE IMMEDIATE q'[INSERT INTO T_GTT_BOROUGH_DMA_INTERACT 
						(ICS_DISTRICT_BOROUGH_ID, ICS_DMA_ID, REGION, NAME, AREA)
						(SELECT 
							a.ICS_DISTRICT_BOROUGH_ID
							b.ics_dma_Id, A.NAME as region,  
							b.name, SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(b.GEOM, a.GEOM, .05), .05) howbig
						  FROM 
						  	base.T_I_DISTRICT_BOROUGH a,
						    t_i_dma b
						  WHERE 
						  	sdo_anyinteract(b.GEOM, a.GEOM)= 'TRUE')]';
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF', V_PROCESSID, 'SPATIAL PROCESS 1 - COMPLETED', 'SUCCESS', null , sysdate, null, ICS_AUDIT_ID);
--DELETE FROM THE TABLE ANY NULL VALUES OR ZERO VALUES
EXECUTE IMMEDIATE q'[DELETE FROM T_GTT_BOROUGH_DMA_INTERACT WHERE AREA IS NULL OR AREA = 0]';
COMMIT;
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF', V_PROCESSID, 'SPATIAL PROCESS 2 - COMPLETED', 'SUCCESS', null , sysdate, null, ICS_AUDIT_ID);
--FIND THE LONGEST SECTION PER MAIN
EXECUTE IMMEDIATE q'[INSERT INTO T_GTT_BOROUGH_LONGEST_SECT_DMA 
						(ICS_DMA_ID, AREA_MAX)
						(SELECT 
							Ics_dma_id,
						  	MAX(AREA) biggest
						 FROM 
						  	t_GTT_BOROUGH_DMA_INTERACT
						  GROUP BY 
						  	ics_dma_id
						)]';
COMMIT;						
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF', V_PROCESSID, 'SPATIAL PROCESS 3 - COMPLETED', 'SUCCESS', null , sysdate, null, ICS_AUDIT_ID);
--ALLOCATE DMA TO LONGEST SECTION OF MAIN
EXECUTE IMMEDIATE q'[INSERT INTO T_GTT_BOROUGH_DMA_GREATEST 
					(ICS_DMA_ID, ICS_BOROUGH_ID)
					(
					SELECT 
						a.ics_dma_id,
					    a.ICS_DISTRICT_BOROUGH_ID
					 FROM 
					  	T_GTT_BOROUGH_DMA_INTERACT a,
					    T_GTT_BOROUGH_LONGEST_SECT_DMA b
					  WHERE 
					  	a.ics_dma_id = b.ics_dma_id AND
					  	a.area = b.area_max
					);]';
COMMIT;					

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	'WATERINF', V_PROCESSID, 'SPATIAL PROCESS 4 - COMPLETED', 'SUCCESS', null , sysdate, null, ICS_AUDIT_ID);


execute immediate q'[truncate table T_P_DMA_BOROUGH]';
execute immediate q'[insert into T_P_DMA_BOROUGH (select * from T_GTT_BOROUGH_DMA_GREATEST where rowid in (select first_value(rowid) over (partition by ics_dma_id order by rowid) from T_GTT_BOROUGH_DMA_GREATEST))]';



COMMIT;

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	NSCHEMA, V_PROCESSID, 'FINISH', 'FINISH', NULL, SYSDATE, NULL,ics_audit_id);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_FINISH(ICS_AUDIT_ID,'SUCCESS',null,sysdate);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'SUCCESS', 'ALLOCATED BOROUGH TO DMA');

EXCEPTION
  WHEN E_MISSING THEN
    ROLLBACK;
    v_msg := 'MISSING DATABASE OBJECTS';
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	NSCHEMA, V_PROCESSID, V_MODULE1, 'ERROR', V_MSG, SYSDATE, NULL,ics_audit_id);
    Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'ERROR',Null,Sysdate);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'ERROR', 'REQUIRED DATABASE OBJECTS MISSING');
  WHEN E_ARCHIVE THEN
    ROLLBACK;
    v_msg := 'ARCHIVING';
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	NSCHEMA, V_PROCESSID, V_MODULE1, 'ERROR', V_MSG, SYSDATE, NULL,ics_audit_id);
    Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'ERROR',Null,Sysdate);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'ERROR', 'ERROR ARCHIVING DATA');
  WHEN OTHERS THEN
    ROLLBACK;
    v_msg := substr(sqlerrm, 1, 500);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(	NSCHEMA, V_PROCESSID, V_MODULE1, 'ERROR', V_MSG, SYSDATE, NULL,ics_audit_id);
    Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'ERROR',Null,Sysdate);
    ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(ICS_AUDIT_ID, 'ERROR', V_MSG);
end;