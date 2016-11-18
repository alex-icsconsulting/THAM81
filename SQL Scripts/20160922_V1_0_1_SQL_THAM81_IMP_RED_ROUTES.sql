create or replace procedure IMP_RED_ROUTES (ics_audit_id adms_config.t_i_audit_process_runs.ics_audit_id%type) as
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
NSCHEMA VARCHAR2(30) := 'BASE';
V_MSG VARCHAR2(500);
N_VALID1 PLS_INTEGER;
N_VALID2 PLS_INTEGER;
V_MODULE varchar2(100) := 'IMP_RED_ROUTES';
V_MODULE1 VARCHAR2(1000);

typSpatial_Order adms_config.Spatial_Order;

begin

V_PROCESSID := ADMS_CONFIG.pkg_adms_util.get_process_id(ics_audit_id);
--process_audit
v_module1 := 'STARTING';
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(NSCHEMA, V_PROCESSID,'PROCESS STARTED OK','START',NULL , sysdate, null,ics_audit_id);
if ADMS_CONFIG.PKG_ADMS_UTIL.CHECK_REQUIRED_OBJECTS_EXIST(NSCHEMA,V_PROCESSID) then
  RAISE E_MISSING;
end if;

IF NOT archive_data(ICS_AUDIT_ID,V_PROCESSID) THEN
  RAISE E_ARCHIVE;
END IF;

MERGE into T_I_RED_ROUTES prod
using
(
Select GEODB_OID,
OBJECTID,
UNIQUE_ID,
DATASET,
TOID,
NOT_USED,
ROADLABEL,
DESCTERM,
NATURE,
TLRN_DATE,
ROAD_TYPE,
ITN_DATE,
SHAPE_LENGTH,
GEOM
from BASE_STAGE.T_R_RED_ROUTES os
where SDO_ANYINTERACT(OS.GEOM, BASE.GET_IMPORT_AREA()) = 'TRUE'                              
                             ORDER BY mdsys.hhencode_bylevel (nvl(sdo_geom.sdo_centroid(sdo_geom.sdo_buffer(sdo_geom.sdo_mbr(geom),1,1),0.05).sdo_point.x,450000), typSpatial_Order.order_xmin, typSpatial_Order.ORDER_xmax, 27, 
                                                            nvl(sdo_geom.sdo_centroid(sdo_geom.sdo_buffer(sdo_geom.sdo_mbr(geom),1,1),0.05).sdo_point.y,200000),  typSpatial_Order.ORDER_ymin,  typSpatial_Order.Order_YMAX, 26)) stage
ON (prod.GEODB_OID = stage.GEODB_OID and prod.OBJECTID = stage.UNIQUE_ID)                                                              
when matched then update set

prod.DATASET = stage.DATASET,
prod.TOID = stage.TOID,
prod.NOT_USED = stage.NOT_USED,
prod.ROADLABEL = stage.ROADLABEL,
prod.DESCTERM = stage.DESCTERM,
prod.NATURE = stage.NATURE,
prod.TLRN_DATE = stage.TLRN_DATE,
prod.ROAD_TYPE = stage.ROAD_TYPE,
prod.ITN_DATE = stage.ITN_DATE,
prod.SHAPE_LENGTH = stage.SHAPE_LENGTH,
prod.GEOM = stage.GEOM

when not matched then insert
(
prod.GEODB_OID,
prod.OBJECTID,
prod.CLIENT_UID,
prod.DATASET,
prod.TOID,
prod.NOT_USED,
prod.ROADLABEL,
prod.DESCTERM,
prod.NATURE,
prod.TLRN_DATE,
prod.ROAD_TYPE,
prod.ITN_DATE,
prod.SHAPE_LENGTH,
prod.GEOM
)
VALUES
(
stage.GEODB_OID,
stage.OBJECTID,
stage.UNIQUE_ID,
stage.DATASET,
stage.TOID,
stage.NOT_USED,
stage.ROADLABEL,
stage.DESCTERM,
stage.NATURE,
stage.TLRN_DATE,
stage.ROAD_TYPE,
stage.ITN_DATE,
stage.SHAPE_LENGTH,
stage.GEOM
);



COMMIT;

DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => NSCHEMA, TABNAME => 'T_I_RED_ROUTES', ESTIMATE_PERCENT=>100);

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'FINISH', 'FINISH', NULL, SYSDATE, NULL,ics_audit_id);
Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'SUCCESS',Null,Sysdate);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'SUCCESS', 'RED_ROUTES COMPLETED');

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