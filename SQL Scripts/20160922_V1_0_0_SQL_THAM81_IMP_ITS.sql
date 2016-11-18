create or replace procedure IMP_ITS (ics_audit_id adms_config.t_i_audit_process_runs.ics_audit_id%type) as
/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
					~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	Script Title:	     IMP_ITS
	Owner: 			       Alexander Cook
	Published: 		     22/09/2016
	Date Last Ran: 	   22/09/2016 15:23
	Version:		       1.0.0
	Called by:		     JOB_IMP_ITS

	Tables 	  	|			         I 			    	| 		               	O
	------------|---------------------------|--------------------------
	            |                           |     
	            |---------------------------|	          T_I_ITS
	            |          *ITS             |
	------------|---------------------------|--------------------------- 

	Purpose:		Imports all ITS data basd on the newest table

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
V_MODULE varchar2(100) := 'IMP_ITS';
V_MODULE1 VARCHAR2(1000);

  indx number:=1;
  
  TYPE col_name is TABLE OF NVARCHAR2(50)
    INDEX BY PLS_INTEGER;
  TYPE col_name_child is TABLE OF NVARCHAR2(50)
    INDEX BY PLS_INTEGER;    
    v_array_master col_name;
  v_array_child col_name_child;
  i       integer;
  v_array_child_counter integer;
  v_element nvarchar2(50);
  v_list  nvarchar2(50);
  v_insert_statement varchar2(10000) :='INSERT INTO T_I_ITS (';
  v_select_statement  varchar2(10000) :='SELECT ';
  v_create_tble varchar2(10000):= q'{CREATE TABLE T_I_ITS (}';
  CURSOR recs_cur
  IS
    Select table_name from all_tables where table_name like '%R_ITS%' order by regexp_substr(table_name,'\d+') desc;

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
 --TRUNCATE AND DROP T_I_ITS 
  execute immediate 'TRUNCATE TABLE T_I_ITS';
   execute immediate 'DROP TABLE T_I_ITS PURGE';
  --Loop through each tbale name that is R_ITS...% This will be ordered desc.
  FOR recs_cur_x
  IN recs_cur
  LOOP
    IF indx = 1 THEN
     --dbms_output.put_line ('XXX' || recs_cur_x.table_name);
      --Obtain all column names into v_array_master. This will be used later to see if other tables have the same column name.
      select column_name bulk collect into v_array_master from all_tab_cols where table_name = recs_cur_x.table_name;
        FOR i IN 1 .. v_array_master.COUNT
          LOOP
      --    DBMS_OUTPUT.put_line ( 'COL_NUMBER' || i || ' is NAME ' || v_array_master ( i));
          IF i = v_array_master.count Then
             v_create_tble:= v_create_tble || v_array_master ( i) || q'{ NVARCHAR2(2000))}';
          ELSE
            IF v_array_master ( i) like '%DATE%' THEN
              v_create_tble:= v_create_tble || v_array_master ( i) || q'{ NVARCHAR2(2000),}';
            ELSE
              v_create_tble:= v_create_tble || v_array_master ( i) || q'{ NVARCHAR2(2000),}';
            END IF;
            
          END IF;
         END LOOP;
         --DBMS_OUTPUT.put_line ( v_create_tble);
         
         execute immediate v_create_tble;
         commit;
         execute immediate 'insert into T_I_ITS (select * from waterinf_stage.' || recs_cur_x.table_name || ')';
         commit;
    ELSE
      --dbms_output.put_line (recs_cur_x.table_name);
      select column_name bulk collect into v_array_child from all_tab_cols where table_name = recs_cur_x.table_name;
v_array_child_counter:=1;
FOR v_array_child_counter IN 1 .. v_array_child.COUNT
  LOOP

    v_element:= v_array_child(v_array_child_counter);
    --look for element in master array
    FOR i IN 1 .. v_array_master.COUNT
      LOOP
      IF v_element = v_array_master(i) THEN
        v_insert_statement:= v_insert_statement ||  v_element|| ', ';
        v_select_statement:= v_select_statement ||  v_element || ', ';                 
     END IF;
  END LOOP; 
           
i:=0;
END LOOP;

          v_select_statement:= substr (v_select_statement,0,length(v_select_statement)-2);
          v_insert_statement:= substr (v_insert_statement,0,length(v_insert_statement)-2);
          v_insert_statement:= v_insert_statement || ') (' || v_select_statement || ' from waterinf_stage.'|| recs_cur_x.table_name ||')';
         -- DBMS_OUTPUT.put_line (v_insert_statement);
          execute immediate  v_insert_statement;
          commit;
  v_insert_statement:='INSERT INTO T_I_ITS (';
  v_select_statement:='SELECT ';
          --DBMS_OUTPUT.put_line (v_insert_statement);

    END IF;
    indx := indx +1;
  END LOOP;
COMMIT;

DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => NSCHEMA, TABNAME => 'T_I_ITS', ESTIMATE_PERCENT=>100);

ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_LOG(        NSCHEMA, V_PROCESSID, 'FINISH', 'FINISH', NULL, SYSDATE, NULL,ics_audit_id);
Adms_Config.Pkg_Adms_Util.Write_To_Audit_Finish(Ics_Audit_Id,'SUCCESS',Null,Sysdate);
ADMS_CONFIG.PKG_ADMS_UTIL.WRITE_TO_AUDIT_VALIDATION(Ics_Audit_Id, 'SUCCESS', 'ITS_IMPORTS_COMPLETE');

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