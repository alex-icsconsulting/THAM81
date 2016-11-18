/*
########################################################################################################################

					ICS CONSULTING LTD :: THAM81
	TITLE:			EQUIVILENT SERVICE PIPE BURSTS
	Owner: 			Alexander Cook
	Published: 		
	Date Last Ran: 	NR
	Version:		1.0.0

	Purpose:		THIS THE EQUIVILENT SERVICE PIPE BURSTS:  we will use the 4 year average and apply
					the same weighting methodology as for the deterioration;  # mains * 3.75, 
					# comms * 1, others *.25 – this only 
					 applies to active leakage. Where ‘service relay’ and ‘service repair’ = ‘comms’? 


	Change History:
					1.0.0		Initial Deployment

#######################################################################################################################
*/

#######MAINS PREPROCESSING SCRIPT############

Select 
'MAINS' as Pipe_Type,
a.DMA,
count(*) * 3.75 as ESPB
from 
T_I_MAINS a,
waterinf_stage.T_R_MAIN_REPAIR b
where 
a.client_uid = b.MATCHEDMAINGISID and
b.activitytype = 'AL' and
a.ASSET_FUNCTION in ('DISTRIBUTION MAIN', 'TRUNK_MAIN','CONNECTION_MAIN')
group by 
'MAINS',
a.DMA
;


Select * from WATERINF_STAGE.T_QA_COMMS_REPAIRS where arearef is not null and LEAKAGE_GROUP_DESCRIPTION in ('Active Leakage','Visible Leakage') and NBS_BURSTTYPE_DESC like 'Comm%';
