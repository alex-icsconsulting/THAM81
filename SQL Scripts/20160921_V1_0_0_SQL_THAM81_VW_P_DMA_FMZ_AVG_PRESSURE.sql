CREATE VIEW VW_P_DMA_FMZ_AVG_PRESSURE AS (

	/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
					~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	Script Title:	VW_P_DMA_FMZ_AVG_PRESSURE
	Owner: 			Alexander Cook
	Published: 		21/09/2016
	Date Last Run: 	21/09/2016 21:25
	Version:		1.0.0
	Called by:		-

				I 			   | 			O
	---------------------------|-----------------------------
	          T_I_FMZ          |     
	---------------------------| VW_P_DMA_FMZ_AVG_PRESSURE
		T_I_FMZ_AVG_PRESSURE   |
	---------------------------|------------------------------ 

	Purpose:		Creates a view which draws the Average FMZ Pressure to each DMA
					within each FMZ

	Change History:
					1.0.0		Initial deployment					ACWC

						TESTING
							1:-		Check output for duplicate ICS_DMA_ID values.	
																			PASS
							2:-		Check output for duplicate ICS_FMZ_ID values.	
																			PASS
							3:-		Check count of DMA within each FMZ with the
									script output.							
																			PASS


########################################################################################
*/

Select distinct fmzpres.ICS_FMZ_ID, dma.ICS_DMA_ID, FMZ_AVG_PRESSURE
from T_I_FMZ_AVG_PRESSURE fmzpres
left join
T_I_FMZ fmz
on fmzpres.ics_fmz_id = fmz.ics_fmz_id
left join
T_I_DMA dma
on fmz.client_uid=dma.FMZ_ID 
where fmzpres.ICS_FMZ_ID is not null and dma.ICS_DMA_ID is not null
)