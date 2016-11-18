create view VW_P_MAIN_DMA_FMZ_AVG_PRES as

	/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
					~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	Script Title:	VW_P_MAIN_DMA_FMZ_AVG_PRES
	Owner: 			Alexander Cook
	Published: 		22/09/2016
	Date Last Run: 	22/09/2016 08:50
	Version:		1.0.0
	Called by:		-

				I 			   | 			O
	---------------------------|-----------------------------
   VW_P_MAINS_DMA_AVG_PRESSURE |     
	---------------------------| VW_P_MAIN_DMA_FMZ_AVG_PRES
	VW_P_DMA_FMZ_AVG_PRESSURE  |
	---------------------------|------------------------------ 

	Purpose:		Creates a view aligning DMA average pressures and FMZ pressures to 
					individual assets

	Change History:
					1.0.0		Initial deployment					ACWC

						TESTING
							1:-		Check output for duplicate ICS_ASSET_ID values.	
																			PASS
							2:-		Check count of DMA within each FMZ with the
									script output.							
																			PASS


########################################################################################
*/


Select ICS_ASSET_ID, dma.ics_dma_id, ics_fmz_id, DMA_AVG_PRESSURE, FMZ_AVG_PRESSURE from 
VW_P_MAINS_DMA_AVG_PRESSURE dma
left join
VW_P_DMA_FMZ_AVG_PRESSURE fmz
on dma.ics_dma_id = fmz.ics_dma_id
