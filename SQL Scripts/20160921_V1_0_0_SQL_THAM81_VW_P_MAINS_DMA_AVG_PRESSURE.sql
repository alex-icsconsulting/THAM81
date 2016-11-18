CREATE VIEW VW_P_MAINS_DMA_AVG_PRESSURE AS

/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
					~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	Script Title:	VW_P_MAINS_DMA_AVG_PRESSURE
	Owner: 			Alexander Cook
	Published: 		21/09/2016
	Date Last Run: 	21/09/2016 20:58
	Version:		1.0.0
	Called by:		-

				I 			   | 			O
	---------------------------|-----------------------------
	      VW_I_ASSET_CORE      |     
	---------------------------| VW_P_MAINS_DMA_AVG_PRESSURE
	 	T_I_DMA_AVG_PRESSURE   |
	---------------------------|------------------------------ 

	Purpose:		Creates a view which draws the Average DMA Pressure to each asset
					within each DMA

	Change History:
					1.0.0		Initial deployment					ACWC

						TESTING
							1:-		Check output for duplicate ICS_ASSET_ID values.	
																			PASS
							2:-		Check output for duplicate ICS_DMA_ID values.	
																			PASS
							3:-		Check count of assets within each DMA with the
									script output.							
																			PASS


########################################################################################
*/

Select ICS_ASSET_ID, core.ICS_DMA_ID, DMA_AVG_PRESSURE from 
VW_I_ASSET_CORE core
left join
T_I_DMA_AVG_PRESSURES dma
on core.ics_dma_id = dma.ics_dma_id
where dma.ics_dma_id is not null
order by core.ics_dma_id;