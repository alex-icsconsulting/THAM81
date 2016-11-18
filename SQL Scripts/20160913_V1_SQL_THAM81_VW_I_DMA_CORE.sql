/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
	Owner: 			
	Published: 		
	Date Last Ran: 	-
	Version:		1.0.0

	Purpose:		Percentage of PMA per DMA

	Change History:
					1.0.0		Initial Deployment

########################################################################################

*/


select 
 
-- BASE DMAs 
A.ICS_DMA_H_ID ICS_DMA_ID,
CASE
  WHEN A.DMA IS NULL THEN TO_NCHAR('UNKN')
  ELSE A.DMA
END
-- BUILDINGS AND DWELLINGS
,   NVL(B.BUILDING_COUNT, 0) BUILDING_COUNT, NVL(B.DWELLING_COUNT, 0)DWELLING_COUNT,
-- RESIDUAL LEAKAGE
 NVL((12*B.DWELLING_COUNT),0) TW_COMMS_RES_LEAKAGE,
    NVL((25*B.DWELLING_COUNT),0) CS_COMMS_RES_LEAKAGE,
    NVL((23*B.DWELLING_COUNT),0) MAINS_RES_LEAKAGE
-- LENGTHS
, C.DISTRIBUTION_MAIN_LENGTH, C.TRUNK_MAIN_LENGTH, C.CONNECTION_MAIN_LENGTH
--
,D.COMMS_TW_LEN, D.COMMS_CS_LEN
FROM 

T_I_DMA_HYDRAULIC A
LEFT JOIN
T_I_DMA_LEAKAGE_PROPERTIES B
ON A.DMA = B.DMA
LEFT JOIN 
T_P_DMA_LENGTHS C
ON A.DMA = C.DMA
LEFT JOIN
(
SELECT DMA, SUM(NVL(COMMS_TW_LEN, 0)) COMMS_TW_LEN, SUM(NVL(COMMS_CS_LEN, 0)) COMMS_CS_LEN FROM VW_I_ASSET_CORE
GROUP BY DMA) D

ON A.DMA = D.DMA