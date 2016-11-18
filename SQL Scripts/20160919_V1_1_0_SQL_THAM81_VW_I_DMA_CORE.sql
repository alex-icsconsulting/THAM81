
  CREATE OR REPLACE FORCE EDITIONABLE VIEW "WATERINF"."VW_I_DMA_CORE" ("ICS_DMA_ID", "DMA", "BUILDING_COUNT", "DWELLING_COUNT", "TW_COMMS_RES_LEAKAGE", "CS_COMMS_RES_LEAKAGE", "MAINS_RES_LEAKAGE", "DISTRIBUTION_MAIN_LENGTH", "TRUNK_MAIN_LENGTH", "CONNECTION_MAIN_LENGTH", "COMMS_TW_LEN", "COMMS_CS_LEN", "PMA_FLAG") AS 
  SELECT 
  A.ics_dma_id,
  A.dma,
  A.building_count,
  A.dwelling_count,
  A.tw_comms_res_leakage,
  A.cs_comms_res_leakage,
  A.mains_res_leakage,
  A.distribution_main_length,
  A.trunk_main_length,
  A.connection_main_length,
  A.comms_tw_len,
  A.comms_cs_len,
  CASE WHEN (e.sum_length / a.distribution_main_length)>=0.5 THEN 1 
        ELSE -1 
        END AS pma_flag
    FROM (
      SELECT 
      -- BASE DMAs 
      A.ics_dma_h_id ics_dma_id,
      CASE
        WHEN A.dma IS NULL THEN to_nchar('UNKN')
        ELSE A.dma
      END AS dma
      -- BUILDINGS AND DWELLINGS
      ,   nvl(b.building_count, 0) building_count, nvl(b.dwelling_count, 0)dwelling_count,
      -- RESIDUAL LEAKAGE
       nvl((12*b.dwelling_count),0) tw_comms_res_leakage,
          nvl((25*b.dwelling_count),0) cs_comms_res_leakage,
          nvl((23*b.dwelling_count),0) mains_res_leakage
      -- LENGTHS
      , c.distribution_main_length, c.trunk_main_length, c.connection_main_length
      --
      ,d.comms_tw_len, d.comms_cs_len
      FROM 
      
      t_i_dma_hydraulic A
      LEFT JOIN
      t_i_dma_leakage_properties b
      ON A.dma = b.dma
      LEFT JOIN 
      t_p_dma_lengths c
      ON A.dma = c.dma
      LEFT JOIN
      (
      SELECT dma, sum(nvl(comms_tw_len, 0)) comms_tw_len, sum(nvl(comms_cs_len, 0)) comms_cs_len FROM vw_i_asset_core
      GROUP BY dma) d
      ON A.dma = d.dma
      
      )A
      --############### INCLUDED 19/09/2016 @ 13:07. V1 BACKUP TAKEN - ACWC
      LEFT JOIN
      (
      
        SELECT y.dma, sum(y.asset_length) AS sum_length FROM (
          SELECT 
             A.ics_pma_id, 
             b.ics_asset_id 
          FROM 
            t_i_pma A,     
            t_i_mains b   
          WHERE 
            sdo_relate(b.geom, A.geom, 'mask=ANYINTERACT')= 'TRUE'
            )x, 
            vw_i_asset_core y
        WHERE 
          x.ics_asset_id = y.ics_asset_id
        GROUP BY 
          y.dma
        ) e
      ON A.dma = e.dma;
