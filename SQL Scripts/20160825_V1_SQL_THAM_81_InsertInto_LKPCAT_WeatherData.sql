/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
	Owner: 			Alexander Cook
	Published: 		25/08/2016
	Date Last Ran: 	25/08/2016 15:30
	Version:		1.0.0

########################################################################################
*/
INSERT INTO 
  T_I_LKP_CATEGORICAL
  (schema_name, table_name, field_name_1, source_value_1, adms_string_value)
VALUES
  ('BASE', 'T_I_WEATHER_STATION_TS', 'STATION_NAME', 'Brize norton', 'PROVINCES' );
  INSERT INTO 
  T_I_LKP_CATEGORICAL
  (schema_name, table_name, field_name_1, source_value_1, adms_string_value)
VALUES
  ('BASE', 'T_I_WEATHER_STATION_TS', 'STATION_NAME', 'Heathrow', 'LONDON' );

  COMMIT;
