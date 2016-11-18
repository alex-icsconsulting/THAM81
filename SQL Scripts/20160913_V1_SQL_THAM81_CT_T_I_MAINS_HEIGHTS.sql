/*
########################################################################################

					ICS CONSULTING LTD :: THAM81
	Owner: 			Alexander Cook
	Published: 		13/09/2016
	Date Last Ran: 	13/09/2016 12:08
	Version:		1.0.0

	Purpose:		Creates the table T_I_MAINS_HEIGHTS

	Change History:
					1.0.0		Initial Deployment

########################################################################################
*/

CREATE TABLE T_I_MAINS_HEIGHTS
(
ICS_MAINS_HEIGHTS_ID  CHAR(32)  DEFAULT SYS_GUID(),
TABLE_NAME            NVARCHAR2(20),
FIELD_NAME            NVARCHAR2(20),
HEIGHT_VALUE_1        FLOAT,
HEIGHT_VALUE_1_SRC    NVARCHAR2(20),
HEIGHT_VALUE_2        FLOAT,
HEIGHT_VALUE_2_SRC    NVARCHAR2(20),
HEIGHT_VALUE_3        FLOAT,
HEIGHT_VALUE_3_SRC    NVARCHAR2(20)
)