USE [stidata]
GO
/****** Object:  Trigger [dbo].[st_pec_eniscope_4_parse]    Script Date: 8/23/2022 6:35:16 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER trigger [dbo].[st_pec_eniscope_4_parse] on [dbo].[ST_PEC_ENISCOPE_4]
FOR INSERT
AS
DECLARE
    @message_id bigint,
	@payload    nvarchar(max),
	@record_time datetime;

BEGIN

	SELECT @message_id = eniscope_uid,
	       @payload    = payload
	  FROM inserted;

    WITH togo AS (SELECT @message_id message_ref_no,
                           data.uid AS unique_indentifier,
                           data.did AS meter_id,
                           data.S1 AS Phase1_Apparent_Power,
                           data.S AS System_Apparent_Power ,
                           data.A1 AS Angle1 ,
                           data.V3 AS Phase3_Voltage ,
                           data.P2 AS Phase2_Power ,
                           data.RE3 AS Phase3_Reactive_Energy ,
                           data.Q3 AS Phase3_Reactive_Power ,
                           data.A2 AS Angle2 ,
                           data.E AS System_Energy ,
                           data.U3 AS Phase3_LinetoLine_Voltage ,
                           data.P3 AS Phase3_Power ,
                           data.REx AS System_Reactive_Energy_Export,
                           data.U1 AS Phase1_LinetoLine_Voltage ,
                           data.V1 AS Phase1_Voltage ,
                           data.RE2 AS Phase2_Reactive_Energy ,
                           data.ts AS server_time ,
                           data.Ex3 AS Phase3_Export_Energy ,
                           data.AE2 AS Phase2_Apparent_Energy ,
                           data.RE AS System_Reactive_Energy ,
                           data.I2 AS Phase2_Current ,
                           data.A3 AS Angle3 ,
                           data.E2 AS Phase2_Energy ,
                           data.REx1 AS Phase1_Reactive_Energy_Export,
                           data.I AS System_Current ,
                           data."In" AS Neutral_Current ,
                           data.S3 AS Phase3_Apparent_Power ,
                           data.U AS System_LinetoLine_Voltage ,
                           data.F AS Frequency ,
                           data.RE1 AS Phase1_Reactive_Energy ,
                           data.P1 AS Phase1_Power ,
                           data.E1 AS Phase1_Energy ,
                           data.PF AS System_Power_Factor ,
                           data.REX3 AS Phase3_Reactive_Energy_Export,
                           data.AE3 AS Phase3_Apparent_Energy ,
                           data.V AS System_Voltage ,
                           data.EX AS System_Export_Energy ,
                           data.Q AS System_Reactive_Power ,
                           data.S2 AS Phase2_Apparent_Power ,
                           data.REX2 AS Phase2_Reactive_Energy_Export,
                           data.PF3 AS Phase3_Power_Factor ,
                           data.PF2 AS Phase2_Power_Factor ,
                           data.E3 AS Phase3_Energy ,
                           data.I3 AS Phase3_Current ,
                           data.PF1 AS Phase1_Power_Factor ,
                           data.P AS System_Power ,
                           data.U2 AS Phase2_LinetoLineVoltage ,
                           data.V2 AS Phase2_Voltage ,
                           data.Q2 AS Phase2_Reactive_Power ,
                           data.Q1 AS Phase1_Reactive_Power ,
                           data.AE1 AS Phase1_Apparent_Energy ,
                           data.Ex2 AS Phase2_Export_Energy ,
                           data.Ex1 AS Phase1_Export_Energy ,
                           data.AE AS System_Apparent_Energy ,
                           data.I1 AS Phase1_Current
                    FROM openjson(@payload)
                    WITH (rtdata nvarchar(MAX) '$.rtdata' AS JSON) AS rtdata
                    CROSS apply openjson (rtdata.rtdata) WITH (uid nvarchar(30),
                                                               did int,
                    										   S1  float,
                    										   S   float,
                    										   A1  float,
                    										   V3  float,
                    										   P2  float,
                    										   RE3 float,
                    										   Q3  float,
                    										   A2  float,
                    										   E   float,
                    										   U3  float,
                    										   P3  float,
                    										   REx float,
                    										   U1  float,
                    										   V1  float,
                    										   RE2 float,
                    										   ts  bigint,
                    										   Ex3 float,
                    										   AE2 float,
                    										   RE  float,
                    										   I2  float,
                    										   A3  float,
                    										   E2  float,
                    										   REx1 float,
                    										   I   float,
                    										   "In" float,
                    										   S3  float,
                    										   U   float,
                    										   F   float,
                    										   RE1 float,
                    										   P1  float,
                    										   E1  float,
                    										   PF  float,
                    										   REx3 float,
                    										   AE3 float,
                    										   V   float,
                    										   Ex  float,
                    										   Q   float,
                    										   S2  float,
                    										   REx2 float,
                    										   PF3 float,
                    										   PF2 float,
                    										   E3  float,
                    										   I3  float,
                    										   PF1 float,
                    										   P   float,
                    										   U2  float,
                    										   V2  float,
                    										   Q2  float,
                    										   Q1  float,
                    										   AE1 float,
                    										   Ex2 float,
                    										   Ex1 float,
                    										   AE  float,
                    										   I1  float)  DATA),
         fina AS (SELECT message_ref_no,
              	         unique_indentifier,
              		     meter_id,
                         DATEADD(S, CONVERT(int,LEFT(server_time, 10)), '1970-01-01') record_time,
                         Neutral_Current,
                         Frequency,
                         System_Voltage,
                         System_Current,
                         System_Power,
                         System_Energy,
                         System_Apparent_Power,
                         System_Apparent_Energy,
                         System_Export_Energy,
                         System_Power_Factor,
                         System_LinetoLine_Voltage,
                         System_Reactive_Energy_Export,
                         System_Reactive_Energy,
                         System_Reactive_Power,
                         Angle1,
                         Phase1_Voltage,
                         Phase1_Current,
                         Phase1_Power,
                         Phase1_Energy,
                         Phase1_Apparent_Power,
                         Phase1_Apparent_Energy,
                         Phase1_Export_Energy,
                         Phase1_Power_Factor,
                         Phase1_LinetoLine_Voltage,
                         Phase1_Reactive_Energy_Export,
                         Phase1_Reactive_Energy,
                         Phase1_Reactive_Power,
                         Angle2,
                         Phase2_Voltage,
                         Phase2_Current,
                         Phase2_Power,
                         Phase2_Energy,
                         Phase2_Apparent_Power,
                         Phase2_Apparent_Energy,
                         Phase2_Export_Energy,
                         Phase2_Power_Factor,
                         Phase2_LinetoLineVoltage,
                         Phase2_Reactive_Energy_Export,
                         Phase2_Reactive_Energy,
                         Phase2_Reactive_Power,
                         Angle3,
                         Phase3_Voltage,
                         Phase3_Current,
                         Phase3_Power,
                         Phase3_Energy,
                         Phase3_Apparent_Power,
                         Phase3_Apparent_Energy,
                         Phase3_Export_Energy,
                         Phase3_Power_Factor,
                         Phase3_LinetoLine_Voltage,
                         Phase3_Reactive_Energy_Export,
                         Phase3_Reactive_Energy,
                         Phase3_Reactive_Power
                    FROM togo)
	INSERT INTO stidata.dbo.st_pec_eniscope_4_meter_data (
	                                                         eniscope_uid_ref,
			                                                 device_uid,
			                                                 device_meter_id,
                                                             Record_time,
                                                             local_datetime,
                                                             local_date,
                                                             local_hour,
                                                             local_min,
                                                             Neutral_Current,
                                                             Frequency,
                                                             System_Voltage,
                                                             System_Current,
                                                             System_Power,
                                                             System_Energy,
                                                             System_Apparent_Power,
                                                             System_Apparent_Energy,
                                                             System_Export_Energy,
                                                             System_Power_Factor,
                                                             System_LinetoLine_Voltage,
                                                             System_Reactive_Energy_Export,
                                                             System_Reactive_Energy,
                                                             System_Reactive_Power,
                                                             Angle1,
                                                             Phase1_Voltage,
                                                             Phase1_Current,
                                                             Phase1_Power,
                                                             Phase1_Energy,
                                                             Phase1_Apparent_Power,
                                                             Phase1_Apparent_Energy,
                                                             Phase1_Export_Energy,
                                                             Phase1_Power_Factor,
                                                             Phase1_LinetoLine_Voltage,
                                                             Phase1_Reactive_Energy_Export,
                                                             Phase1_Reactive_Energy,
                                                             Phase1_Reactive_Power,
                                                             Angle2,
                                                             Phase2_Voltage,
                                                             Phase2_Current,
                                                             Phase2_Power,
                                                             Phase2_Energy,
                                                             Phase2_Apparent_Power,
                                                             Phase2_Apparent_Energy,
                                                             Phase2_Export_Energy,
                                                             Phase2_Power_Factor,
                                                             Phase2_LinetoLineVoltage,
                                                             Phase2_Reactive_Energy_Export,
                                                             Phase2_Reactive_Energy,
                                                             Phase2_Reactive_Power,
                                                             Angle3,
                                                             Phase3_Voltage,
                                                             Phase3_Current,
                                                             Phase3_Power,
                                                             Phase3_Energy,
                                                             Phase3_Apparent_Power,
                                                             Phase3_Apparent_Energy,
                                                             Phase3_Export_Energy,
                                                             Phase3_Power_Factor,
                                                             Phase3_LinetoLine_Voltage,
                                                             Phase3_Reactive_Energy_Export,
                                                             Phase3_Reactive_Energy,
                                                             Phase3_Reactive_Power)
         SELECT message_ref_no,
     	        unique_indentifier,
     		    meter_id,
                record_time,
                record_time AT TIME ZONE 'UTC' AT TIME ZONE 'Singapore Standard Time' AS local_datetime,
     			CAST(FORMAT(record_time AT TIME ZONE 'UTC' AT TIME ZONE 'Singapore Standard Time', 'yyyy-MM-dd') AS DATE) AS local_date,
     			FORMAT(record_time AT TIME ZONE 'UTC' AT TIME ZONE 'Singapore Standard Time', 'HH') AS local_hour,
                FORMAT(record_time AT TIME ZONE 'UTC' AT TIME ZONE 'Singapore Standard Time', 'mm') AS local_min,
                Neutral_Current,
                Frequency,
                System_Voltage,
                System_Current,
                System_Power,
                System_Energy,
                System_Apparent_Power,
                System_Apparent_Energy,
                System_Export_Energy,
                System_Power_Factor,
                System_LinetoLine_Voltage,
                System_Reactive_Energy_Export,
                System_Reactive_Energy,
                System_Reactive_Power,
                Angle1,
                Phase1_Voltage,
                Phase1_Current,
                Phase1_Power,
                Phase1_Energy,
                Phase1_Apparent_Power,
                Phase1_Apparent_Energy,
                Phase1_Export_Energy,
                Phase1_Power_Factor,
                Phase1_LinetoLine_Voltage,
                Phase1_Reactive_Energy_Export,
                Phase1_Reactive_Energy,
                Phase1_Reactive_Power,
                Angle2,
                Phase2_Voltage,
                Phase2_Current,
                Phase2_Power,
                Phase2_Energy,
                Phase2_Apparent_Power,
                Phase2_Apparent_Energy,
                Phase2_Export_Energy,
                Phase2_Power_Factor,
                Phase2_LinetoLineVoltage,
                Phase2_Reactive_Energy_Export,
                Phase2_Reactive_Energy,
                Phase2_Reactive_Power,
                Angle3,
                Phase3_Voltage,
                Phase3_Current,
                Phase3_Power,
                Phase3_Energy,
                Phase3_Apparent_Power,
                Phase3_Apparent_Energy,
                Phase3_Export_Energy,
                Phase3_Power_Factor,
                Phase3_LinetoLine_Voltage,
                Phase3_Reactive_Energy_Export,
                Phase3_Reactive_Energy,
                Phase3_Reactive_Power
    FROM fina;



END
