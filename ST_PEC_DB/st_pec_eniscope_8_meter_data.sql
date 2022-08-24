USE [stidata]
GO

/****** Object:  Table [dbo].[st_pec_eniscope_8_meter_data]    Script Date: 22/8/2022 4:16:13 pm ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[st_pec_eniscope_8_meter_data](
	[uid] [bigint] NULL,
	[eniscope_uid_ref] [bigint] NULL,
	[device_uid] [nvarchar](max) NULL,
	[device_meter_id] [int] NULL,
	[Record_time] [datetime] NULL,
	[local_datetime] [datetime] NULL,
	[local_date] [date] NULL,
	[local_hour] [int] NULL,
	[local_min] [int] NULL,
	[Neutral_Current] [float] NULL,
	[Frequency] [float] NULL,
	[System_Voltage] [float] NULL,
	[System_Current] [float] NULL,
	[System_Power] [float] NULL,
	[System_Energy] [float] NULL,
	[System_Apparent_Power] [float] NULL,
	[System_Apparent_Energy] [float] NULL,
	[System_Export_Energy] [float] NULL,
	[System_Power_Factor] [float] NULL,
	[System_LinetoLine_Voltage] [float] NULL,
	[System_Reactive_Energy_Export] [float] NULL,
	[System_Reactive_Energy] [float] NULL,
	[System_Reactive_Power] [float] NULL,
	[Angle1] [float] NULL,
	[Phase1_Voltage] [float] NULL,
	[Phase1_Current] [float] NULL,
	[Phase1_Power] [float] NULL,
	[Phase1_Energy] [float] NULL,
	[Phase1_Apparent_Power] [float] NULL,
	[Phase1_Apparent_Energy] [float] NULL,
	[Phase1_Export_Energy] [float] NULL,
	[Phase1_Power_Factor] [float] NULL,
	[Phase1_LinetoLine_Voltage] [float] NULL,
	[Phase1_Reactive_Energy_Export] [float] NULL,
	[Phase1_Reactive_Energy] [float] NULL,
	[Phase1_Reactive_Power] [float] NULL,
	[Angle2] [float] NULL,
	[Phase2_Voltage] [float] NULL,
	[Phase2_Current] [float] NULL,
	[Phase2_Power] [float] NULL,
	[Phase2_Energy] [float] NULL,
	[Phase2_Apparent_Power] [float] NULL,
	[Phase2_Apparent_Energy] [float] NULL,
	[Phase2_Export_Energy] [float] NULL,
	[Phase2_Power_Factor] [float] NULL,
	[Phase2_LinetoLineVoltage] [float] NULL,
	[Phase2_Reactive_Energy_Export] [float] NULL,
	[Phase2_Reactive_Energy] [float] NULL,
	[Phase2_Reactive_Power] [float] NULL,
	[Angle3] [float] NULL,
	[Phase3_Voltage] [float] NULL,
	[Phase3_Current] [float] NULL,
	[Phase3_Power] [float] NULL,
	[Phase3_Energy] [float] NULL,
	[Phase3_Apparent_Power] [float] NULL,
	[Phase3_Apparent_Energy] [float] NULL,
	[Phase3_Export_Energy] [float] NULL,
	[Phase3_Power_Factor] [float] NULL,
	[Phase3_LinetoLine_Voltage] [float] NULL,
	[Phase3_Reactive_Energy_Export] [float] NULL,
	[Phase3_Reactive_Energy] [float] NULL,
	[Phase3_Reactive_Power] [float] NULL,
	[update_datetime] [datetime] NULL,
	[update_id] [nvarchar](30) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[st_pec_eniscope_8_meter_data] ADD  DEFAULT (NEXT VALUE FOR [st_pec_eniscope_8_meter_data_seq]) FOR [uid]
GO

ALTER TABLE [dbo].[st_pec_eniscope_8_meter_data] ADD  DEFAULT (getdate()) FOR [update_datetime]
GO

ALTER TABLE [dbo].[st_pec_eniscope_8_meter_data] ADD  DEFAULT (user_name()) FOR [update_id]
GO
