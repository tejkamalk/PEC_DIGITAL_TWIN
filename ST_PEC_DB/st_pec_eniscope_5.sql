USE [stidata]
GO


/****** Object:  Table [dbo].[ST_PEC_ENISCOPE_5]    Script Date: 22/8/2022 4:14:24 pm ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ST_PEC_ENISCOPE_5](
	[eniscope_meessage_uid] [bigint] NOT NULL,
	[eniscope_uid] [bigint] NOT NULL,
	[mqttc_client] [nvarchar](255) NOT NULL,
	[mid] [nvarchar](1) NULL,
	[state] [nvarchar](1) NULL,
	[qos] [nvarchar](1) NULL,
	[dup] [nvarchar](1) NULL,
	[retain] [nvarchar](1) NULL,
	[topic] [nvarchar](255) NOT NULL,
	[payload] [nvarchar](max) NULL,
	[update_timestamp] [datetime2](7) NOT NULL,
	[update_user] [nvarchar](30) NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[ST_PEC_ENISCOPE_5] ADD  DEFAULT (NEXT VALUE FOR [ST_PEC_ENISCOPE_5_seq]) FOR [eniscope_uid]
GO

ALTER TABLE [dbo].[ST_PEC_ENISCOPE_5] ADD  DEFAULT (NEXT VALUE FOR [st_pec_eniscope_seq]) FOR [eniscope_meessage_uid]
GO

ALTER TABLE [dbo].[ST_PEC_ENISCOPE_5] ADD  DEFAULT (getdate()) FOR [update_timestamp]
GO

ALTER TABLE [dbo].[ST_PEC_ENISCOPE_5] ADD  DEFAULT ('MQTT_CLIENT') FOR [update_user]
GO
