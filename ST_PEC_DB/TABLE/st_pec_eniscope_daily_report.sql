USE [stidata]
GO

/****** Object:  Table [dbo].[ST_PEC_ENISCOPE_DAILY_REPORT]    Script Date: 24/8/2022 10:32:09 am ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ST_PEC_ENISCOPE_DAILY_REPORT](
	[report_date] [date] NOT NULL,
	[seq_no] [int] NOT NULL,
	[eniscope_seq_no] [int] NOT NULL,
	[eniscope_meter_point] [int] NOT NULL,
	[eniscope_meter_name] [nvarchar](30) NOT NULL,
	[energy] [float] NULL,
	[energy_in_units] [nvarchar](30) NULL,
	[energy_cost] [float] NULL,
	[avg_energy_last_week] [nvarchar](30) NULL,
	[avg_energy_last_week_in_units] [nvarchar](30) NULL,
	[percentage_change] [float] NULL,
	[increment_type] [nvarchar](5) NULL,
	[update_timestamp] [datetime2](7) NOT NULL,
	[update_id] [nvarchar](30) NOT NULL
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[ST_PEC_ENISCOPE_DAILY_REPORT] ADD  DEFAULT (getdate()) FOR [update_timestamp]
GO

ALTER TABLE [dbo].[ST_PEC_ENISCOPE_DAILY_REPORT] ADD  DEFAULT (user_name()) FOR [update_id]
GO

ALTER TABLE [dbo].[ST_PEC_ENISCOPE_DAILY_REPORT]  WITH CHECK ADD  CONSTRAINT [increment_type] CHECK  (([increment_type]='DOWN' OR [increment_type]='UP'))
GO

ALTER TABLE [dbo].[ST_PEC_ENISCOPE_DAILY_REPORT] CHECK CONSTRAINT [increment_type]
GO

