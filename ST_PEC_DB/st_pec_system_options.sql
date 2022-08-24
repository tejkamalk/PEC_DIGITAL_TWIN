USE [stidata]
GO


/****** Object:  Table [dbo].[ST_PEC_SYSTEM_OPTIONS]    Script Date: 22/8/2022 4:14:24 pm ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ST_PEC_SYSTEM_OPTIONS](
    [power_cost] float NOT NULL,
	[power_cost_currency]nvarchar(5) NOT NULL
) 
GO

INSERT INTO [dbo].[ST_PEC_SYSTEM_OPTIONS] (power_cost, power_cost_currency)
  VALUES ('0.00', 'SGD')
