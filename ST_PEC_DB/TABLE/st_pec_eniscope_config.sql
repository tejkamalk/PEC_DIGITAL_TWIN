USE [stidata]
GO


/****** Object:  Table [dbo].[ST_PEC_ENISCOPE_CONFIG]    Script Date: 22/8/2022 4:14:24 pm ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ST_PEC_ENISCOPE_CONFIG](
    [seq_no] [int] NOT NULL,
	[eniscope_seq_no] [int] NOT NULL,
	[eniscope_meter_name] [nvarchar](30) NOT NULL,
	[eniscope_uid] [nvarchar](30) NOT NULL,
	[eniscope_meter_point] [int] NOT NULL,
	[eniscope_meter_status] [nvarchar](1) NOT NULL,
	[eniscope_model] [nvarchar](30) NOT NULL,
	[eniscope_ip] [nvarchar](30) NOT NULL,
	[update_timestamp] [datetime2](7) NOT NULL,
	[update_user] [nvarchar](30) NOT NULL
	)
GO

ALTER TABLE [dbo].[ST_PEC_ENISCOPE_CONFIG] ADD  DEFAULT (getdate()) FOR [update_timestamp]
GO

ALTER TABLE [dbo].[ST_PEC_ENISCOPE_CONFIG] ADD  DEFAULT ('DBO') FOR [update_user]
GO


--SQLCODE

INSERT INTO [dbo].[ST_PEC_ENISCOPE_CONFIG] (seq_no,
             eniscope_seq_no,
			 eniscope_meter_name,
			 eniscope_uid,
			 eniscope_meter_point,
			 eniscope_meter_status,
			 eniscope_model,
			 eniscope_ip
			 )
	 VALUES (1,
	         1,
			 'PEC_AHU1-1',
			 '68:27:19:84:01:e8:00:01',
			 1,
			 'Y',
			 'ENISCOPE 4',
			 '192.168.7.11'
			 ),
			(2,
			 2,
			 'AHU1-2',
			 '68:27:19:bd:93:38:00:01',
			 1,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.12'
			),
			(3,
			 3,
			 'PEC_AHU2-1',
			 '68:27:19:bd:f8:a2:00:01',
			 1,
			 'Y',
			 'ENISCOPE 4',
			 '192.168.7.13'
			),
			(4,
			 4,
			 'PEC_AHU2-2(1)',
			 '68:27:19:be:1c:49:00:01',
			 1,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.14'
			),
			(5,
			 4,
			 'PEC_AHU2-2(2)',
			 '68:27:19:be:1c:49:00:02',
			 2,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.14'
			),
			(6,
			 5,
			 'PEC_AHU3-1',
			 '68:27:19:bd:f8:d0:00:01',
			 1,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.15'
			),
			(7,
			 6,
			 'PEC_AHU3-2',
			 '80:1f:12:5a:6e:99:00:01',
			 1,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.16'
			),
			(8,
			 6,
			 'PEC_AHU3-2(A)',
			 '80:1f:12:5a:6e:99:00:02',
			 2,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.16'
			),
			(9,
			 7,
			 'PEC_AHU4-1',
			 '68:27:19:be:42:7c:00:01',
			 1,
			 'Y',
			 'ENISCOPE 4',
			 '192.168.7.17'
			),
		    (10,
			 7,
			 'PEC_AHU4-2',
			 '68:27:19:be:42:7c:00:02',
			 2,
			 'Y',
			 'ENISCOPE 4',
			 '192.168.7.17'
			),
			(11,
			 8,
			 'CHR-1',
			 '68:27:19:84:9d:8c:00:01',
			 1,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.18'
			),
			(12,
			 8,
			 'CHR-2',
			 '68:27:19:84:9d:8c:00:02',
			 2,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.18'
			),
			(13,
			 8,
			 'CHR-3',
			 '68:27:19:84:9d:8c:00:03',
			 3,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.18'
			),
			(14,
			 8,
			 'CHR-4',
			 '68:27:19:84:9d:8c:00:04',
			 4,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.18'
			),
			(15,
			 8,
			 'CHR-5',
			 '68:27:19:84:9d:8c:00:05',
			 5,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.18'
			),
			(16,
			 8,
			 'CHR-6',
			 '68:27:19:84:9d:8c:00:06',
			 6,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.18'
			),
			(17,
			 8,
			 'CHR-7',
			 '68:27:19:84:9d:8c:00:07',
			 7,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.18'
			),
			(18,
			 9,
			 'CHWP-1',
			 '68:27:19:bd:ce:bf:00:01',
			 1,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.19'
			),
			(19,
			 9,
			 'CHWP-2',
			 '68:27:19:bd:ce:bf:00:02',
			 2,
			 'Y',
			 'ENISCOPE 8',
			 '192.168.7.19'
			);