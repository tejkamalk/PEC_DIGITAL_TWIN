USE [stidata]
GO
/****** Object:  StoredProcedure [dbo].[st_pec_eniscope_daily_report_proc]    Script Date: 29/8/2022 9:57:46 am ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[st_pec_eniscope_daily_report_proc]
	@Error_message nvarchar(MAX)  OUTPUT
AS

DECLARE
    @Err_Message nvarchar(MAX),
	@Err_Severity int,
	@Err_State int;

	   BEGIN TRY

	      MERGE [dbo].[ST_PEC_ENISCOPE_DAILY_REPORT_ARCHIVE] Target
          USING [dbo].[ST_PEC_ENISCOPE_DAILY_REPORT] Source
             ON (    Source.report_date          = Target.report_date
                 AND Source.seq_no               = Target.seq_no
         	     AND Source.eniscope_seq_no      = Target.eniscope_seq_no
         	     AND Source.eniscope_meter_point = Target.eniscope_meter_point
         	     AND Source.eniscope_meter_name  = Target.eniscope_meter_name)
           WHEN NOT MATCHED BY Target THEN
         INSERT (report_date,
                 seq_no,
         		 eniscope_seq_no,
         		 eniscope_meter_point,
          		 eniscope_meter_name,
         		 energy,
         		 energy_in_units,
         		 energy_cost,
         		 avg_energy_last_week,
         		 avg_energy_last_week_in_units,
         		 percentage_change,
         		 increment_type,
         		 update_timestamp,
         		 update_id)
         VALUES (Source.report_date,
                 Source.seq_no,
          		 Source.eniscope_seq_no,
         		 Source.eniscope_meter_point,
         		 Source.eniscope_meter_name,
         		 Source.energy,
         		 Source.energy_in_units,
         		 Source.energy_cost,
         		 Source.avg_energy_last_week,
         		 Source.avg_energy_last_week_in_units,
         		 Source.percentage_change,
         		 Source.increment_type,
         		 Source.update_timestamp,
         		 Source.update_id)
            WHEN MATCHED THEN
           UPDATE SET
                  Target.energy	                        = Source.energy,
                  Target.energy_in_units		            = Source.energy_in_units,
         	      Target.energy_cost	                    = Source.energy_cost,
         		  Target.avg_energy_last_week	            = Source.avg_energy_last_week,
         		  Target.avg_energy_last_week_in_units	= Source.avg_energy_last_week_in_units,
         		  Target.percentage_change	            = Source.percentage_change,
         		  Target.increment_type                 	= Source.increment_type,
         		  Target.update_timestamp	                = Source.update_timestamp,
         		  Target.update_id	                    = Source.update_id,
         		  Target.archive_timestamp                = GETDATE(),
         		  Target.archive_id                       = USER_NAME();

		 PRINT ('ROWS Merged INTO ST_PEC_ENISCOPE_DAILY_REPORT_ARCHIVE :'+CAST(@@ROWCOUNT AS nvarchar(30)));

		 DELETE
		   FROM [dbo].[ST_PEC_ENISCOPE_DAILY_REPORT];

		 PRINT ('ROWS Deleted FROM ST_PEC_ENISCOPE_DAILY_REPORT :'+CAST(@@ROWCOUNT AS nvarchar(30)));


        WITH eniscope_1 AS (SELECT DISTINCT device_uid,
                                   device_meter_id,
        						   local_date,
        						   SUM(ISNULL(system_energy,1)) OVER (PARTITION BY device_uid, device_meter_id, local_date) total_system_energy
                              FROM ST_PEC_ENISCOPE_1_METER_DATA
        					 WHERE local_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
        					  ),
             eniscope_2 AS (SELECT DISTINCT device_uid,
                                   device_meter_id,
        						   local_date,
        						   SUM(ISNULL(system_energy,1)) OVER (PARTITION BY device_uid, device_meter_id, local_date) total_system_energy
                              FROM ST_PEC_ENISCOPE_2_METER_DATA
        					 WHERE local_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
        					  ),
        	 eniscope_3 AS (SELECT DISTINCT device_uid,
                                   device_meter_id,
        						   local_date,
        						   SUM(ISNULL(system_energy,1)) OVER (PARTITION BY device_uid, device_meter_id, local_date) total_system_energy
                              FROM ST_PEC_ENISCOPE_3_METER_DATA
        					 WHERE local_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
        					  ),
        	 eniscope_4 AS (SELECT DISTINCT device_uid,
                                   device_meter_id,
        						   local_date,
        						   SUM(ISNULL(system_energy,1)) OVER (PARTITION BY device_uid, device_meter_id, local_date) total_system_energy
                              FROM ST_PEC_ENISCOPE_4_METER_DATA
        					 WHERE local_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
        					  ),
        	 eniscope_5 AS (SELECT DISTINCT device_uid,
                                   device_meter_id,
        						   local_date,
        						   SUM(ISNULL(system_energy,1)) OVER (PARTITION BY device_uid, device_meter_id, local_date) total_system_energy
                              FROM ST_PEC_ENISCOPE_5_METER_DATA
        					 WHERE local_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
        					  ),
        	 eniscope_6 AS (SELECT DISTINCT device_uid,
                                   device_meter_id,
        						   local_date,
        						   SUM(ISNULL(system_energy,1)) OVER (PARTITION BY device_uid, device_meter_id, local_date) total_system_energy
                              FROM ST_PEC_ENISCOPE_6_METER_DATA
        					 WHERE local_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
        					  ),
        	 eniscope_7 AS (SELECT DISTINCT device_uid,
                                   device_meter_id,
        						   local_date,
        						   SUM(ISNULL(system_energy,1)) OVER (PARTITION BY device_uid, device_meter_id, local_date) total_system_energy
                              FROM ST_PEC_ENISCOPE_7_METER_DATA
        					 WHERE local_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
        					  ),
        	 eniscope_8 AS (SELECT DISTINCT device_uid,
                                   device_meter_id,
        						   local_date,
        						   SUM(ISNULL(system_energy,1)) OVER (PARTITION BY device_uid, device_meter_id, local_date) total_system_energy
                              FROM ST_PEC_ENISCOPE_8_METER_DATA
        					 WHERE local_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
        					  ),
        	 eniscope_9 AS (SELECT DISTINCT device_uid,
                                   device_meter_id,
        						   local_date,
        						   SUM(ISNULL(system_energy,1)) OVER (PARTITION BY device_uid, device_meter_id, local_date) total_system_energy
                              FROM ST_PEC_ENISCOPE_9_METER_DATA
        					 WHERE local_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
        					  ),
        	 all_eniscope AS (SELECT device_uid,
        	                         device_meter_id,
        							 local_date,
        							 total_system_energy
        					    FROM eniscope_1 e1
        					   UNION
        					  SELECT device_uid,
        	                         device_meter_id,
        							 local_date,
        							 total_system_energy
        						FROM eniscope_2 e2
        					   UNION
        					  SELECT device_uid,
        	                         device_meter_id,
        							 local_date,
        							 total_system_energy
        						FROM eniscope_3 e3
        					   UNION
        					  SELECT device_uid,
        	                         device_meter_id,
        							 local_date,
        							 total_system_energy
        						FROM eniscope_4 e4
        					   UNION
        					  SELECT device_uid,
        	                         device_meter_id,
        							 local_date,
        							 total_system_energy
        						FROM eniscope_5 e5
        					   UNION
        					  SELECT device_uid,
        	                         device_meter_id,
        							 local_date,
        							 total_system_energy
        						FROM eniscope_6 e6
        					   UNION
        					  SELECT device_uid,
        	                         device_meter_id,
        							 local_date,
        							 total_system_energy
        						FROM eniscope_7 e7
        					   UNION
        					  SELECT device_uid,
        	                         device_meter_id,
        							 local_date,
        							 total_system_energy
        						FROM eniscope_8 e8
        					   UNION
        					  SELECT device_uid,
        	                         device_meter_id,
        							 local_date,
        							 total_system_energy
        						FROM eniscope_9 e9
        	                 ),
             total_energy AS (SELECT al.local_date AS report_date,
        	                         conf.seq_no,
        							 conf.eniscope_seq_no,
        							 conf.eniscope_meter_point,
        	                         conf.eniscope_meter_name,
        	                         al.total_system_energy AS energy,
        							 CONCAT(STR(al.total_system_energy/1000000), ' KWh') AS energy_in_units,
        							 (al.total_system_energy * op.power_cost) AS energy_cost
        	                    FROM st_pec_system_options op,
        						     st_pec_eniscope_config conf
        							 LEFT OUTER JOIN
        						     all_eniscope al
        						  ON     conf.eniscope_uid          = al.device_uid
        						     AND conf.eniscope_meter_point  = al.device_meter_id
        					   WHERE conf.eniscope_meter_status = 'Y'),
        	 avg_week_report AS (SELECT seq_no,
        	                            eniscope_seq_no,
        								eniscope_meter_point,
        								eniscope_meter_name,
        								AVG(energy) OVER (PARTITION BY seq_no, eniscope_seq_no, eniscope_meter_point, eniscope_meter_name) avg_system_energy
        	                       FROM ST_PEC_ENISCOPE_DAILY_REPORT_ARCHIVE
        						  WHERE report_date BETWEEN CAST(FORMAT (getdate() - 8 , 'yyyy-MM-dd') AS DATE) AND CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
        	                    ),
             daily_report AS (SELECT te.report_date,
        	                         te.seq_no,
        							 te.eniscope_seq_no,
        							 te.eniscope_meter_point,
        							 te.eniscope_meter_name,
        							 te.energy,
        							 te.energy_in_units,
        							 te.energy_cost,
        							 ISNULL(awr.avg_system_energy,1) AS avg_energy_last_week,
        							 CONCAT(STR(ISNULL(awr.avg_system_energy,1)/1000000), ' KWh') AS avg_energy_last_week_in_units
        	                    FROM total_energy te
        						     LEFT OUTER JOIN
        						     avg_week_report awr
        						  ON     te.seq_no               = awr.seq_no
        						     AND te.eniscope_seq_no      = awr.eniscope_seq_no
        							 AND te.eniscope_meter_point = awr.eniscope_meter_point
        							 AND te.eniscope_meter_name  = awr.eniscope_meter_name),
             daily_report_percentage AS (SELECT dr.report_date,
        	                                    dr.seq_no,
        										dr.eniscope_seq_no,
        										dr.eniscope_meter_point,
        										dr.eniscope_meter_name,
        										dr.energy,
        										dr.energy_in_units,
        										dr.energy_cost,
        										dr.avg_energy_last_week,
        										dr.avg_energy_last_week_in_units,
        										--(dr.energy - dr.avg_energy_last_week) as a,
        										--(dr.energy - dr.avg_energy_last_week) / (dr.avg_energy_last_week) as b
        										ROUND( (dr.energy - dr.avg_energy_last_week) * 100 / (dr.avg_energy_last_week), 2)  AS percentage_change,
        										CASE WHEN ((dr.energy - dr.avg_energy_last_week) * 100 / (dr.avg_energy_last_week)) < 0 THEN
        										    'DOWN'
        										ELSE
        										    'UP'
        										END AS increment_type
        	                               FROM daily_report dr),
             daily_report_final AS (SELECT drp.report_date,
        	                               drp.seq_no,
            							   drp.eniscope_seq_no,
        								   drp.eniscope_meter_point,
        								   drp.eniscope_meter_name,
        								   drp.energy,
        								   drp.energy_in_units,
        								   drp.energy_cost,
        								   drp.avg_energy_last_week,
        								   drp.avg_energy_last_week_in_units,
        								   drp.percentage_change,
        								   drp.increment_type
        	                          FROM daily_report_percentage drp
									 WHERE drp.report_date IS NOT NULL)
	    INSERT INTO [dbo].[ST_PEC_ENISCOPE_DAILY_REPORT] (report_date,
		                                                  seq_no,
														  eniscope_seq_no,
														  eniscope_meter_point,
														  eniscope_meter_name,
														  energy,
														  energy_in_units,
														  energy_cost,
														  avg_energy_last_week,
														  avg_energy_last_week_in_units,
														  percentage_change,
														  increment_type)
        SELECT DISTINCT report_date,
		       seq_no,
			   eniscope_seq_no,
			   eniscope_meter_point,
			   eniscope_meter_name,
			   energy,
			   energy_in_units,
			   energy_cost,
			   avg_energy_last_week,
			   avg_energy_last_week_in_units,
			   percentage_change,
			   increment_type
          FROM daily_report_final;

      PRINT ('ROWS Insert INTO ST_PEC_ENISCOPE_DAILY_REPORT :'+CAST(@@ROWCOUNT AS nvarchar(30)));

	  WITH daily_report_summ AS (SELECT DISTINCT report_date,
                                  999 AS seq_no,
                            	  1 AS eniscope_seq_no,
                            	  1 AS eniscope_meter_point,
                            	  'DAILY REPORT' AS eniscope_meter_name,
                            	  SUM(energy) OVER (PARTITION BY report_date) AS energy,
                            	  SUM(energy_cost) OVER (PARTITION BY report_date) AS energy_cost,
                            	  SUM(avg_energy_last_week) OVER (PARTITION BY report_date) AS avg_energy_last_week
                             FROM st_pec_eniscope_daily_report),
	 daily_report_summ_final AS (SELECT report_date,
	                                    seq_no,
										eniscope_seq_no,
										eniscope_meter_point,
										eniscope_meter_name,
										energy,
										CONCAT(STR(ISNULL(energy,1)/1000000), ' KWh') AS energy_in_units,
										energy_cost,
										avg_energy_last_week,
										CONCAT(STR(ISNULL(avg_energy_last_week,1)/1000000), ' KWh') AS avg_energy_last_week_in_units,
										ROUND( (energy - avg_energy_last_week) * 100 / (avg_energy_last_week), 2)  AS percentage_change,
        								CASE WHEN ((energy - avg_energy_last_week) * 100 / (avg_energy_last_week)) < 0 THEN
        								    'DOWN'
        								ELSE
        								    'UP'
        								END AS increment_type
							       FROM daily_report_summ)
	    INSERT INTO [dbo].[ST_PEC_ENISCOPE_DAILY_REPORT] (report_date,
		                                                  seq_no,
														  eniscope_seq_no,
														  eniscope_meter_point,
														  eniscope_meter_name,
														  energy,
														  energy_in_units,
														  energy_cost,
														  avg_energy_last_week,
														  avg_energy_last_week_in_units,
														  percentage_change,
														  increment_type)
        SELECT report_date,
		       seq_no,
			   eniscope_seq_no,
			   eniscope_meter_point,
			   eniscope_meter_name,
			   energy,
			   energy_in_units,
			   energy_cost,
			   avg_energy_last_week,
			   avg_energy_last_week_in_units,
			   percentage_change,
			   increment_type
          FROM daily_report_summ_final;

	  PRINT ('ROWS Insert INTO ST_PEC_ENISCOPE_DAILY_REPORT FOR Daily Report Summary:'+CAST(@@ROWCOUNT AS nvarchar(30)));


	   END TRY
	   BEGIN CATCH

	      SET @Err_Message  = ERROR_MESSAGE();
          SET @Err_Severity = ERROR_SEVERITY();
		  SET @Err_State    = ERROR_STATE();
		  SET @Error_Message = @Err_Message+' : '+@Err_Severity+' : '+@Err_State;

		  RAISERROR (@Err_Message, @Err_Severity, @Err_State)

       END CATCH
