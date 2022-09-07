import argparse
import smtplib
import sys
import time as Time
import requests
import logging
import logging.handlers
from requests.auth import HTTPBasicAuth
import urllib3
import logging
from datetime import datetime
import pytz
import os
import paho.mqtt.client as mqtt
import pyodbc
import threading
import re
import json
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import smtplib
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders

#python RDJ_Python_eniscope.py -c credentials-eniscope-ST.properties

#Return Variables
global OK
OK = 0
global NOTOK
NOTOK = 1
global scriptname
script = sys.argv[0]
scriptname_1 = os.path.basename(script)
scriptname = scriptname_1.split(".")[0]

def ParseCommandLine():
    """
    Name: ParseCommandLine() Function
    Desc: Process and Validate the command line arguments
    use Python Standard Library module argparse
    Input: none
    Actions:
    Uses the standard library argparse to process the command line
    establishes a global variable gl_args where any of the functions can
    obtain argument information
    """
    # define an object of type parser
    parser = argparse.ArgumentParser(description="Argument parser for informatica cloud run job... InfaRunAuto")
    # add credentials file option
    parser.add_argument('-c', '--credFile', required=True, help='specifies credentials file name')
    # create global object that can hold all valid arguments and make it avialable to all functions
    global gl_args
    # save the arguments gl_args
    gl_args = parser.parse_args()
    return OK

def ReadFileToDict(fileName):
    """
    Name: ReadFileToDict() Function
    Desc: Reads credentials from the supplied file and returns dictionary
    Input: file name that must be read
    Actions: Uses the standard library file open function to read data
    """
    # define the dictionary
    keyStore = {}
    logging.info("Reading keys and values from the file " + fileName)
    try:
        with open(fileName, "r") as fileData:
            for line in fileData:
                key, value = line.strip().split(':')
                keyStore[key] = value
        fileData.close()
        logging.info("Reading keys and values from the file " + fileName + " Successful")
        return keyStore
    except Exception as e:
        logging.exception("open / read the file provided failed with error " + str(e))
        raise

def godB():
    try:
        global sqlconn
        global cnxn
        global engine
        cnxn=pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+Creds["SQLSERVER"]+';DATABASE='+Creds["SQLDATABASE"]+';UID='+Creds["SQLUSER"]+';PWD='+ Creds["SQLPASSWORD"])
        sqlconn=cnxn.cursor()
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+Creds["SQLSERVER"]+';DATABASE='+Creds["SQLDATABASE"]+';UID='+Creds["SQLUSER"]+';PWD='+ Creds["SQLPASSWORD"]
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)
        logging.info('Database Connection Established Successfully..!')
        return OK
    except Exception as Error_godB:
        logging.info('Error in established initial database connection')
        logging.exception("dB connection with error " + str(Error_godB))
        sys.exit(NOTOK)

def loaddB(sqlupsert):
    try:
        count = sqlconn.execute(sqlupsert).rowcount
        sqlconn.commit()
        return count
    except Exception as Error_loaddB:
        logging.info('Error in loading data')
        logging.exception("UPSERT failed with error " + str(Error_loaddB))
        sys.exit(NOTOK)

def call_stored_procedure():
    logging.info("DBCALL Function")
    error_message = ""
    try:
        stored_proc = "dbo.st_pec_eniscope_daily_report_proc"
        sqlinsert = "SET NOCOUNT ON; DECLARE @Error_message nvarchar(max); EXEC [stidata].[dbo].[st_pec_eniscope_daily_report_proc] @Error_message = NULL"
        ret_val = loaddB(sqlinsert)

        if ret_val != -1:
            print ("error_message : "+error_message)
            raise Error_dbLoading

        if error_message:
            print ("error_message : "+error_message)
            raise Error_dbCall

        return OK

    except Exception as Error_dbCall:
        logging.info('Error in executing store procedure :'+stored_proc)
        logging.exception("Store proc execution failed " + str(Error_dbCall))
        logging.exception(error_message)
        sys.exit(NOTOK)
    except Exception as Error_loading:
        logging.info('Error in loading the payload')
        logging.exception("Store proc execution failed "+ str(Error_loading))
        sys.exit(NOTOK)

def create_graphs():

    logging.info("Creating Graphs Started")
    # Create Graph 1 - Cosumption for Whole Day
    sql_data1 = pd.read_sql_query('''
                                     SELECT eniscope_meter_name,
                                            FORMAT (getdate() - 1, 'yyyy-MM-dd') Reading,
                                            CAST(ROUND(energy,2) AS FLOAT) AS Energy
                                       FROM [STIDATA].[dbo].[ST_PEC_ENISCOPE_DAILY_REPORT_ARCHIVE]
                                      WHERE seq_no      <> 999
                                        AND report_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
                                      UNION
                                     SELECT eniscope_meter_name,
                                            'AVG_ENERGY_LAST_WEEK' Reading,
                                            CAST(ROUND(avg_energy_last_week,2) AS FLOAT) as Energy
                                       FROM [STIDATA].[dbo].[ST_PEC_ENISCOPE_DAILY_REPORT_ARCHIVE]
                                      WHERE seq_no      <> 999
                                        AND report_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
                                  '''
                              ,engine) # here, the 'engine' is the variable that contains your database connection information
    df1 = pd.DataFrame(sql_data1)
    fig = px.bar(df1,
                 title = "Energy Usage on Yesterday Against the Average day for the Last Week",
                 x="Energy",
                 y="eniscope_meter_name",
                 color="Reading",
                 barmode="group",
                 orientation="h",
                 )
    global plot1_name
    plot1_name = 'C:\ENISCOPE_DAILY_REPORT_GRAPHS\Energy_Usage_on_Yesterday_Against_the_Average_day_for_the_Last_Week_'+tfortime+'.html'
    fig.write_html(plot1_name)

    logging.info("Plot 1 executed checking for errors")

    if not os.path.exists(plot1_name):
        logging.exception("Plot 1 executed but some error please check for: "+plot1_name)
        sys.exit(NOTOK)
    else:
        logging.info("Plot 1 executed and graph file created : "+plot1_name)

    #Create Graph 2 - Consumption for Whole Day at Unit level
    sql_data2 = pd.read_sql_query('''
                                     SELECT eniscope_meter_name,
                                            FORMAT (report_date, 'yyyy-MM-dd') Reading,
                                            CAST(ROUND(energy,2) AS FLOAT) AS Energy
                                       FROM [STIDATA].[dbo].[ST_PEC_ENISCOPE_DAILY_REPORT_ARCHIVE]
                                      WHERE seq_no      = 999
                                        AND report_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
                                      UNION
                                     SELECT eniscope_meter_name,
                                            'AVG_ENERGY_LAST_WEEK' Reading,
                                            CAST(ROUND(avg_energy_last_week,2) AS FLOAT) as Energy
                                       FROM [STIDATA].[dbo].[ST_PEC_ENISCOPE_DAILY_REPORT_ARCHIVE]
                                      WHERE seq_no      = 999
                                        AND report_date = CAST(FORMAT (getdate() - 1, 'yyyy-MM-dd') AS DATE)
                                  '''
                              ,engine) # here, the 'conn' is the variable that contains your database connection information

    df2 = pd.DataFrame(sql_data2)
    fig = px.bar(df2,
                 title = "Energy Usage Summary on Yesterday Against the Average day for the Last Week",
                 x="Energy",
                 y="eniscope_meter_name",
                 color="Reading",
                 barmode="group",
                 )
    global plot2_name
    plot2_name = 'C:\ENISCOPE_DAILY_REPORT_GRAPHS\Energy_Usage_Summary_on_Yesterday_Against_the_Average_day_for_the_Last_Week_'+tfortime+'.html'
    fig.write_html(plot2_name)

    logging.info("Plot 2 executed checking for errors")

    if not os.path.exists(plot2_name):
        logging.exception("Plot 2 executed but some error please check for: "+plot2_name)
        sys.exit(NOTOK)
    else:
        logging.info("Plot 2 executed and graph file created : "+plot2_name)
        return OK

def create_html_body():
    logging.info ("create_html_body started")
    html_template = '''\
                    <html>
<head>

   <title>Tutsplus Email Newsletter</title>
   <style type="text/css">
    a {color: #d80a3e;}
  body, #header h1, #header h2, p {margin: 0; padding: 0;}
  #main {border: 1px solid #cfcece;}
  img {display: block;}
  #top-message p, #bottom p {color: #3f4042; font-size: 12px; font-family: Arial, Helvetica, sans-serif; }
  #header h1 {color: #ffffff !important; font-family: "Lucida Grande", sans-serif; font-size: 24px; margin-bottom: 0!important; padding-bottom: 0; }
  #header p {color: #ffffff !important; font-family: "Lucida Grande", "Lucida Sans", "Lucida Sans Unicode", sans-serif; font-size: 12px;  }
  h5 {margin: 0 0 0.8em 0;}
    h5 {font-size: 18px; color: #444444 !important; font-family: Arial, Helvetica, sans-serif; }
  p {font-size: 12px; color: #444444 !important; font-family: "Lucida Grande", "Lucida Sans", "Lucida Sans Unicode", sans-serif; line-height: 1.5;}
   </style>

  <style>
	.demo {
	    width:90%;
		font-family:Arial, Helvetica, sans-serif;
		border-collapse:collapse;
		padding:5px;
	}
	.demo th {
		font-family:Arial, Helvetica, sans-serif;
		border:1px outset #272525;
		padding:5px;
		background:#FDFCFC;
	}
	.demo td {
		font-family:Arial, Helvetica, sans-serif;
		border:1px inset #272525;
		text-align:center;
		padding:5px;
		font-size: 10px;
	}

	<style>
	.summary {
	    width:90%;
		font-family:Arial, Helvetica, sans-serif;
		border-collapse:seperate;
		padding:5px;
	}
	.summary th {
	    width:1000;
		font-family:Arial, Helvetica, sans-serif;
		border:0px outset #272525;
		padding:5px;
		background:#ECEA98;
	}
	.summary td {
		font-family:Arial, Helvetica, sans-serif;
		border:0px outset #272525;
		text-align:center;
		padding:5px;
		font-size: 10px
		background:#ECEA98;
	}


</style>

</head>
<body>
<table width="100%" cellpadding="0" cellspacing="0" bgcolor="e4e4e4"><tr><td>
<table id="top-message" cellpadding="20" cellspacing="0" width="600" align="center">

  </table>

<table id="main" width="600" align="center" cellpadding="0" cellspacing="15" bgcolor="ffffff">
    <tr>
      <td>
        <table id="header" cellpadding="10" cellspacing="0" align="center" bgcolor="8fb3e9">
          <tr>
            <td width="570" align="center"  bgcolor="#d80a3e"><h1>Eniscope Daily Report For PEC LTD</h1></td>
          </tr>
          <tr>
            <td width="570" align="right" bgcolor="#d80a3e"><p>@@@@@@@@@@1</p></td>
          </tr>
        </table>
      </td>
    </tr>

	<tr>
      <td>
        <table id="content-1" class = "summary" cellpadding="0" cellspacing="0" align="center">

		  <thead>
	<tr>
		<th>Energy Used</th>
		<th>Up On Average</th>
		<th>Cost</th>
	</tr>
	</thead>
	<tbody>
	<tr>
		@@@@@@@@@@2
	</tr>
	</tbody>

        </table>
      </td>
    </tr>


    <tr>
      <td>

		<div style="text-align: center;"> <span style="font-size: 15px; width:90%;"><h5>Usage breakdown</h5></span></div>
		<table id="content-4" class="demo" align="center">

	<thead>
	<tr>
		<th>Eniscope Name</th>
		<th>Cost</th>
		<th>Usage</th>
		<th>Average Day</th>
		<th>Stats</th>
	</tr>
	</thead>
	<tbody>
	@@@@@@@@@@3
	<tbody>

</table>


<div style="text-align: left;"> <span style="font-size: 10px; width:3; padding:5px">
*Note : Please see attachments for the graphical analysis of the energy consumption</span></div>
</table>

  </table>




</body>
</html>\
'''

    sql_data = pd.read_sql_query('''
                                     WITH token_data AS (
                    SELECT -99 AS seq, FORMAT (report_date, 'dd-MM-yyyy') AS token
                     FROM st_pec_eniscope_daily_report
                    WHERE seq_no = 999
                    UNION
                   SELECT -9 AS seq, CONCAT('<td>',energy_in_units,'</td><td>',percentage_change,' %</td><td>$ ',energy_cost,'</td>') AS token
                     FROM st_pec_eniscope_daily_report
                    WHERE seq_no = 999
                    UNION
                   SELECT seq_no AS seq, CONCAT('<tr><td>',eniscope_meter_name,'</td><td>$ ',energy_cost,'</td><td>',energy_in_units,'</td><td>',avg_energy_last_week_in_units,'</td><td bgcolor=#',CASE WHEN increment_type = 'UP' THEN 'E51A2F' WHEN increment_type = 'DOWN' THEN '44d43b' END,'>',increment_type,' ',percentage_change,' %</td></tr>') AS token
                     FROM st_pec_eniscope_daily_report
                    WHERE seq_no <> 999)
SELECT *
  FROM token_data
 ORDER BY seq
                                  '''
                              ,engine) # here, the 'engine' is the variable that contains your database connection information
    df = pd.DataFrame(sql_data)

    range_max = len(df)
    for x in range(range_max):

        if x == 0:
            html_template = html_template.replace('@@@@@@@@@@1', df.token[x])
        elif x == 1:
            html_template = html_template.replace('@@@@@@@@@@2', df.token[x])
        elif x == 2:
            html_string = df.token[x]
        elif x+1 == range_max:
            html_string += df.token[x]
            html_template = html_template.replace('@@@@@@@@@@3', html_string)
        else:
            html_string += df.token[x]

    global html_body
    html_body = html_template


    logging.info ("create_html_body completed")

def shoot_email():

    logging.info("send email started")

    #Create HTML Graph on server
    create_graphs()

    #Create HTML body for the mail.
    create_html_body()

    From=Creds["EMAILFROM"]
    To=Creds["EMAILTO"]
    Subject=Creds["EMAILSUBJECT"]
    Server=Creds["EMAILSERVER"]
    Port=Creds["EMAILPORT"]
    Auth=Creds["EMAILAUTH"]


    message = MIMEMultipart()

    message['From'] = From
    message['To'] = To
    message['Subject'] =Subject
    body_email = html_body


    message.attach(MIMEText(body_email, 'html'))

    #Attching First Graph
    filename = plot1_name
    attachment = open(filename, "rb")

    x = MIMEBase('application', 'octet-stream')
    x.set_payload((attachment).read())
    encoders.encode_base64(x)

    x.add_header('Content-Disposition', "attachment; filename= %s" % filename)
    message.attach(x)

    #Attaching Second Graph
    filename = plot2_name
    attachment = open(filename, "rb")

    y = MIMEBase('application', 'octet-stream')
    y.set_payload((attachment).read())
    encoders.encode_base64(y)

    y.add_header('Content-Disposition', "attachment; filename= %s" % filename)
    message.attach(y)

    s_e = smtplib.SMTP(Server, Port)
    s_e.starttls()

    s_e.login(From, Auth)
    text = message.as_string()
    s_e.sendmail(From, To, text)

    s_e.quit()

    logging.info("send email completed")



def main():

    ParseCommandLine()

    global Creds
    Creds = ReadFileToDict(gl_args.credFile)


    # turn on logging
    global tfortime
    tfortime = Time.strftime('%Y%m%d%H%M%S', Time.localtime())
    #logfile
    logfile = "C:\\ST_PEC_SCRIPTS\\log\\"
    logfile += str(scriptname)
    logfile += "_"
    logfile += str(tfortime)
    logfile += ".log"

    #rawlogfile = r'%s' %logfile

    # turn on logging
    handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", logfile))
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(os.environ.get("LOGLEVEL", "DEBUG"))
    root.addHandler(handler)

    #Declaring Array for maintaining payload
    global mqtt_payload
    mqtt_payload = []
    elements = len(mqtt_payload)


    logging.warning('Get Credentials')
    logging.info('Credentials file is ' + gl_args.credFile)

    #Check Database Connectivity
    godB()

    #Call Stored Procedure to create Daily Report
    call_stored_procedure()

    #Send Email
    shoot_email()



if __name__ == "__main__":

    main()
