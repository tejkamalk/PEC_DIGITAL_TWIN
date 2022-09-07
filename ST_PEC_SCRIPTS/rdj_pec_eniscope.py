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
    parser.add_argument('-e', '--eniscopeId', required=True, help='specifies eniscope Id')
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
        cnxn=pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+Creds["SQLSERVER"]+';DATABASE='+Creds["SQLDATABASE"]+';UID='+Creds["SQLUSER"]+';PWD='+ Creds["SQLPASSWORD"])
        sqlconn=cnxn.cursor()
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

def call_stored_procedure(json):
    logging.info("DBCALL Function")
    upsert_count = ""
    error_message = ""
    print("json :" + str(json))
    try:
        stored_proc = "dbo.upload_eniscope_topic_msg"
        sql = """\
              SET NOCOUNT ON;
              DECLARE @json varchar;
              DECLARE @upsert_count int;
              DECLARE @upserted_count int;
              DECLARE @error_message varchar;
              EXEC [stidata].[dbo].[upload_eniscope_topic_msg] @mqtt_json_payload=?, @upserted_count=@upserted_count OUTPUT, @error_message=@error_message OUTPUT;
              SELECT @upserted_count ',
		     @Error_message '
              """
        values = (json, )
        sqlconn.execute(sql, values)
        rows = sqlconn.fetchall()
        upsert_count = [item[0] for item in rows]
        upsert_count = upsert_count[0]
        error_message = [item[1] for item in rows]
        error_message = error_message[0]
        logging.info("upsert_count : "+str(upsert_count))
        logging.info("error_message : "+str(error_message))
        sqlconn.commit()

        if not error_message:
            raise Error_dbCall

        if not upsert_count:
            raise Error_loading

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


def on_connect(mqttc, obj, flags, rc):
    logging.info("Connect Function")


def on_message(mqttc, obj, msg):
    logging.info("On_Message Function")
    sqlinsert  = "INSERT INTO "
    sqlinsert +=  Creds["SQLDATABASE"]+"."
    sqlinsert +=  Creds["SQLSCHEMA"]+"."
    sqlinsert +=  Creds["SQLRAWTABLE"]+" ("
    sqlinsert += "                              mqttc_client, "
    sqlinsert += "                              mid, "
    sqlinsert += "                              state, "
    sqlinsert += "                              qos, "
    sqlinsert += "                              dup, "
    sqlinsert += "                              retain, "
    sqlinsert += "                              topic, "
    sqlinsert += "                              payload) "
    sqlinsert += "                       VALUES ('"
    sqlinsert +=                                str(mqttc._client_id.decode('utf-8'))+"','"
    sqlinsert +=                                str(msg.mid)+"','"
    sqlinsert +=                                str(msg.state)+"','"
    sqlinsert +=                                str(msg.qos)+"','"
    sqlinsert +=                                str(msg.dup)+"','"
    sqlinsert +=                                str(msg.retain)+"','"
    sqlinsert +=                                str(msg.topic)+"','"
    sqlinsert +=                                str(msg.payload.decode('utf-8'))+"')"
    rowsupserted = loaddB(sqlinsert)
    logging.info('Rows inserted: ' + str(rowsupserted))
    Time.sleep(0.1)

def on_publish(mqttc, obj, mid):
    #logging.info("Publish Function")


def on_subscribe(mqttc, obj, mid, granted_qos):
    #logging.info("Subscribe Function")


def on_log(mqttc, obj, level, string):
    #logging.info("On Log Function")


def main():

    ParseCommandLine()

    global Creds
    Creds = ReadFileToDict(gl_args.credFile)

    print("creds " +Creds["MQTTUSER"])

    global eniscope_id
    eniscope_id = gl_args.eniscopeId

    print("eniscope_id " +eniscope_id)

    # turn on logging
    tfortime = Time.strftime('%Y%m%d%H%M%S', Time.localtime())
    #logfile
    logfile = "C:\\ST_PEC_SCRIPTS\\log\\"
    logfile += str(scriptname)
    logfile += "_"
    logfile += str(eniscope_id)
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


    logging.info('Get Credentials')
    logging.info('Credentials file is ' + gl_args.credFile)
    logging.info('Get Eniscope Id')
    logging.info('Eniscope Id '+ gl_args.eniscopeId)

    godB()

    mqttc = mqtt.Client(Creds["MQTTUSER"],False)

    mqttc.on_message = on_message

    mqttc.on_connect = on_connect

    mqttc.on_publish = on_publish

    mqttc.on_subscribe = on_subscribe

    mqttc.username_pw_set(Creds["MQTTUSER"],password=Creds["MQTTPASSWORD"])

    mqttc.connect(Creds["MQTTBROKER"],port=int(Creds["MQTTPORT"]))

    mqttc.subscribe(Creds["MQTTTOPIC"], 2)

    #mqttc.on_log = on_log

    mqttc.loop_forever()

if __name__ == "__main__":

    thread = threading.Thread(target=main, name='thread')

    thread.start()

    thread.join
