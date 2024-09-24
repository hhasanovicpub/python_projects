import json
import psycopg2
import sys
import pandas as pd
import io
import os
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text 
from sql_repo import *
from minio import Minio

sys.path.append("/mnt/c/Users/Haris/Documents/python_projects")
from encrypt_decrypt import *

CURRENT_TIMESTAMP = datetime.now()
FORMATED_CURRENT_TIMESTAMP = CURRENT_TIMESTAMP.strftime("%Y%m%d")
CONFIG_FILE_LOCATION = "/mnt/c/Users/Haris/Documents/python_projects/report_automation_framework/config_file.json"
LOG_FILE_LOCATION = "/mnt/c/Users/Haris/Documents/python_projects/report_automation_framework/"

logger = logging.getLogger(__name__)

logging.basicConfig(filename = LOG_FILE_LOCATION + "report_automation_framework_"+FORMATED_CURRENT_TIMESTAMP+".log", level = logging.INFO, format = "%(asctime)s %(levelname)s: %(message)s")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
console.setFormatter(formatter)
logger.addHandler(console)

def connect_to_postgres():
    logger.info("Starting Postgres connection process.")
    config_file = open(CONFIG_FILE_LOCATION)
    data = json.load(config_file)
    key = data["postgres"]["key"]
    key_encoded = str.encode(key)
    secret = data["postgres"]["password"]
    secret_encoded = str.encode(secret)
    password_decrypted = decyrpt_secret(key_encoded, secret_encoded)
    try:
        conn = psycopg2.connect(
            database = data["postgres"]["database"],
            user = data["postgres"]["user"],
            password = password_decrypted,
            host = data["postgres"]["host_ip"],
            port = data["postgres"]["port"]
        )
        logger.info("Successfully connected to Postgres.")
    except Exception as e:
        logger.error("An error occured while trying to connect to Postgres: {}".format(str(e)), exc_info=True)
    config_file.close()
    return conn 

def create_postgres_engine():
    logger.info("Starting process of creating Postgres engine.")
    config_file = open(CONFIG_FILE_LOCATION)
    data = json.load(config_file)
    user = data["postgres"]["user"]
    key = data["postgres"]["key"]
    key_encoded = str.encode(key)
    password = data["postgres"]["password"]
    password_encoded = str.encode(password)
    database = data["postgres"]["database"]
    host = data["postgres"]["host"]
    password_decrypted = decyrpt_secret(key_encoded, password_encoded)
    config_file.close()
    try:
        engine = create_engine("postgresql://" + user + ":" + password_decrypted + "@" + host + "/" + database)
        logger.info("Engine has been created successfully.")
    except Exception as e:
        logger.error("An error occured while trying to create Postgres engine: {}".format(str(e)), exc_info=True)
    return engine

def create_new_schedule(json_data, user, current_timestamp):
    logger.info("Starting process of creating new schedule.")
    connection = connect_to_postgres()
    cursor = connection.cursor()
    logger.info("Checking if a schedule " + json_data["report_name"] + " already exist.")
    cursor.execute(find_report_name_sql, (json_data["report_name"],))
    query_result = cursor.fetchone()
    if query_result == None:
       logger.info("Schedule " + json_data["report_name"] + " doesn't exist. Proceeding with creating a new schedule.")
       cursor.execute(insert_into_report_configuration_sql, (json_data["report_name"], user, current_timestamp))
       cursor.execute(find_report_id_sql, (json_data["report_name"],))
       report_id = cursor.fetchone()[0]
       cursor.execute(insert_into_report_schedule_sql, (report_id, json_data["run_date"], json_data["begin_date"], json_data["end_date"], json_data["status"], json_data["next_run_formula"], json_data["is_active"], current_timestamp))
       cursor.execute(insert_into_report_definition_sql, (report_id, json_data["sql_path"], json_data["output_filename"], json_data["date_format"], json_data["delimiter"], json_data["output_file_location"], current_timestamp))
       connection.commit()
       logger.info("New schedule " + json_data["report_name"] + " has been successfully created.")
    else:
       logger.info("Schedule " + json_data["report_name"] + " already exist. Updating the existing schedule with new configuration.")
       cursor.execute(update_report_configuration_sql, (user, current_timestamp, json_data["report_name"]))
       cursor.execute(find_report_id_sql, (json_data["report_name"],))
       report_id = cursor.fetchone()[0]
       cursor.execute(update_report_schedule_sql, (json_data["run_date"], json_data["begin_date"], json_data["end_date"], json_data["status"], json_data["next_run_formula"], json_data["is_active"], current_timestamp, report_id))
       cursor.execute(update_report_definition_sql, (json_data["sql_path"], json_data["output_filename"], json_data["date_format"], json_data["delimiter"], json_data["output_file_location"], current_timestamp, report_id))
       connection.commit()
       logger.info("Schedule " + json_data["report_name"] + " been succesfully updated.")
    connection.close()

def connect_to_Minio():
    logger.info("Starting Minio connection process.")
    config_file = open(CONFIG_FILE_LOCATION)
    data = json.load(config_file)
    key = data["minio"]["key"]
    key_encoded = str.encode(key)
    secret = data["minio"]["secret_key"]
    secret_encoded = str.encode(secret)
    password_decrypted = decyrpt_secret(key_encoded, secret_encoded)
    try:
        client = Minio(
        endpoint = data["minio"]["api_host"],
        access_key = data["minio"]["access_key"],
        secret_key = password_decrypted,
        secure = False
        )
        logger.info("Successfully connected to Minio.")
    except Exception as e:
        logger.error("An error occured while trying to connect to Minio: {}".format(str(e)), exc_info=True)
    config_file.close()
    return client 

def read_json_file(json_config_file):
    minio_client = connect_to_Minio()
    logger.info("Reading configuration file from Minio.")
    data = minio_client.get_object("report-automation-framework", json_config_file)
    file_content = data.read()
    json_data = json.loads(file_content)
    return json_data

def pause_report_schedule(report_name):
    logger.info("Starting pause process for the report: " + report_name + ".")
    connection = connect_to_postgres()
    cursor = connection.cursor()
    logger.info("Checking if the given report name exists.")
    cursor.execute(find_report_id_sql, (report_name,))
    query_result = cursor.fetchone()
    if query_result == None:
        logger.info("Provided report name" + report_name + " doesn't exist.")
    else:
        cursor.execute(set_schedule_inactive_sql, (query_result,))
        connection.commit()
        logger.info("Report " + report_name + " has been paused.")
    connection.close()

def activate_report_schedule(report_name):
    logger.info("Starting activation process for the report: " + report_name + ".")
    connection = connect_to_postgres()
    cursor = connection.cursor()
    logger.info("Checking if the given report name exists.")
    cursor.execute(find_report_id_sql, (report_name,))
    query_result = cursor.fetchone()
    if query_result == None:
        logger.info("Provided report name" + report_name + " doesn't exist.")
    else:
        cursor.execute(set_schedule_active_sql, (query_result,))
        connection.commit()
        logger.info("Report " + report_name + " has been activated.")
    connection.close()

def delete_local_log_file(log_file):
    logger.info("Removing local file" + log_file + " after uploading it to Minio.")
    try:
        os.close(LOG_FILE_LOCATION+log_file)
        os.remove(LOG_FILE_LOCATION+log_file)
        logger.info("File " + log_file + " has been removed.")
    except Exception as e:
        logger.error("An error occured while trying to delete file: {}".format(str(e)), exc_info=True)

def upload_log_file_to_Minio(log_file):
    logger.info("----------Uploading log file " + log_file + " to Minio----------")
    minio_client = connect_to_Minio()
    minio_client.fput_object("report-automation-framework", "/logs/"+log_file, LOG_FILE_LOCATION + log_file)
    logger.info("----------File " + log_file + " has been successfully uploaded----------")

def format_date(date, date_format):
    if date_format == "YYYY-MM-DD":
       date = date.strftime("%Y-%m-%d")
    elif date_format == "YYYYMMDD":
       date = date.strftime("%Y%m%d")
    elif date_format == "YYYY/MM/DD":
       date = date.strftime("%Y/%m/%d")
    elif date_format == "YYYY.MM.DD":
       date = date.strftime("%Y.%m.%d")
    elif date_format == "YYYYMM":
       date = date.strftime("%Y%m")
    elif date_format == "YYYY":
       date = date.strftime("%Y")
    elif date_format == "MM":
       date = date.strftime("%m")
    elif date_format == "DD":
       date = date.strftime("%d")
    elif date_format == "DDMMYYYY":
       date = date.strftime("%d%m%Y")
    elif date_format == "DD-MM-YYYY":
       date = date.strftime("%d-%m-%Y")
    elif date_format == "DD/MM/YYYY":
       date = date.strftime("%d/%m/%Y")
    elif date_format == "DD.MM.YYYY":
       date = date.strftime("%d.%m.%Y")
    elif date_format == "MMYYYY":
       date = date.strftime("%m%Y")
    return date

def format_report_name(report_name, date_format, run_date, begin_date, end_date):
    logger.info("Formating report name.")
    count = report_name.count("date")
    counter = 0 
    while counter < count:
        if "{run_date}" in report_name:
            formated_date = format_date(run_date, date_format)
            report_name = report_name.replace("{run_date}", formated_date)
        elif "{begin_date}" in report_name:
            formated_date = format_date(begin_date, date_format)
            report_name = report_name.replace("{begin_date}", formated_date)
        else:
            formated_date = format_date(end_date, date_format)
            report_name = report_name.replace("{end_date}", formated_date)
        counter = counter + 1 
    return report_name

def update_dates_for_next_run(connection, report_name, report_id, run_date, begin_date, end_date, query, next_run_formula):
   logger.info("Setting up dates for the next run using the next run formula.")
   if next_run_formula == "daily":
       run_date = run_date + timedelta(days=1)
       begin_date = begin_date + timedelta(days=1)
       end_date = end_date + timedelta(days=1)
       cursor = connection.cursor()
       cursor.execute(query, (run_date, begin_date, end_date, report_id))
       connection.commit()
   elif next_run_formula == "weekly":
       run_date = run_date + timedelta(days=7)
       begin_date = begin_date + timedelta(days=7)
       end_date = end_date + timedelta(days=7)
       cursor = connection.cursor()
       cursor.execute(query, (run_date, begin_date, end_date, report_id))
       connection.commit()
   elif next_run_formula == "adhoc":
        os.chdir("/mnt/c/Users/Haris/Documents/python_projects/report_automation_framework")
        os.system("python3 deploy_config_file.py pause "+ report_name)

def insert_information_about_run(connection, report_id, run_date, begin_date, end_date, status, next_run_formula, sql_path, output_filename,output_file_location):
    logger.info("Inserting the data about the report run into admin.report_run table.")
    cursor = connection.cursor()
    cursor.execute(insert_into_report_run_sql, (report_id, run_date, begin_date, end_date, status, next_run_formula, sql_path, output_filename, output_file_location))
    connection.commit()

def find_reports_to_execute(connection):
    logger.info("Fiding the reports to execute.")
    connection = connect_to_postgres()
    cursor = connection.cursor()
    cursor.execute(find_queued_schedules_sql)
    reports_to_execute = cursor.fetchall()
    return reports_to_execute

def format_query(query, run_date, begin_date, end_date):
    query = query.replace("{run_date}", "'" + run_date.strftime("%Y-%m-%d") + "'")
    query = query.replace("{begin_date}", "'" + begin_date.strftime("%Y-%m-%d") + "'")
    query = query.replace("{end_date}", "'" + end_date.strftime("%Y-%m-%d") + "'")
    return query

def execute_reports(connection, postgres_engine, minio_client, reports_to_execute, number_of_reports_to_execute, update_query):
   if number_of_reports_to_execute > 0:
      for report in reports_to_execute:
          sql_object = minio_client.get_object("report-automation-framework", report[4])
          query = sql_object.read().decode()
          query = format_query(query, report[1], report[2], report[3])
          df = pd.read_sql_query(sql=text(query), con=postgres_engine.connect())
          if report[6] != "":
             report_name = format_report_name(report[5], report[6], report[1], report[2], report[3])
          else:
             report_name = report[5]
          csv_data = df.to_csv(sep=report[7], index=False).encode("utf-8")
          csv_bytes = io.BytesIO(csv_data)
          minio_client.put_object("report-bucket", report[8]+"/"+report_name, csv_bytes, len(csv_data))
          logger.info("Report "+ report[0] + " has been executed. Updating dates for next run.")
          update_dates_for_next_run(connection, report[0], report[9], report[1], report[2], report[3], update_query, report[10])
          insert_information_about_run(connection, report[9], report[1], report[2], report[3], report[11], report[10], report[4], report_name, report[8])

      connection.close()
      postgres_engine.dispose()
      del minio_client
   else: 
      logger.info("No reports to execute at the moment.")
