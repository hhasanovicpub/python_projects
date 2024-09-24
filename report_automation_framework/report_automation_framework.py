from python_repo import *

if __name__ == "__main__":
   logger.info("----------Starting report automation framework----------")
   log_filename = "report_automation_framework_"+FORMATED_CURRENT_TIMESTAMP+".log"
   connection = connect_to_postgres()
   reports_to_execute = find_reports_to_execute(connection)
   number_of_reports_to_execute = len(reports_to_execute)
   minio_client = connect_to_Minio()
   postgres_engine = create_postgres_engine()
   execute_reports(connection, postgres_engine, minio_client, reports_to_execute, number_of_reports_to_execute, update_dates_for_next_run_sql)
   logger.info("----------Report automation framework run completed----------")
   upload_log_file_to_Minio(log_filename)