from python_repo import *

if __name__ == "__main__":
    args = sys.argv
    log_filename = "report_automation_framework_"+FORMATED_CURRENT_TIMESTAMP+".log"
    if args[1] == "deploy":
       logger.info("----------Starting process of deployment of a new configuration file----------")
       json_config_file = args[2]
       json_data = read_json_file(json_config_file)
       create_new_schedule(json_data, "postgresUser", CURRENT_TIMESTAMP)
       logger.info("----------Deployment process has been completed----------")
    elif args[1] == "pause":
         logger.info("----------Starting pausing process of a report schedule----------")
         report_name = args[2]
         pause_report_schedule(report_name)
         logger.info("----------Pausing process has been completed----------")
    elif args[1] == "activate":
         logger.info("----------Starting activation process of a report schedule----------")
         report_name = args[2]
         activate_report_schedule(report_name)
         logger.info("----------Activation process has been completed----------")
    else:
        logger.info("Incorrect use of the deployment script. Please read the guidance below on how to use it.")
        logger.info("Please use the script in the correct format:.\n"+
        "1. Create a new schedule by running the following command: python deploy_config_file.py deploy /json-files/folder-name/config-file-name.json or\n"+
        "2. Pause the existing schedule by running the following command: python deploy_config_file.py pause report_name or\n"+
        "3. Activate the existing schedule by running the following command: python deploy_config_file.py activate report_name")
    upload_log_file_to_Minio(log_filename)