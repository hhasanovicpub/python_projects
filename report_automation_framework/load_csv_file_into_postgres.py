import json
import pandas as pd
import sys 
from sqlalchemy import create_engine, text

sys.path.append("/Users/Haris/Documents/python_projects")

from encrypt_decrypt import *

config_file_location = "/Users/Haris/Documents/python_projects/report_automation_framework/config_file.json"

csv_file_location = '/Users/Haris/Documents/python_projects/report_automation_framework/csv-files/annual-balance-sheets-2007-2021-provisional.csv'

def create_postgres_engine():
    config_file = open(config_file_location)
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
    engine = create_engine("postgresql://" + user + ":" + password_decrypted + "@" + host + "/" + database)
    return engine

engine = create_postgres_engine()
table = 'annual_balance'
df = pd.read_csv(csv_file_location)
df.to_sql(table, engine, schema='dw', if_exists='append', index=False)
