import psycopg2
from zipfile import ZipFile
import pandas as pd
import os
from io import StringIO

zipped_folder=os.path.join('C:\\Users\\Munira\\Desktop\\Assessment','zipped_raw_data')

unzipped_folder=os.path.join('C:\\Users\\Munira\\Desktop\\Assessment','raw_data')

def connect_to_db(db_name,password):
    conn = psycopg2.connect(f"dbname={db_name} user=postgres host='mydb.cb7sygkkbh0u.us-east-2.rds.amazonaws.com' port=5432 password={password}")
    cur = conn.cursor()
    return conn,cur

def extract_and_unzip_files(zipped_folder, unzipped_folder):
    for zip_filename in os.listdir(zipped_folder):
        z=ZipFile(os.path.join(zipped_folder,zip_filename))
        z.extractall(unzipped_folder)

extract_and_unzip_files(zipped_folder,unzipped_folder)