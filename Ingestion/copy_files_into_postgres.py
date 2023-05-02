import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")


def connect_to_db(database_name, database_password,database_user=db_user,database_host=db_host):
    '''
    Connects to the Postgres database hosted on RDS. Provides connection and cursor objects.
    '''
    conn = psycopg2.connect(
        f"dbname={database_name} user={database_user} host={database_host} \
        port=5432 password={database_password}"
    )
    cur = conn.cursor()
    return conn, cur



def copy_csv_file_to_postgres_table(*,csv_file_path,postgres_table_name,database_password):
    conn,cursor=connect_to_db('staging',database_password)
    conn.autocommit=True
    with open(csv_file_path,'r',encoding='utf-8') as myfile:
        copy_sql = "COPY crash FROM STDIN WITH (FORMAT CSV, HEADER)"
        cursor.copy_expert(copy_sql, myfile)
    cursor.close()
    conn.close()
    print(f'{csv_file_path} copied to table {postgres_table_name}')



csv_folder_path=os.path.join(os.getcwd(),'raw_data','CleanedData')

csv_dict={}
for csv_file in os.listdir(csv_folder_path):
    if os.path.isfile(os.path.join(csv_folder_path,csv_file)):
        csv_dict[csv_file]=csv_file.split('_')[1].lower()
for csv_file,table_name in csv_dict.items():
    copy_csv_file_to_postgres_table(csv_file_path=os.path.join(csv_folder_path,csv_file), \
                                    postgres_table_name=table_name,database_password=db_password)
