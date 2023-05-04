"""
Module for copying CSV files to a Postgres database.

This module provides functions for connecting to a Postgres database
hosted on RDS and copying CSV files into tables on the database. 
It also includes a main function for copying all files in a specified folder
to the corresponding tables on the database and moving them to a new subfolder.

Functions:
- connect_to_db: Connects to the Postgres database hosted on RDS
and returns connection and cursor objects.
- copy_csv_file_to_postgres_table: Copies a CSV file into the specified
table on a Postgres database.
- main: Main function of the script.

Dependencies:
- os: Provides a way to interact with the operating system.
- psycopg2: Provides a way to connect to Postgres databases.
- dotenv: Loads environment variables from a .env file.
- prepare_data_for_ingestion: Provides a function for moving files to a new subfolder.

"""

import os

import psycopg2
from dotenv import load_dotenv

from prepare_data_for_ingestion import move_files_to_new_subfolder

load_dotenv()

db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")


def connect_to_db(
    database_name, database_password, database_user=db_user, database_host=db_host
):
    """
    Connects to the Postgres database hosted on RDS. Provides connection and cursor objects.
    """
    conn = psycopg2.connect(
        f"dbname={database_name} user={database_user} host={database_host} \
        port=5432 password={database_password}"
    )
    cur = conn.cursor()
    return conn, cur


def copy_csv_file_to_postgres_table(
    *, csv_file_path, postgres_table_name, database_password
):
    """
    Copies a CSV file into the table postgres_table_name on a Postgres database.
    """
    conn, cursor = connect_to_db("staging", database_password)
    conn.autocommit = True

    try:
        cursor.execute("set client_encoding='win1252'")
        with open(csv_file_path, "r", encoding="cp1252") as myfile:
            copy_sql = (
                f"COPY {postgres_table_name} FROM STDIN WITH (FORMAT CSV, HEADER)"
            )
            cursor.copy_expert(copy_sql, myfile)
    except psycopg2.errors.QueryCanceled or psycopg2.errors.UntranslatableCharacter:
        cursor.execute("set client_encoding='utf-8'")
        with open(csv_file_path, "r", encoding="utf8") as myfile:
            copy_sql = (
                f"COPY {postgres_table_name} FROM STDIN WITH (FORMAT CSV, HEADER)"
            )
            cursor.copy_expert(copy_sql, myfile)
    cursor.close()
    conn.close()
    print(f"{csv_file_path} copied to table {postgres_table_name}")


def main(*csv_folder_path):
    """
    Copies all files in csv_folder to similar named tables on a Postgres database,
    then moves copied file into subfolder 'Processed'.
    """
    csv_dict = {}
    for csv_file in os.listdir(csv_folder_path):
        if os.path.isfile(os.path.join(csv_folder_path, csv_file)):
            csv_dict[csv_file] = (
                csv_file.split("_")[1].lower().replace(".csv", "").replace("1", "")
            )
    for csv_file, table_name in csv_dict.items():
        copy_csv_file_to_postgres_table(
            csv_file_path=os.path.join(csv_folder_path, csv_file),
            postgres_table_name=table_name,
            database_password=db_password,
        )
        move_files_to_new_subfolder(
            new_folder_name="Processed",
            original_folder_path=csv_folder,
            file_identifier=csv_file,
        )

local_repo_path="C:\\Users\\Munira\\Desktop\\demo-pipeline"
csv_folder_path = os.path.join(local_repo_path,"ingestion","raw_data", "CleanedData")

if __name__ == "__main__":
    main(csv_folder_path=csv_folder_path)
