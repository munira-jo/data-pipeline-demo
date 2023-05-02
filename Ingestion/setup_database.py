"""
Module for creating a Postgres database and a user.


Functions:
- connect_to_db: Connects to the Postgres database hosted on RDS
and returns connection and cursor objects.
- main: Creates the database 'staging' and the user 'dataengineer'

Dependencies:
- os: Provides a way to interact with the operating system.
- psycopg2: Provides a way to connect to Postgres databases.
- dotenv: Loads environment variables from a .env file.
"""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Database password for 'postgres' user
postgre_user_db_password = os.getenv("POSTGRES_USER_DB_PASSWORD")
# Database password for 'dataengineer' user
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")


def connect_to_db(*, db_name, database_password, database_user):
    """
    Connects to the Postgres database hosted on RDS. Provides connection and cursor objects.
    """
    conn = psycopg2.connect(
        f"dbname={db_name} user={database_user} host={db_host} port=5432 \
              password={database_password}"
    )
    cur = conn.cursor()
    return conn, cur


def create_user(database_password):
    """
    Creates the user 'dataengineer'.
    """
    sql = "create user dataengineer with password 'aculocity' login createdb"
    conn, cursor = connect_to_db(
        db_name="postgres",
        database_user="postgres",
        database_password=database_password,
    )
    conn.autocommit = True
    try:
        cursor.execute(sql)
        print("Database user created.")
    except psycopg2.errors.DuplicateObject:
        print(f"User already exists")
    cursor.close()
    conn.close()


def create_database(database_password):
    """
    Creates the database 'staging'.
    """
    sql = "create database staging"

    conn, cursor = connect_to_db(
        db_name="postgres",
        database_user="dataengineer",
        database_password=database_password,
    )
    conn.autocommit = True
    try:
        cursor.execute(sql)
        print("Database staging created.")
    except psycopg2.errors.DuplicateObject:
        print(f"Database already exists")
    cursor.close()
    conn.close()


def main(*, postgres_user_password, database_password):
    create_user(postgres_user_password)
    create_database(database_password)


if __name__ == "__main__":
    main(postgres_user_password=postgre_user_db_password, database_password=db_password)
