# Data Pipeline Demo

A data pipeline that will ingest zipped data files in a given folder, load them into a postgres database hosted on AWS RDS and then transform the data into a star schema.


This data pipeline does the following:
- ingests raw data in the form of zipped files
- extracts the data to a given folder
- creates the Postgres database and database user
- creates tables in a Postgres database
- copies CSV files from a cleaned data folder into the tables.
- creates star schema fact and dimension tables from the raw data tables
- runs data tests to check if keys in star schema tables are unique 


## Getting the raw data

The raw data can be downloaded from [here](https://drive.google.com/drive/folders/15z4P78n1LbgM2FBMPHH1K8Hl3jE4pPpr?usp=drive_link). For the scripts to work, it should be placed in the subfolder `ingestion/zipped_raw_data`.


## How to run

1. Ingest the data by running `python3 ingestion/main.py`.
2. Transform the ingested data into a star schema by navigating into `transforms`
and running `dbt run`.


## Ingestion

Raw zipped data files are placed in the `ingestion/zipped_raw_data` folder. 

For the ingestion into a Postgres database, run the `ingestion/main.py` script. Before running the script, the directory structure for the `ingestion` folder looks like this:

```
│   copy_files_into_postgres.py
│   create_tables.py
│   main.py
│   prepare_data_for_ingestion.py
│
└───zipped_raw_data
        Crash_2022Aug.zip
        FMCSA_CENSUS1_2022Aug.zip
        Inspection_2022Aug.zip
        Violation_2022Aug.zip
```

After the script is run, files are extracted from the zipped files and are organized into ReadMe and regular data files. Then a cleaning script prepares the data for ingestion into Postgres and writes the cleaned data to CSV files stored in `CleanedData`.  

Then the `create_tables.py` module creates the relevant tables on the database (if they don't already exist).

Finally, the `copy_files_into_postgres` module copies the CSV files into the tables.

This is the directory structure for the `ingestion` folder after running `ingestion/main.py`:
```
│   copy_files_into_postgres.py
│   create_tables.py
│   main.py
│   prepare_data_for_ingestion.py
│
├───raw_data
│   ├───CleanedData
│   │       2022Aug_Crash.csv
│   │       2022Aug_Inspection.csv
│   │       2022Aug_Violation.csv
│   │       FMCSA_CENSUS1_2022Aug.csv
│   │
│   ├───DataFiles
│   │   └───Processed
│   │           2022Aug_Crash.txt
│   │           2022Aug_Inspection.txt
│   │           2022Aug_Violation.txt
│   │           FMCSA_CENSUS1_2022Aug.txt
│   │
│   └───ReadMeFiles
│           Crash_Readme.txt
│           Inspection_Readme.txt
│           ReadMe_Census.txt
│           Violation_Readme.txt
│
├───zipped_raw_data
    └───Processed
           Crash_2022Aug.zip
           FMCSA_CENSUS1_2022Aug.zip
           Inspection_2022Aug.zip
           Violation_2022Aug.zip
```

Note: Once files have been handled by the scripts, they are moved into `Processed` subfolders.

## Data Transforms

All of the data transformation post-ingestion are in the `transforms` folder. This is a [dbt](https://docs.getdbt.com/docs/) project linked to our Postgres database.

The main data transformation code is in the `transforms/models` folder. Each of the mart models have tests associated with them to check whether the relevant keys are unique and not null.

