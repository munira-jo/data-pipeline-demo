# Data Pipeline Demo

A data pipeline that will ingest zipped data files in a given folder, load them into a postgres database hosted on AWS RDS and then transform the data into a star schema.

Raw zipped data files are placed in the `ingestion/zipped_raw_data` folder. 

For the ingestion into a Postgres database, run the `ingestion/main.py` script. Before running the script, the directory structure for the `ingestion` folder looks like this:

ª   copy_files_into_postgres.py
ª   create_tables.py
ª   prepare_data_for_ingestion.py
ª   
+---zipped_raw_data
        Crash_2022Aug.zip
        FMCSA_CENSUS1_2022Aug.zip
        Inspection_2022Aug.zip
        Violation_2022Aug.zip


After the script is run, files are extracted from the zipped files and are organized into ReadMe and regular data files. Then a cleaning script prepares the data for ingestion into Postgres and writes the cleaned data to CSV files stored in `CleanedData`.  

Then the `create_tables.py` module creates the relevant tables on the database (if they don't already exist).

Finally, the `copy_files_into_postgres` module copies the CSV files into the tables.

This is the directory structure for the `ingestion` folder after running `ingestion/main.py`:

+---raw_data
ª   +---CleanedData
ª   ª       2022Aug_Crash.csv
ª   ª       2022Aug_Inspection.csv
ª   ª       2022Aug_Violation.csv
ª   ª       FMCSA_CENSUS1_2022Aug.csv
ª   ª       
ª   +---DataFiles
ª   ª   +---Processed
ª   ª           2022Aug_Crash.txt
ª   ª           2022Aug_Inspection.txt
ª   ª           2022Aug_Violation.txt
ª   ª           FMCSA_CENSUS1_2022Aug.txt
ª   ª           
ª   +---ReadMeFiles
ª           Crash_Readme.txt
ª           Inspection_Readme.txt
ª           ReadMe_Census.txt
ª           Violation_Readme.txt
ª           
+---zipped_raw_data
    +---Processed
            Crash_2022Aug.zip
            FMCSA_CENSUS1_2022Aug.zip
            Inspection_2022Aug.zip
            Violation_2022Aug.zip


Once files are handled by the scripts, they are moved into `Processed` subfolders. 