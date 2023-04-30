\copy crash FROM 'C:\\Users\Munira\Desktop\demo-pipeline\raw_data\CleanedData\2022Aug_Crash.csv' WITH (FORMAT CSV, DELIMITER ',',HEADER);

\copy inspection FROM 'C:\\Users\Munira\Desktop\demo-pipeline\raw_data\CleanedData\2022Aug_Inspection.csv' WITH (FORMAT CSV, DELIMITER ',',HEADER);