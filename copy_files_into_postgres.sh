\copy crash FROM 'C:\\Users\Munira\Desktop\demo-pipeline\raw_data\CleanedData\2022Aug_Crash.csv' WITH (FORMAT CSV,HEADER);

\copy inspection FROM 'C:\\Users\Munira\Desktop\demo-pipeline\raw_data\CleanedData\2022Aug_Inspection.csv' WITH (FORMAT CSV,HEADER);

\copy violation FROM 'C:\\Users\Munira\Desktop\demo-pipeline\raw_data\CleanedData\2022Aug_Violation.csv' WITH (FORMAT CSV,HEADER);

\copy census FROM 'C:\\Users\Munira\Desktop\demo-pipeline\raw_data\CleanedData\2022Aug_Violation.csv' WITH (FORMAT CSV,HEADER);