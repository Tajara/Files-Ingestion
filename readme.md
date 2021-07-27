# Files Ingestion and Export using Pyspark

The objective of this project is demonstrate the ingestion of different types of file, and answer some questions exporting in .xls files the results.
The whole process was developed using Pyspark dataframes.


# Challenge

"The assignment involves an FPSO (Floating Production, Storage and Offloading) vessel data. The vessel contains some equipment and each equipment have multiple sensors. Every time a failure happens, we get all the sensors data from the failed equipment and we store this information in a log file, the time is in GMT time zone.

You are provided 3 files: the log file named equipment_failure_sensors.log; the file named equipment_sensors.csv with the relationships between sensors and equipment and the file named equipment.json with the equipment data.

To solve this problem, we expect you to answer a few questions related to January 2020 considering GMT time zone:

1 – Total equipment failures that happened?
Anwser:
|total_failures|
|-----------------|
|         36979|



2 – Which equipment code had most failures?
Anwser:
|top_failure_equipment_code|total_errors|
|---------------|----------------|
|                  E1AD07D4| 3753|

3 – Average amount of failures across equipment group, ordering by the amount of failures in ascending order?"
Anwser:
|equipment_group_name|average_failures|
|---------------|----------------|
|            VAPQY59S|          2592.0|
|            FGHQWR2Q|          2703.5|
|            PA92NCXZ|          2726.5|
|            NQWPA8D3|          2881.0|
|            9N127Z5P|          3033.5|
|            Z9K1SAP4|          3699.0|

 

Feel free to use the programming language and tools (cloud, SQL databases, ...) you would like. Once you’re done, please send us a zip file containing all your code and a document explaining the steps that were taken with the responses for the questions, but also other conclusions and insights that you think are relevant to the project. We value creativity!

## Tools

- Python 3.9.2
- Spark 3.1.1

## Functions

Following is listed the functions available on the application:

- load_files(): Responsible to import the files of the skill test and collect the data from structured and non structured files to dataframes.
- data_transformation(): Responsible to join the dataframes loaded before and generate a final dataframe that'll be used to querying .
- challenge_result(): Queries scripts built to answer the challenge questions
- def data_export(): Get the dataframes built to answer the questions previously and export to ".../export"

# Import Path
The path ".../import" contains the paths where the files of the challenge were placed:
- .../import/equipment_data/equipment.json
- .../import/equipment_sensors/equipment_sensors.csv
- .../import/sensors_log/equipment_failure_sensors.log
# Export Path

The path ".../export" contains the .xls files responsible to answer the questions of the challenge: 
The files are:
- .../export/question_1_result.xls
- .../export/question_2_result.xls
- .../export/question_3_result.xls
