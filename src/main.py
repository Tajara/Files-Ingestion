from pyspark.sql.types import *
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


def load_files(path):
    '''Function responsible for loading the files available for this challenge
        For more information about the directories, access ../readme.me
    '''
    '''CHANGE THE PATH OF THE VARIABLE ON LINE 204 ACCORDING TO YOUR ENVIRONMENT'''

    # Read equipment data from json files
    equipment_data = spark.read.option(
        "multiline", "true"
    ).json(
        path+'\import\equipment_data\*.json'
    ).withColumnRenamed(
        "equipment_id", "equipment_idx"
    )

    # Read equipment sensors from .csv files
    equipment_sensors = spark.read.format(
        "csv"
    ).option(
        "header", "true"
    ).option(
        "delimiter", ";"
    ).load(
        path+'\import\equipment_sensors\*.csv')

    # Read sensor logs from .log files
    sensor_import = spark.read.format(
        "text"
    ).load(
        path+'\import\sensors_log\*.log'
    )

    # Conversion of unstructured data from log files to structured dataframe
    #the .log files available don't have a delimiter, regular expressions were used to solve the case
    sensor_logs = sensor_import.withColumn(
        "event_datetime",
        regexp_extract(
            #regex to match the timestamp, example: 2020-03-04 23:01:04
            col('value'), r'[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}', 0)
    ).withColumn(
        "event_status",
        regexp_extract(
            # regex to match the string with the string ERROR(or other status that starts or ends with capital letter
            col('value'), r'[A-Z]*[A-Z]', 0)
    ).withColumn(
        "sensor",
        regexp_replace(regexp_extract(
            #regex to match the value between the two brackets after the string 'sensor'
            col('value'), r'(?<=sensor)\[(.*?)\]', 0), '(\[)|(\])', '')
    ).withColumn(
        "temperature",
        regexp_extract(
            # regex to match the float value after the string "temperature" and before the ","
            col('value'), r'(?<=temperature\t).*?(?=,)', 0)
    ).withColumn(
        "vibration",
        regexp_extract(
            # regex to match the float value after the string "vibration" and before the ")"
            col('value'), r'(?<=vibration\t).*?(?=\))', 0)
    ).drop("value")

    return(equipment_data,equipment_sensors,sensor_logs)

def data_transformation(equipment_data,equipment_sensors,sensor_logs):
    '''Function responsible for transform the data loaded before, as joins between the tables
    and column types cast of the final dataframe that's used to answer the questions
     '''

    # Left Join between the sensors and equipment dataframes
    equipment_sensors_info = equipment_sensors.join(
        equipment_data,
        equipment_sensors.equipment_id == equipment_data.equipment_idx,
        'left'
    )

    # Left Join between the sensors/equipment information and the equipment logs dataframe
    equipment_sensor_log = equipment_sensors_info.join(
        sensor_logs, equipment_sensors_info.sensor_id == sensor_logs.sensor
    ).drop("sensor", "equipment_idx")

    # Cast Column Types
    df = equipment_sensor_log.withColumn(
        "equipment_id", col("equipment_id").cast(IntegerType())
    ).withColumn(
        "sensor_id", col("sensor_id").cast(IntegerType())
    ).withColumn(
        "equipment_code", col("code")
    ).withColumn(
        "equipment_group_name", col("group_name")
    ).withColumn(
        "event_datetime", col("event_datetime").cast(TimestampType())
    ).withColumn(
        "event_status", col("event_status").cast(StringType())
    ).withColumn(
        "temperature", col("temperature").cast(DecimalType())
    ).withColumn(
        "vibration", col("vibration").cast(DecimalType())
    ).drop("group_name", "code")

    return df

def challenge_result(df):

    '''Scripts responsible to answering the questions of the challenge
           '''

    #1 – Total equipment failures that happened?
    total_failures = df.count()
    total_failures_df = df.withColumn(
        'total_failures',
        lit(
            total_failures
            )
    )
    question_1_result_df = total_failures_df.select("total_failures").distinct()

    # 2 – Which equipment code had most failures?
    group_equipment_failures = df.groupBy(
        "equipment_code"
    ).agg(
        count('sensor_id').alias("total_errors")
    ).orderBy(
        desc("total_errors")
    ).withColumnRenamed(
        "equipment_code","top_failure_equipment_code"
    )
    '''Below, the column named: "rank" has been created, with this column it's easier to find the 
    equipment with most failures by any ranked position as used on the line 134 of the script'''
    w = Window.orderBy(desc(col("total_errors")))
    equipment_failures_rank = group_equipment_failures.withColumn(
        "rank", row_number().over(w)
    )
    question_2_result_df = equipment_failures_rank.filter(
        col('rank') == 1
    ).drop('rank')


    # 3 – Average amount of failures across equipment group, ordering by the amount of failures in ascending order?
    total_erros_equipment_group = df.groupBy(
        "equipment_group_name",
        "equipment_code"
    ).agg(
        count('sensor_id').alias("total_errors")
    ).orderBy(
        desc("total_errors")
    )

    question_3_result_df = total_erros_equipment_group.groupBy(
        "equipment_group_name"
    ).agg(
        avg('total_errors').alias('average_failures')
    ).orderBy(
        asc('average_failures')
    )

    return question_1_result_df,question_2_result_df,question_3_result_df

def data_export(question_1_df, question_2_df, question_3_df,path):
    """
    Function responsible to export to .xls the dataframes:
    question_1_df: Question 1 answer
    question_2_df: Question 2 answer
    question_3_df: Question 3 answer
    """

    #Terminal output for the questions
    print('# 1 – Total equipment failures that happened?')
    question_1_df.show()
    print('# 2 – Which equipment code had most failures?')
    question_2_df.show()
    print('# 3 – Average amount of failures across equipment group, ordering by the amount of failures in ascending order?')
    question_3_df.show()

    #Dataframe To Pandas conversion
    question_1 = question_1_df.toPandas()
    question_2 = question_2_df.toPandas()
    question_3 = question_3_df.toPandas()


    #Files Export to '...\shape\export'
    question_1.to_excel(path+'\export\question_1_result.xls',index = False)
    print('QUESTION 1 REPORT SUCCESFULLY GENERATED, AVAILABLE IN: \shape\export\question_1_result.xls')
    question_2.to_excel(path + '\export\question_2_result.xls', index=False)
    print('QUESTION 2 REPORT SUCCESFULLY GENERATED, AVAILABLE IN: \shape\export\question_2_result.xls')
    question_3.to_excel(path + '\export\question_3_result.xls', index=False)
    print('QUESTION 3 REPORT SUCCESFULLY GENERATED, AVAILABLE IN: \shape\export\question_3_result.xls')

    print('PROCESS CONCLUDED SUCCESFULLY')

    pass

if __name__ == '__main__':
    '''
    This application was developed to the resolution of the Data Engineer challenge position at Shape.
    '''
    spark = SparkSession.builder.appName('file_ingestion_and_export').getOrCreate()

    '''Define the root path of the project below'''
    path = "D:\Projetos\ingestion_and_export"
    #Function responsible for extract and load the files in dataframes
    equipment_data,equipment_sensors,sensor_logs = load_files(path)

    # Function responsible for data transformation and modelling
    df = data_transformation(equipment_data,equipment_sensors,sensor_logs)

    #Function with the script reponsible to retrieve the challenge answers
    question_1_df,question_2_df,question_3_df = challenge_result(df)

    #Function responsible to convert the dataframes to pandas and export the files to .xls type
    data_export(question_1_df,question_2_df,question_3_df,path)







