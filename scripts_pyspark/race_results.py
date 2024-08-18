# Generate a table from the 2020 Yas Marina Grand Prix from the drivers, constructors and results, circuits and races tables. The schema of the table generated needs to be equals to the one from the course (BBC Sport website).

import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, TimestampType, DateType
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime as dt

# Setting up the environment
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.master().getOrCreate()

today = dt.today()
date = today.date()

DRIVERS_DF_FILE_PATH = 'drivers_df'
CONSTRUCTORS_DF_FILE_PATH = 'constructors_df'
RESULTS_DF_FILE_PATH = 'results_df'
CIRCUITS_DF_FILE_PATH = 'circuits_df'
RACES_DF_FILE_PATH = 'races_df'
CLEANSED_BUCKET = 'cleansed'
PRESENTATION_BUCKET = ''

# Reading the dataframes
drivers_df = spark.read.parquet('/file_path/')
constructors_df = spark.read.parquet('/file_path/')
results_df = spark.read.parquet('/file_path/')
circuits_df = spark.read.parquet('/file_path/')
races_df = spark.read.parquet('/file_path/')

# Renaming column names to not interfer in the joining
races_df = races_df.withColumnRenamed('name', 'race_name') \
                   .withColumnRenamed('year', 'race_year') \
                   .withColumnRenamed('date', 'race_date')

circuits_df = circuits_df.withColumnRenamed('location', 'circuit_location')

results_df = results_df.withColumnRenamed('fastestLapTime', 'fastest_lap') \
                       .withColumnRenamed('time', 'race_time')

drivers_df = drivers_df.withColumnRenamed('name', 'driver_name') \
                       .withColumnRenamed('number', 'driver_number') \
                       .withColumnRenamed('nationality', 'driver_nationality')

constructors_df = constructors_df.withColumnRenamed('name', 'team')

# Joining the races dataframe wity the circuits dataframe
races_circuits_df = races_df.join(circuits_df, on=results_df.circuitId == races_df.circuitId, how='inner')

# Joining the results dataframe with the rest of the dataframes
final_df = results_df.join(races_circuits_df, on=results_df.raceId == races_circuits_df.raceId, how='inner') \
                     .join(drivers_df, on=results_df.driverId == drivers_df.driverId, how='inner') \
                     .join(constructors_df, on=results_df.constructorId == constructors_df.constructorId, how='inner')

# Selecting the columns for the final dataframe and create the 'created_date' column
final_df = final_df.select(
    final_df.race_year,
    final_df.race_name,
    final_df.race_date,
    final_df.circuit_location,
    final_df.driver_name,
    final_df.driver_number,
    final_df.driver_nationality,
    final_df.team,
    final_df.grid,
    final_df.fastest_lap,
    final_df.race_time,
    final_df.points
).withColumn('created_date', F.current_timestamp())

# Defining the column dtypes
final_table_schema = StructType(fields=[
    StructField('race_year', StringType(), nullable=False),
    StructField('race_name', StringType(), nullable=False),
    StructField('race_date', DateType(), nullable=False),
    StructField('circuit_location', StringType(), nullable=False),
    StructField('driver_name', StringType(), nullable=False),
    StructField('driver_number', IntegerType(), nullable=True),
    StructField('driver_nationality', StringType(), nullable=False),
    StructField('team', StringType(), nullable=False),
    StructField('grid', IntegerType(), nullable=False),
    StructField('fastest_lap', StringType(), nullable=True),
    StructField('race_time', StringType(), nullable=True),
    StructField('points', FloatType(), nullable=False),
    StructField('created_date', TimestampType(), nullable=False)
])

# Save the final dataframe 
final_df.write.mode('overwrite').parquet('/file_path/')
