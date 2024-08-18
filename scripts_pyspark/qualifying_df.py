import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime as dt

# Setting up the environment
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.master().getOrCreate()

today = dt.today()
date = today.date()

INPUT_FILE_PATH = 'qualifying_df/*'
TABLE_NAME = 'qualifying_df'
RAW_BUCKET = 'raw'
CLEANSED_BUCKET = 'cleansed'
OUTPUT_PATH = f'/{TABLE_NAME}/snapshot_date={date}/'

# Creating the schema for the dataset
qualifying_schema = StructType(fields=[
    StructField('qualifyingId', IntegerType(), False),
    StructField('raceID', IntegerType(), True),
    StructField('driverID', IntegerType(), True),
    StructField('constructorID', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

# Reading qualifying results of multiple json files
qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .options('MultiLine', True) \
    .json(f's3://{RAW_BUCKET}/{INPUT_FILE_PATH}')

# Renaming the columns
qualifying_df = qualifying_df.withColumnRenamed('qualifyingId', 'qualifying_id') \
    .withColumnRenamed('raceID', 'race_id') \
    .withColumnRenamed('driverID', 'driver_id') \
    .withColumnRenamed('constructorID', 'constructor_id') \
    .withColumn('snapshot_date', F.current_timestamp())
            
# Writing as a .parquet file
qualifying_df.write.mode('overwrite').parquet(f's3://{CLEANSED_BUCKET}/{OUTPUT_PATH}/')