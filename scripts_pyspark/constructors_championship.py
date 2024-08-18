# Gets a table with the grouped values for the constructor championship with the teams with the most wins, ranked
'''
Develop an aggregated table with the sum of all of the points achieved in the construction championship, counting the wins grouped by the race year and team. Creates an 'snapshot_date' column with the date of the snapshot and saves in the presentation zone of the datalake. 
'''
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from datetime import datetime as dt

# Setting up the environment
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.master().getOrCreate()

today = dt.today()
date = today.date()

FILE_PATH = 'constructors_df'
TABLE_NAME = 'ranked_chanpionship'
RAW_BUCKET = 'raw'
PRESENTATION_BUCKET = 'presentation'
OUTPUT_PATH = f'/{TABLE_NAME}/snapshot_date={date}/'

# Reading the file to the memory
constructors_df = spark.read.parquet(f's3://{RAW_BUCKET}/{FILE_PATH}/')

constructors_df.groupBy('race_year', 'team') \
               .agg(F.sum(constructors_df.points).alias('total_points'),
                    F.count(F.when(constructors_df.position == 1, True)).alias('wins')
                             .otherwise('lose'))

championshipRanked = Window.partitionBy('race_year').orderBy(F.desc('total_points'), F.desc('wins'))
final_df = constructors_df.withColumn('rank', F.rank().over(championshipRanked)) \
                          .withColumn('snapshot_date', F.lit(date))

final_df.write.mode('overwrite').parquet(f's3:///{PRESENTATION_BUCKET}/{OUTPUT_PATH}/')
