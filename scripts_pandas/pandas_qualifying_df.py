import pandas as pd
from datetime import datetime as dt

today = dt.today()
date = today.date()

INPUT_FILE_PATH = 'qualifying_df/*'
TABLE_NAME = 'qualifying_df'
RAW_BUCKET = 'raw'
CLEANSED_BUCKET = 'cleansed'
OUTPUT_PATH = f'/{TABLE_NAME}/snapshot_date={date}/'

qualifying_df = pd.read_json(path=f's3://{RAW_BUCKET}/{INPUT_FILE_PATH}')

# Renaming the dataset to be in the desired format
new_column_names = {'qualifyingId': 'qualifying_id',
                    'raceID': 'race_id',
                    'constructorID': 'constructor_id'}

qualifying_df.rename(columns=new_column_names, axis='columns', inplace=True)

qualifying_df = qualifying_df.astype(
    {'qualifying_id': 'int64',
    'race_id': 'int64',
    'driver_id': 'int64',
    'constructor_id': 'int64',
    'number': 'int64',
    'position': 'int64',
    'q1': 'str',
    'q2': 'str',
    'q3': 'str'}
)

qualifying_df['snapshot_date'] = date

qualifying_df.to_parquet(path=f's3://{CLEANSED_BUCKET}/{OUTPUT_PATH}/',
                         partition_cols='snapshot_date')