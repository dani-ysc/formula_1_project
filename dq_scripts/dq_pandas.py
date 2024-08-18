'''
This script contain basic commands to verify the quality of the dataset, like null checks, shape of dataset before and after the transformations and joins, etc.
'''
import pandas as pd

df = pd.read_csv()

# See the dataset shape (row and column count):
df.shape

# See info about the columns and their datatypes:
df.info()

# See the amount of null values in a column:
print(df['column'].isnull().sum())

# Looking at a list of unique values in a column:
df['column'].unique

