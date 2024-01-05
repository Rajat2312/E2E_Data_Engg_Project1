# Import Libraries
import numpy as numpy
import pandas as pd
pd.set_option('display.max_columns',100)

# Read data
df = pd.read_csv('/opt/airflow/dags/Wine.csv')
#print(df.head()) # Successful

# Data Quality Analysis

### Data Type Check
print('Dataframe dimensions:',df.shape)
print(df.info())

### 1. Missing value analysis
# print(df.isna().sum())

# Data Analysis
print(df.describe())

# Add index column
df['id'] = [i for i in range(len(df))] 
print(df.info())

# Store new csv in the dags directory
df.to_csv('/opt/airflow/dags/wine_cleaned.csv',index=False)
