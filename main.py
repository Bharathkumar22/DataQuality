import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName('DataQualityCheck').getOrCreate()

# Read data from Oracle and S3
oracle_df = spark.read.format("jdbc").option("url", "<Oracle URL>").option("dbtable", "<Oracle table>").load()
s3_df = spark.read.format("csv").option("header", "true").load("<S3 file path>")

# Convert data to Pandas dataframe
oracle_pandas_df = oracle_df.toPandas()
s3_pandas_df = s3_df.toPandas()

# Define data quality rules
def compare_dataframes(df1, df2):
    assert df1.equals(df2), 'Dataframes are not equal'

# Compare dataframes and generate report with highlighted differences
try:
    compare_dataframes(oracle_pandas_df, s3_pandas_df)
    print('Data quality check passed')
except AssertionError as error:
    print('Data quality check failed')
    # highlight differences in the two dataframes
    df_diff = oracle_pandas_df.compare(s3_pandas_df)
    df_styled = df_diff.style.applymap(lambda x: 'background-color: yellow', subset=pd.IndexSlice[:, df_diff.columns])
    display(df_styled)
