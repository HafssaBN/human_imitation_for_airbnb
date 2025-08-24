from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pandas as pd
import os
import csv
import Config

# Initialize a Spark session with Hive support
spark = SparkSession.builder \
    .appName("Create Hive Table") \
    .enableHiveSupport() \
    .getOrCreate()


excel_path = Config.CONFIG_OUTPUT_FILE

df = pd.read_excel(excel_path, engine='openpyxl')
print(df.head(5))
print(df.shape)

script_dir = os.path.dirname(os.path.abspath(__file__))
fp = os.path.join(script_dir, 'metadata.txt')
with open(fp, 'r') as f:
    content = f.readline()

old_scrape_time = int(content.strip())

df = df[df['scrape_time'] > old_scrape_time]

new_scrape_time = df['scrape_time'].max()

# Convert all columns to string
df = df.astype(str)

# Rename columns: replace spaces with underscores and convert to lowercase
df.columns = df.columns.str.lower()

# Define the schema based on the modified DataFrame columns
schema = StructType([StructField(col, StringType(), True) for col in df.columns])
schema = schema.add(StructField("ingestion_date", TimestampType(), True))

# Dynamically generate the create table SQL statement using the schema
hive_table_name = "webdata.airbnb"
hive_table_location = "/warehouse/tablespace/External/hive/webdata.db/airbnb/FULL"

# Define the schema based on the modified DataFrame columns
schema = StructType([StructField(col, StringType(), True) for col in df.columns])

# Convert Pandas DataFrame to Spark DataFrame
sdf = spark.createDataFrame(df, schema=schema)

#Add the ingestion_date column with current timestamp
from pyspark.sql.functions import current_timestamp
sdf = sdf.withColumn("ingestion_date", current_timestamp())


#Register the DataFrame as a temporary view in Spark SQL
#sdf.createOrReplaceTempView("marchespublics_temp")

# Insert data into the Hive table
sdf.repartition(1).write.mode('append').parquet('/warehouse/tablespace/External/hive/webdata.db/airbnb/FULL')

with open(fp, 'w') as f:
    f.write(str(new_scrape_time))

# Stop the Spark session
spark.stop()
