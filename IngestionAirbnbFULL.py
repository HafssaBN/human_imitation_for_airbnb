from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pandas as pd
import os
import Config
import csv

# Initialize a Spark session with Hive support
spark = SparkSession.builder \
    .appName("Create Hive Table") \
    .enableHiveSupport() \
    .getOrCreate()


excel_path = Config.CONFIG_OUTPUT_FILE

df = pd.read_excel(excel_path, engine='openpyxl')
print(df.head(5))
print(df.shape)

max_timestamp = df['scrape_time'].max()

script_dir = os.path.dirname(os.path.abspath(__file__))
fp = os.path.join(script_dir, 'metadata.txt')
with open(fp, 'w') as f:
    f.write(str(max_timestamp))

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

columns = ",\n    ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in schema.fields])

drop_table_query = f"""
DROP TABLE {hive_table_name}
"""
create_table_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table_name} (
    {columns}
)
STORED AS PARQUET
LOCATION '{hive_table_location}'
"""

# Print the generated create table query
print(create_table_query)

# Execute the create table query
spark.sql(create_table_query)


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

# Stop the Spark session
spark.stop()
