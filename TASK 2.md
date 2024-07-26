#task2
#step1
databricks fs cp your-local-file.json dbfs:/mnt/your-directory/your-file.json

#step2
from pyspark.sql.functions import col, explode, flatten
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Load JSON data into a DataFrame
json_df = spark.read.json("/mnt/your-directory/your-file.json")

# Function to flatten nested JSON
def flatten_df(nested_df):
    flat_cols = [col for col in nested_df.columns if isinstance(nested_df.schema[col].dataType, StructType)]
    array_cols = [col for col in nested_df.columns if isinstance(nested_df.schema[col].dataType, ArrayType)]
    
    flat_df = nested_df
    for flat_col in flat_cols:
        flat_df = flat_df.selectExpr("*", f"{flat_col}.*").drop(flat_col)
    
    for array_col in array_cols:
        flat_df = flat_df.withColumn(array_col, explode(col(array_col)))
        flat_df = flatten_df(flat_df)
    
    return flat_df

# Flatten the JSON DataFrame
flattened_df = flatten_df(json_df)
flattened_df.show()

#step3
# Write the flattened DataFrame as a Parquet file
output_path = "/mnt/your-directory/flattened-parquet"
flattened_df.write.parquet(output_path, mode='overwrite')

# Register the Parquet file as an external table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS flattened_parquet_table
    USING PARQUET
    OPTIONS (
      path '{output_path}'
    )
""")
#step4
from pyspark.sql.functions import col, explode, flatten
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Load JSON data into a DataFrame
json_df = spark.read.json("/mnt/your-directory/your-file.json")

# Function to flatten nested JSON
def flatten_df(nested_df):
    flat_cols = [col for col in nested_df.columns if isinstance(nested_df.schema[col].dataType, StructType)]
    array_cols = [col for col in nested_df.columns if isinstance(nested_df.schema[col].dataType, ArrayType)]
    
    flat_df = nested_df
    for flat_col in flat_cols:
        flat_df = flat_df.selectExpr("*", f"{flat_col}.*").drop(flat_col)
    
    for array_col in array_cols:
        flat_df = flat_df.withColumn(array_col, explode(col(array_col)))
        flat_df = flatten_df(flat_df)
    
   



